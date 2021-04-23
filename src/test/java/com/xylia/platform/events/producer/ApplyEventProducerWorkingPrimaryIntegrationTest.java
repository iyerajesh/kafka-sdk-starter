package com.xylia.platform.events.producer;

import com.xylia.platform.events.configuration.SDKConfiguration;
import com.xylia.platform.events.model.ApplyEventTypes;
import com.xylia.platform.events.serialization.CloudEventKafkaDeserializer;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.event.annotation.BeforeTestClass;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.junit4.SpringRunner;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest(properties = {"spring.main.allow-bean-definition-overriding=true"})
@DirtiesContext
@EmbeddedKafka(
        topics = {ApplyEventProducerWorkingPrimaryIntegrationTest.TOPIC},
        partitions = 1,
        controlledShutdown = false,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:3333",
                "port=3333"
        })

public class ApplyEventProducerWorkingPrimaryIntegrationTest {

    public static final String TOPIC = "application.submission.topic";

    private KafkaMessageListenerContainer<String, CloudEventImpl> container;
    private BlockingQueue<ConsumerRecord<String, CloudEventImpl>> consumerRecords;
    private CountDownLatch countDownLatch;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ApplyEventProducer service;

    @Autowired
    private MockProducerCallback callback;

    private CloudEventKafkaDeserializer deserializer = new CloudEventKafkaDeserializer();
    private URL CE_GOLD_DECISION_URL = getClass().getClassLoader().getResource("submission_payload.json");

    @BeforeEach
    public void setUp() {

        countDownLatch = new CountDownLatch(5);
        consumerRecords = new LinkedBlockingDeque<>();

        ContainerProperties containerProperties = new ContainerProperties(TOPIC);

        Map<String, Object> consumerProperties = KafkaTestUtils
                .consumerProps("consumer", "false", embeddedKafkaBroker);

        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventKafkaDeserializer.class);

        DefaultKafkaConsumerFactory<String, CloudEventImpl> consumer =
                new DefaultKafkaConsumerFactory<>(consumerProperties);

        container = new KafkaMessageListenerContainer<>(consumer, containerProperties);
        container.setupMessageListener((MessageListener<String, CloudEventImpl>) record -> {
            System.out.println("RECORD:" + record.toString());
            consumerRecords.add(record);
        });
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @AfterEach
    public void tearDown() {
        container.stop();
        callback.setKafkaClusterRegion(null);
    }

    @Test
    @Disabled
    public void testIfMessageGotProducedByPrimaryCluster() throws Exception {

        CloudEventImpl ce = deserializer.deserialize("",
                Files.readAllBytes(Paths.get(CE_GOLD_DECISION_URL.toURI())));
        service.publish(ApplyEventTypes.APPLICATION_SUBMITTED_EVENT, ce, callback);

        countDownLatch.await(10L, TimeUnit.SECONDS);
        ConsumerRecord<String, CloudEventImpl> received = consumerRecords.poll(2, TimeUnit.SECONDS);

        assertThat(received.value().getAttributes().getSource()).isEqualTo(ce.getAttributes().getSource());
    }

    @Test
    @Disabled
    public void publishCloudEvent() throws Exception {

        CloudEventImpl ce = deserializer.deserialize("", Files.readAllBytes(Paths.get(CE_GOLD_DECISION_URL.toURI())));

        service.publish(ce, callback);
        countDownLatch.await(10L, TimeUnit.SECONDS);

        ConsumerRecord<String, CloudEventImpl> received = consumerRecords.poll(2, TimeUnit.SECONDS);

        assertThat(received.value().getAttributes().getSource()).isEqualTo(ce.getAttributes().getSource());
        assertThat(received.value().getAttributes().getDataschema()).isEqualTo(ce.getAttributes().getDataschema());
        assertThat(received.value().getAttributes().getDatacontenttype()).isEqualTo(ce.getAttributes().getDatacontenttype());
    }
}
