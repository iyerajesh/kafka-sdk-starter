package com.xylia.platform.events.client;

import com.xylia.platform.events.exception.ApplySchemaValidationException;
import com.xylia.platform.events.model.ApplyEventTypes;
import com.xylia.platform.events.serialization.CloudEventKafkaDeserializer;
import com.xylia.platform.events.serialization.CloudEventKafkaSerializer;
import com.xylia.platform.events.support.ApplyKafkaHeaders;
import com.xylia.platform.events.support.DeadLetterFailureReasons;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import kafka.Kafka;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.backoff.FixedBackOff;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(topics = {DeadLetterPublisherTest.TEST_TOPIC_DLT, DeadLetterPublisherTest.TEST_TOPIC})
@Slf4j
public class DeadLetterPublisherTest {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Mock
    Acknowledgment acknowledgment;

    private static final String TYPE = ApplyEventTypes.APPLICATION_SUBMITTED_EVENT.getType();
    private static final URI DATA_SCHEMA_URL = URI.create("url/dataschema");
    private static final String DATA_CONTENT_TYPE = "application/json";
    private static final String SUBJECT = "subject";
    private static final ZonedDateTime TIME = ZonedDateTime.parse("2007-12-03T10:15:30+01:00[Europe/Paris]");
    private static final String DATA = "{}";
    private static final URI SOURCE_URL = URI.create("url/source");
    private static final String ID = UUID.randomUUID().toString();
    private static final String APPLICATION_ID = "2020309EC359209NOK";

    public static final String TEST_TOPIC = "test-topic";
    public static final String TEST_TOPIC_DLT = "test-topic.DLT";
    public static final String TEST_CONSUMER_GROUP = "test-consumer-group";

    private BlockingQueue<ConsumerRecord<String, CloudEventImpl>> consumerRecords;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private KafkaMessageListenerContainer<String, CloudEventImpl> container;

    @Autowired
    private KafkaTemplate<Object, Object> dltKafkaTemplate;

    private DeadLetterPublisher deadLetterPublisher;

    @BeforeEach
    public void setUp() {

        this.deadLetterPublisher = new DeadLetterPublisher(dltKafkaTemplate);
        consumerRecords = new LinkedBlockingDeque<>();

        // consumer is listening to the DLT topic
        ContainerProperties containerProperties = new ContainerProperties(TEST_TOPIC_DLT);

        Map<String, Object> consumerProperties = KafkaTestUtils.consumerProps(TEST_CONSUMER_GROUP,
                "false", embeddedKafkaBroker);

        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventKafkaDeserializer.class);

        DefaultKafkaConsumerFactory<String, CloudEventImpl> consumer = new DefaultKafkaConsumerFactory<>(consumerProperties);

        container = new KafkaMessageListenerContainer<>(consumer, containerProperties);
        container.setupMessageListener((MessageListener<String, CloudEventImpl>) record -> {
            log.debug("Listened to message: {}", record.toString());
            consumerRecords.add(record);
        });
        container.start();

        ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
    }

    @AfterEach
    public void tearDown() {
        container.stop();
    }

    private KafkaTemplate<Object, Object> buildKafkaTemplate() {

        // setup the Kafka producer properties
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker.getBrokersAsString());

        producerProps.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventKafkaSerializer.class);

        // create a Kafka producer factory
        ProducerFactory<Object, Object> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);

        return new KafkaTemplate<Object, Object>(producerFactory);

    }

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(TEST_CONSUMER_GROUP,
                "true", embeddedKafkaBroker);

        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CloudEventKafkaDeserializer.class);

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, TEST_CONSUMER_GROUP);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return consumerProps;
    }

    public ConsumerFactory<String, Message> consumerFactory() {
        return new DefaultKafkaConsumerFactory<String, Message>(consumerConfigs());
    }

    private DeadLetterPublisher deadLetterPublisher() {
        return new DeadLetterPublisher(dltKafkaTemplate);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Message> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new DeadLetterPublishingRecoverer(dltKafkaTemplate),
                new FixedBackOff(0L, 2L)));

        return factory;
    }


    @Test
    @Disabled
    public void testDeadLetterTopicPublishAndConsume() throws Exception {

        CloudEventImpl cloudEvent = CloudEventBuilder.builder()
                .withId(ID)
                .withTime(TIME)
                .withType(TYPE)
                .withSource(SOURCE_URL)
                .withSubject(SUBJECT)
                .withDataschema(DATA_SCHEMA_URL)
                .withDataContentType(DATA_CONTENT_TYPE)
                .withData("")
                .build();

        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<>(TEST_TOPIC, 0, 123L,
                null, cloudEvent);

        deadLetterPublisher.publish(consumerRecord, cloudEvent, acknowledgment,
                new ApplySchemaValidationException("Schema Validation failed!"), DeadLetterFailureReasons.SCHEMA_VALIDATION_FAILURE);

        ConsumerRecord<String, CloudEventImpl> receivedRecord = consumerRecords.poll(5, TimeUnit.SECONDS);

        assertThat(receivedRecord.value().getAttributes().getSource()).isEqualTo(cloudEvent.getAttributes().getSource());

        String failureReason = new String(receivedRecord.headers()
                .headers(ApplyKafkaHeaders.DLT_FAILURE_REASON).iterator().next().value());

        assertThat(failureReason).isEqualTo(DeadLetterFailureReasons.SCHEMA_VALIDATION_FAILURE.getFailureReason());
    }

    @Test
    public void testPublisherWithNullEvent() throws Exception {

        ConsumerRecord<Object, Object> consumerRecord = new ConsumerRecord<>(TEST_TOPIC, 0, 123L, APPLICATION_ID, null);

        deadLetterPublisher.publish(consumerRecord, new ApplySchemaValidationException("Schema validation failed!"),
                DeadLetterFailureReasons.SCHEMA_VALIDATION_FAILURE);
    }


}