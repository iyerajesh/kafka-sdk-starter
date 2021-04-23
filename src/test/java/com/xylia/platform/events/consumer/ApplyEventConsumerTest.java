package com.xylia.platform.events.consumer;

import com.xylia.platform.events.callback.EventCallbackRegistry;
import com.xylia.platform.events.client.DeadLetterPublisher;
import com.xylia.platform.events.model.ApplyEventTypes;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.verification.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import javax.sql.rowset.serial.SerialException;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@EmbeddedKafka(
        topics = {ApplyEventConsumerTest.TEST_TOPIC}
)
public class ApplyEventConsumerTest {

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


    private CountDownLatch countDownLatch;

    @MockBean
    private EventCallbackRegistry eventCallbackRegistry;

    @Autowired
    private ApplyEventConsumer consumer;

    @Autowired
    @Qualifier("primaryCluster")
    private KafkaTemplate kafkaTemplate;

    @BeforeEach
    public void setUp() {
        countDownLatch = new CountDownLatch(5);
    }

    @AfterEach
    public void cleanUp() {
        consumer.deRegisterAndShutdown();
    }

    @Test
    public void listenToRegisteredTopics() {
        consumer.listenToRegisteredTopics();
        verify(eventCallbackRegistry, new Timeout(1000, times(1))).getRegisteredTopics();
    }

    @Test
    public void testGetHandleToDeadLetterPublisher() {
        DeadLetterPublisher deadLetterPublisher = consumer.deadLetterPublisher();
        assertThat(deadLetterPublisher).isInstanceOf(DeadLetterPublisher.class);
    }

    @Test
    public void listenToRegisteredTopicsWithListener() throws Exception {
        when(eventCallbackRegistry.getRegisteredTopics())
                .thenReturn(Collections.singleton(TEST_TOPIC));
        consumer.listenToRegisteredTopics();

        verify(eventCallbackRegistry, new Timeout(1000, times(1))).getRegisteredTopics();

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

        kafkaTemplate.send(TEST_TOPIC, cloudEvent);
        countDownLatch.await(5L, TimeUnit.SECONDS);
    }

    @Test
    public void listenToRegisteredTopicsWithListener_withNoType() throws Exception {

        when(eventCallbackRegistry.getRegisteredTopics())
                .thenReturn(Collections.singleton(TEST_TOPIC));

        when(eventCallbackRegistry.isEventRegistered(any())).thenReturn(false);
        consumer.listenToRegisteredTopics();

        verify(eventCallbackRegistry, new Timeout(1000, times(1))).getRegisteredTopics();

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

        kafkaTemplate.send(TEST_TOPIC, cloudEvent);
        countDownLatch.await(5L, TimeUnit.SECONDS);
    }

    @Test
    public void listenToRegisteredTopicsWithRegister_ThenFailedAtCasting() throws Exception {

        Assertions.assertThrows(SerializationException.class, () -> {

            when(eventCallbackRegistry.getRegisteredTopics())
                    .thenReturn(Collections.singleton(TEST_TOPIC));
            consumer.listenToRegisteredTopics();
            verify(eventCallbackRegistry, new Timeout(1000, times(1))).getRegisteredTopics();

            kafkaTemplate.send(TEST_TOPIC, "something");
            countDownLatch.await(5L, TimeUnit.SECONDS);

        });
    }

    @Test
    public void deRegisterAndShutdown() {
        consumer.listenToRegisteredTopics();
        consumer.deRegisterAndShutdown();

        verify(eventCallbackRegistry, new Timeout(5000, times(2))).getRegisteredTopics();
    }


}