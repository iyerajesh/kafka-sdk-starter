package com.xylia.platform.events.consumer;

import com.xylia.platform.events.ApplyEventPlatformSDK;
import com.xylia.platform.events.callback.ApplyConsumerCallback;
import com.xylia.platform.events.callback.RegisterCallbackContract;
import com.xylia.platform.events.client.DeadLetterPublisher;
import com.xylia.platform.events.configuration.SDKConfiguration;
import com.xylia.platform.events.exception.ApplyPublishingException;
import com.xylia.platform.events.model.ApplyEventTypes;
import com.xylia.platform.events.sendresult.ApplyProducerCallback;
import com.xylia.platform.events.sendresult.ApplySendResult;
import com.xylia.platform.events.support.KafkaClusterRegion;
import io.cloudevents.v1.CloudEventBuilder;
import io.cloudevents.v1.CloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import scala.App;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@EmbeddedKafka
public class ApplyEventConsumerIntegrationTest {

    private ApplyEventPlatformSDK applyEventPlatformSDK;
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

    @BeforeEach
    public void setUp() {
        SDKConfiguration.setConsumerGroup(TEST_CONSUMER_GROUP);
    }

    @AfterEach
    public void clean() {
        applyEventPlatformSDK = null;
    }

    @Test
    public void consumer() {

        applyEventPlatformSDK = new ApplyEventPlatformSDK();
        applyEventPlatformSDK.bootstrapSDK();
        RegisterCallbackContract.builder()
                .forEventName(ApplyEventTypes.APPLICATION_SUBMITTED_EVENT)
                .withCallback(MockcallbackClass.class)
                .build();

        applyEventPlatformSDK.consumer().listenToRegisteredTopics();
        CloudEventImpl cloudEvent = CloudEventBuilder.builder()
                .withId(ID)
                .withTime(TIME)
                .withType(TYPE)
                .withSource(SOURCE_URL)
                .withSubject(SUBJECT)
                .withDataschema(DATA_SCHEMA_URL)
                .withDataContentType(DATA_CONTENT_TYPE)
                .withData(DATA)
                .build();

        applyEventPlatformSDK.producer().publish(cloudEvent, new ApplyProducerCallback() {
            @Override
            public void onSuccess(ApplySendResult onSuccess) {

                CloudEventImpl cloudEvent = (CloudEventImpl) onSuccess.getProducerRecord().getValue();
                assertThat(cloudEvent.getAttributes().getType()).isEqualTo(ApplyEventTypes.APPLICATION_SUBMITTED_EVENT);
            }

            @Override
            public void onFailure(ApplyPublishingException ex) {

            }

            @Override
            public void setKafkaClusterRegion(KafkaClusterRegion kafkaClusterRegion) {

            }

            @Override
            public KafkaClusterRegion getKafkaClusterRegion() {
                return null;
            }
        });
    }

    @Test
    public void testForkJoinPoolCreatesSeparateThread() throws InterruptedException {

        String currentThread = Thread.currentThread().getName();
        ForkJoinPool customThreadPool = new ForkJoinPool(4);
        customThreadPool.submit(() -> {
            new MockCallbackImpl(currentThread);
        });
    }
}

class MockCallbackImpl implements ApplyConsumerCallback {

    private String parentThread;

    public MockCallbackImpl(String parentThread) {
        this.parentThread = parentThread;

        assertThat(Thread.currentThread().getName()).isNotEqualTo(parentThread);
    }

    @Override
    public void processEvent(ConsumerRecord<Object, Object> consumerRecord, CloudEventImpl payload, Acknowledgment acknowledgment, DeadLetterPublisher deadLetterPublisher) {

    }
}

@Slf4j
class MockcallbackClass implements ApplyConsumerCallback {

    @Override
    public void processEvent(ConsumerRecord<Object, Object> consumerRecord, CloudEventImpl payload, Acknowledgment acknowledgment, DeadLetterPublisher deadLetterPublisher) {
        log.info("Mockcallback class called!");
        acknowledgment.acknowledge();
    }
}
