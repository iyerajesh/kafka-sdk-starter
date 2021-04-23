package com.xylia.platform.events;

import com.xylia.platform.events.consumer.ApplyEventConsumer;
import com.xylia.platform.events.health.KafkaConsumerMonitor;
import com.xylia.platform.events.health.KafkaHealthIndicator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(
        topics = {ApplyEventPlatformSDKTest.TOPIC},
        partitions = 1,
        controlledShutdown = false,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:3333",
                "port=3333"
        }
)
public class ApplyEventPlatformSDKTest {

    public static final String TOPIC = "conversion.application.submission.topic";
    private ApplyEventPlatformSDK applyEventPlatformSDK;

    @AfterEach
    public void clean() {
        applyEventPlatformSDK = null;
    }

    @Test
    public void consumer_throwException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            applyEventPlatformSDK = new ApplyEventPlatformSDK();
            applyEventPlatformSDK.consumer();
        });
    }

    @Test
    public void testHandleToConsumer() {
        applyEventPlatformSDK = new ApplyEventPlatformSDK();
        applyEventPlatformSDK.bootstrapSDK();

        assertThat(applyEventPlatformSDK.consumer()).isNotNull();
        assertThat(applyEventPlatformSDK.consumer()).isInstanceOf(ApplyEventConsumer.class);
    }

    @Test
    public void health_throwException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            applyEventPlatformSDK = new ApplyEventPlatformSDK();
            applyEventPlatformSDK.health();
        });
    }

    @Test
    public void testHandleToHealth() {
        applyEventPlatformSDK = new ApplyEventPlatformSDK();
        applyEventPlatformSDK.bootstrapSDK();

        assertThat(applyEventPlatformSDK.health()).isNotNull();
        assertThat(applyEventPlatformSDK.health()).isInstanceOf(KafkaHealthIndicator.class);
    }

    @Test
    public void consumerMonitor_throwException() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            applyEventPlatformSDK = new ApplyEventPlatformSDK();
            applyEventPlatformSDK.consumerMonitor();
        });
    }

    @Test
    public void testHandleToConsumerMonitor() {
        applyEventPlatformSDK = new ApplyEventPlatformSDK();
        applyEventPlatformSDK.bootstrapSDK();

        assertThat(applyEventPlatformSDK.consumerMonitor()).isNotNull();
        assertThat(applyEventPlatformSDK.consumerMonitor()).isInstanceOf(KafkaConsumerMonitor.class);
    }


}