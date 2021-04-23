package com.xylia.platform.events;

import com.xylia.platform.events.configuration.SDKConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.event.annotation.BeforeTestClass;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka
@DirtiesContext
public class ApplyEventPlatformStarterTest {

    @BeforeTestClass
    public void setUp() {
        SDKConfiguration.setConsumerGroup("test-consumer");
    }

    @Test
    public void main() {
        ApplyEventPlatformStarter.main(new String[0]);
    }
}
