package com.xylia.platform.events.consumer;

import org.apache.kafka.common.protocol.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@EmbeddedKafka(
        topics = {ApplyKafkaListenerTest.TEST_TOPIC}
)
public class ApplyKafkaListenerTest {

    public static final String TEST_TOPIC = "test-topic";

    private ApplyKafkaListener applyKafkaListenerPrimary;
    private ApplyKafkaListener applyKafkaListenerSecondary;

    @Autowired
    @Qualifier("primaryConsumerCluster")
    private ConcurrentKafkaListenerContainerFactory<String, Message> primaryListenerFactory;

    @Autowired
    @Qualifier("secondaryConsumerCluster")
    private ConcurrentKafkaListenerContainerFactory<String, Message> secondaryListenerFactory;

    @BeforeEach
    public void setUp() {
        applyKafkaListenerPrimary = new ApplyKafkaListener(primaryListenerFactory);
        applyKafkaListenerSecondary = new ApplyKafkaListener(secondaryListenerFactory);
    }

    @Test
    public void primaryStartAndStop() {
        applyKafkaListenerPrimary.start();
        applyKafkaListenerPrimary.stop();
    }

    @Test
    public void secondaryStartAndStop() {
        applyKafkaListenerSecondary.start();
        applyKafkaListenerSecondary.stop();
    }
}