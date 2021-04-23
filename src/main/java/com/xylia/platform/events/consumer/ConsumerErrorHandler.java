package com.xylia.platform.events.consumer;

import com.xylia.platform.events.client.DeadLetterPublisher;
import com.xylia.platform.events.support.DeadLetterFailureReasons;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerAwareErrorHandler;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
@Slf4j
public class ConsumerErrorHandler implements ContainerAwareErrorHandler {

    private static final String KEY_DESSERIALIZATON_ERROR_KEY = "springDeserializerExceptionKey";
    private static final String VALUE_DESERIALIZATION_ERROR_KEY = "springDeserializatonExceptionValue";

    @Autowired
    @Qualifier("deadLetterProducer")
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Override
    public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {

        log.info("Invoked the Consumer Error Handler, exception thrown: {}", thrownException.fillInStackTrace());
        if (thrownException.getClass().equals(DeserializationException.class) || thrownException.getCause().getClass().equals(DeserializationException.class)) {

            records.stream()
                    .filter(consumerRecord -> !Objects.nonNull(consumerRecord.headers().lastHeader(KEY_DESSERIALIZATON_ERROR_KEY))
                            || !Objects.nonNull(consumerRecord.headers().lastHeader(VALUE_DESERIALIZATION_ERROR_KEY)))
                    .findFirst()
                    .ifPresent(consumerRecord -> {

                        String topic = consumerRecord.topic();
                        Object value = consumerRecord.value();
                        long offset = consumerRecord.offset();

                        log.error("ApplyEventConsumer unable to read the record in offset: {}, with value: {}" +
                                "in topic: {}", offset, value, topic, thrownException);

                        /** Publish to the DLT topic, acknowledge once the processing of the event is complete **/
                        deadLetterPublisher().publish((ConsumerRecord<Object, Object>) consumerRecord,
                                thrownException, DeadLetterFailureReasons.SERIALIZATION_FAILURE);

                        int partition = consumerRecord.partition();
                        TopicPartition topicPartition = new TopicPartition(topic, partition);
                        consumer.seek(topicPartition, offset + 1L);
                    });

        }
    }

    private DeadLetterPublisher deadLetterPublisher() {
        return new DeadLetterPublisher(kafkaTemplate);
    }
}
