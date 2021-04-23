package com.xylia.platform.events.client;

import com.xylia.platform.events.support.ApplyKafkaHeaders;
import com.xylia.platform.events.support.DeadLetterFailureReasons;
import io.cloudevents.v1.CloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.joda.time.DateTime;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Throwables.getStackTraceAsString;

/**
 * DeadLetterPublisher class, responsible for publishing a message to the .DLT topic, with custom Apply Kafka headers.
 *
 * @author Rajesh Iyer
 */
@Slf4j
public class DeadLetterPublisher {

    private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(DeadLetterPublisher.class));

    public static final String DLT_EXTENSION = ".DLT";
    private static final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>
            DEFAULT_DESTINATION_RESOLVER = (cr, e) -> new TopicPartition(cr.topic() + DLT_EXTENSION, cr.partition());

    private KafkaTemplate<Object, Object> template;
    private BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition> destinationResolver;

    public DeadLetterPublisher(KafkaTemplate<Object, Object> template) {

        Assert.isTrue(!ObjectUtils.isEmpty(template), "At least one Kafka template is required!");
        this.template = template;
        this.destinationResolver = DEFAULT_DESTINATION_RESOLVER;
    }

    public void publish(ConsumerRecord<Object, Object> record, Exception exception, DeadLetterFailureReasons deadLetterFailureReasons) {
        this.publishInternal(record, null, null, exception, deadLetterFailureReasons);
    }

    public void publish(ConsumerRecord<Object, Object> record, Acknowledgment acknowledgment, Exception exception, DeadLetterFailureReasons deadLetterFailureReasons) {
        this.publishInternal(record, null, acknowledgment, exception, deadLetterFailureReasons);
    }

    public void publish(ConsumerRecord<Object, Object> record, CloudEventImpl cloudEvent, Acknowledgment acknowledgment, Exception exception, DeadLetterFailureReasons deadLetterFailureReasons) {
        this.publishInternal(record, cloudEvent, acknowledgment, exception, deadLetterFailureReasons);
    }

    public void publish(ProducerRecord<Object, Object> record, Throwable exception, DeadLetterFailureReasons deadLetterFailureReasons) {
        publishToDLTTopic(enhanceHeaders(record, exception, deadLetterFailureReasons), this.template);
    }

    private void publishInternal(ConsumerRecord<Object, Object> record, CloudEventImpl cloudEvent,
                                 Acknowledgment acknowledgment, Exception exception, DeadLetterFailureReasons deadLetterFailureReasons) {

        /** Acknowledge the current offset, before the DLT write **/
        if (acknowledgment != null)
            acknowledgment.acknowledge();

        log.info("Dead Letter publisher successfully acknowledged commit at timestamp: "
                + record.timestamp() + " with offset: " + record.offset());

        TopicPartition topicPartition = this.destinationResolver.apply(record, exception);
        RecordHeaders headers = new RecordHeaders(record.headers().toArray());
        enhanceHeaders(headers, record, cloudEvent, exception, deadLetterFailureReasons);

        boolean isKey = false;
        DeserializationException deserializationException = ListenerUtils
                .getExceptionFromHeader(record, ErrorHandlingDeserializer.VALUE_DESERIALIZER_EXCEPTION_HEADER, LOGGER);
        if (deserializationException == null) {
            deserializationException = ListenerUtils
                    .getExceptionFromHeader(record, ErrorHandlingDeserializer.KEY_DESERIALIZER_EXCEPTION_HEADER, LOGGER);
            isKey = true;
        }

        ProducerRecord<Object, Object> outRecord = createProducerRecord(record, topicPartition, headers,
                deserializationException == null ? null : deserializationException.getData(), isKey);

        publishToDLTTopic(outRecord, this.template);
    }

    private void publishToDLTTopic(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate) {
        try {
            kafkaTemplate.send(outRecord).addCallback(result -> {
                log.warn("Successful dead-letter publication with headers: "
                        + result.getProducerRecord().headers().toString());
            }, ex -> {
                log.error("Dead letter publication failed for: " + outRecord, (Exception) ex);
            });
        } catch (Exception e) {
            log.error("Dead Letter publication failed for: " + outRecord, e);
        }
    }

    private void enhanceHeaders(RecordHeaders kafkaHeaders, ConsumerRecord<?, ?> record, CloudEventImpl cloudEvent,
                                Exception exception, DeadLetterFailureReasons deadLetterFailureReasons) {

        if (Optional.ofNullable(cloudEvent).isPresent()) {
            kafkaHeaders.add(
                    new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_EVENT_TYPE, cloudEvent.getAttributes().getType().getBytes(StandardCharsets.UTF_8)));
        }

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_FAILURE_REASON, deadLetterFailureReasons.getFailureReason().getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(record.partition()).getBytes()));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_OFFSET, String.valueOf(record.offset()).getBytes()));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_FAILURE_REASON, deadLetterFailureReasons.getFailureReason().getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_FAILURE_REASON, deadLetterFailureReasons.getFailureReason().getBytes(StandardCharsets.UTF_8)));

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE, record.timestampType().toString().getBytes()));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_TIMESTAMP, new DateTime(record.timestamp()).toString().getBytes()));

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_EXCEPTION_FQCN, exception.getClass().getName().getBytes(StandardCharsets.UTF_8)));

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_EXCEPTION_MESSAGE, exception.getMessage().getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_EXCEPTION_STACKTRACE, getStackTraceAsString(exception).getBytes(StandardCharsets.UTF_8)));
    }

    private ProducerRecord<Object, Object> enhanceHeaders(ProducerRecord<Object, Object> record, Throwable exception, DeadLetterFailureReasons deadLetterFailureReasons) {

        RecordHeaders kafkaHeaders = new RecordHeaders(record.headers());
        if (Optional.ofNullable(record.value()).isPresent() && record.value() instanceof CloudEventImpl) {
            CloudEventImpl cloudEvent = (CloudEventImpl) record.value();

            if (cloudEvent.getAttributes().getType() != null) {
                kafkaHeaders.add(
                        new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_EVENT_TYPE, cloudEvent.getAttributes().getType().getBytes(StandardCharsets.UTF_8)));

            }
        }

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_FAILURE_REASON, deadLetterFailureReasons.getFailureReason().getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(record.partition()).getBytes()));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_FAILURE_REASON, deadLetterFailureReasons.getFailureReason().getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_FAILURE_REASON, deadLetterFailureReasons.getFailureReason().getBytes(StandardCharsets.UTF_8)));

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_ORIGINAL_TIMESTAMP, new DateTime(record.timestamp()).toString().getBytes()));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_EXCEPTION_FQCN, exception.getClass().getName().getBytes(StandardCharsets.UTF_8)));

        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_EXCEPTION_MESSAGE, exception.getMessage().getBytes(StandardCharsets.UTF_8)));
        kafkaHeaders.add(
                new RecordHeader(ApplyKafkaHeaders.DLT_EXCEPTION_STACKTRACE, getStackTraceAsString(exception).getBytes(StandardCharsets.UTF_8)));

        ProducerRecord<Object, Object> dltRecord = new ProducerRecord<>(record.topic() + DLT_EXTENSION,
                record.partition(), record.key(), record.value(), kafkaHeaders);
        return dltRecord;
    }

    private ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record, TopicPartition topicPartition, RecordHeaders recordHeaders, @Nullable byte[] data, boolean isKey) {

        return new ProducerRecord<Object, Object>(topicPartition.topic(),
                topicPartition.partition() < 0 ? null : topicPartition.partition(),
                isKey && data != null ? data : record.key(),
                data == null || isKey ? record.value() : data,
                recordHeaders);
    }
}

