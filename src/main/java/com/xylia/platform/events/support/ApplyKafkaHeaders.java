package com.xylia.platform.events.support;

import org.springframework.kafka.support.Acknowledgment;

/**
 * The Kafka specific message headers constants.
 *
 * @author Rajesh Iyer
 */
public abstract class ApplyKafkaHeaders {

    /**
     * The prefix for Kafka headers.
     */
    public static final String PREFIX = "apply_";

    /**
     * The prefix for Kafka headers containing 'received' values.
     */
    public static final String RECEIVED = PREFIX + "received";

    /**
     * The header containing the topic when sending data to Kafka.
     */
    public static final String TOPIC = PREFIX + "topic";

    /**
     * The header containing the message key when sending data to Kafka.
     */
    public static final String MESSAGE_KEY = PREFIX + "messageKey";

    /**
     * The header containing the topic partition when sending data to Kafka.
     */
    public static final String PARTITION_ID = PREFIX + "partitionId";

    /**
     * The header for the partition offset.
     */
    public static final String OFFSET = PREFIX + "offset";

    /**
     * The header containing the raw data received from Kafka ({@code ConsumerRecord} or
     * {@code ConsumerRecords}). Usually used to enhance error messages.
     */
    public static final String RAW_DATA = PREFIX + "data";

    /**
     * The header containing the {@code RecordMetadata} object after successful send to the topic.
     */
    public static final String RECORD_METADATA = PREFIX + "recordMetadata";

    /**
     * The header for the {@link Acknowledgment}.
     */
    public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";

    /**
     * The header for the {@code Consumer} object.
     */
    public static final String CONSUMER = PREFIX + "consumer";

    /**
     * The header containing the topic from which the message was received.
     */
    public static final String RECEIVED_TOPIC = RECEIVED + "Topic";

    /**
     * The header containing the message key for the received message.
     */
    public static final String RECEIVED_MESSAGE_KEY = RECEIVED + "MessageKey";

    /**
     * The header containing the topic partition for the received message.
     */
    public static final String RECEIVED_PARTITION_ID = RECEIVED + "PartitionId";

    /**
     * The header for holding the {@link org.apache.kafka.common.record.TimestampType type} of timestamp.
     */
    public static final String TIMESTAMP_TYPE = PREFIX + "timestampType";

    /**
     * The header for holding the timestamp of the producer record.
     */
    public static final String TIMESTAMP = PREFIX + "timestamp";

    /**
     * The header for holding the timestamp of the consumer record.
     */
    public static final String RECEIVED_TIMESTAMP = PREFIX + "receivedTimestamp";

    /**
     * The header for holding the native headers of the consumer record; only provided
     * if no header mapper is present.
     */
    public static final String NATIVE_HEADERS = PREFIX + "nativeHeaders";

    /**
     * The header for a list of Maps of converted native Kafka headers. Used for batch
     * listeners; the map at a particular list position corresponds to the data in the
     * payload list position.
     */
    public static final String BATCH_CONVERTED_HEADERS = PREFIX + "batchConvertedHeaders";

    /**
     * The header containing information to correlate requests/replies.
     * Type: byte[].
     */
    public static final String CORRELATION_ID = PREFIX + "correlationId";

    /**
     * The header containing the default reply topic.
     * Type: byte[].
     */
    public static final String REPLY_TOPIC = PREFIX + "replyTopic";

    /**
     * The header containing a partition number on which to send the reply.
     * Type: binary (int) in byte[].
     */
    public static final String REPLY_PARTITION = PREFIX + "replyPartition";

    /**
     * Original event type of the record published, to be sent to the dead letter topic.
     */
    public static final String DLT_ORIGINAL_EVENT_TYPE = PREFIX + "dlt_original_event_type";

    /**
     * Failure reason, why the message was sent to the DLT topic.
     */
    public static final String DLT_FAILURE_REASON = PREFIX + "dlt_failure_reason";

    /**
     * Original application ID of the record published, to be sent to the dead letter topic.
     */
    public static final String DLT_ORIGINAL_APPLICATION_ID = PREFIX + "dlt_original_application_id";


    /**
     * Exception class name for a record published sent to a dead-letter topic.
     */
    public static final String DLT_EXCEPTION_FQCN = PREFIX + "dlt-exception-fqcn";

    /**
     * Exception stack trace for a record published to a dead-letter topic.
     */
    public static final String DLT_EXCEPTION_STACKTRACE = PREFIX + "dlt-exception-stacktrace";

    /**
     * Exception message for a record published to a dead-letter topic.
     */
    public static final String DLT_EXCEPTION_MESSAGE = PREFIX + "dlt-exception-message";

    /**
     * Original topic for a record published to a dead-letter topic.
     */
    public static final String DLT_ORIGINAL_TOPIC = PREFIX + "dlt-original-topic";

    /**
     * Original partition for a record published to a dead-letter topic.
     */
    public static final String DLT_ORIGINAL_PARTITION = PREFIX + "dlt-original-partition";

    /**
     * Original offset for a record published to a dead-letter topic.
     */
    public static final String DLT_ORIGINAL_OFFSET = PREFIX + "dlt-original-offset";

    /**
     * Original timestamp for a record published to a dead-letter topic.
     */
    public static final String DLT_ORIGINAL_TIMESTAMP = PREFIX + "dlt-original-timestamp";

    /**
     * Original timestamp type for a record published to a dead-letter topic.
     */
    public static final String DLT_ORIGINAL_TIMESTAMP_TYPE = PREFIX + "dlt-original-timestamp-type";

    /**
     * For inbound messages, the container's {@code group.id} consumer property.
     */
    public static final String GROUP_ID = PREFIX + "groupId";

    /**
     * For inbound messages, when container retries are enabled the delivery attempt.
     */
    public static final String DELIVERY_ATTEMPT = PREFIX + "deliveryAttempt";

}
