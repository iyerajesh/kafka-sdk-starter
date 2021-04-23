package com.xylia.platform.events.sendresult;

import lombok.Getter;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Partition value of the record, without the partition assigned.
 */
@Getter
public final class ApplyRecordMetadata {

    private static final int UNKNOWN_PARTITION = -1;

    private long offset;
    private long timestamp;
    private int serializedKeySize;
    private int serializedValueSize;
    private int partition;
    private String topic;

    private volatile Long checksum;

    public ApplyRecordMetadata(int partition, String topic, long offset, long timestamp, int serializedKeySize, int serializedValueSize) {

        this.offset = offset;
        this.timestamp = timestamp;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.partition = partition;
        this.topic = topic;
    }

    public ApplyRecordMetadata(RecordMetadata recordMetadata) {
        this(recordMetadata.partition(), recordMetadata.topic(), recordMetadata.offset(), recordMetadata.timestamp(),
                recordMetadata.serializedKeySize(), recordMetadata.serializedValueSize());
    }

    @Override
    public String toString() {
        return "ApplyRecordMetadata{" +
                "offset=" + offset +
                ", timestamp=" + timestamp +
                ", serializedKeySize=" + serializedKeySize +
                ", serializedValueSize=" + serializedValueSize +
                ", partition=" + partition +
                ", topic='" + topic + '\'' +
                ", checksum=" + checksum +
                '}';
    }
}
