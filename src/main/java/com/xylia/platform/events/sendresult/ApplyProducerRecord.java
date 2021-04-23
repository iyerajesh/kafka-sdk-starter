package com.xylia.platform.events.sendresult;

import lombok.Getter;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * Apply Producer record
 * <p>
 * From Apache Kafka - extended by Rajesh Iyer
 *
 * @param <K> key
 * @param <V> value
 */
@Getter
public class ApplyProducerRecord<K, V> {

    private String topic;
    private Integer partition;
    private Headers headers;
    private K key;
    private V value;
    private Long timestamp;

    public ApplyProducerRecord(String topic, Integer partition, Long timestamp, K key, V value,
                               Iterable<Header> headers) {

        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null!");

        if (timestamp != null && timestamp < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null!", timestamp));
        }

        if (partition != null && partition < 0) {
            throw new IllegalArgumentException(
                    String.format("Invalid partition: %d. Partition should always be non-negative or null!", partition));
        }

        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.headers = new RecordHeaders(headers);
    }

    public ApplyProducerRecord(ProducerRecord<K, V> producerRecord) {
        this(producerRecord.topic(), producerRecord.partition(), producerRecord.timestamp(), producerRecord.key(),
                producerRecord.value(), producerRecord.headers());
    }

    @Override
    public String toString() {
        return "ApplyProducerRecord{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", headers=" + headers +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
