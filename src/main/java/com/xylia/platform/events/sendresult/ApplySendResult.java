package com.xylia.platform.events.sendresult;

import lombok.Getter;
import org.springframework.kafka.support.SendResult;

@Getter
public class ApplySendResult<K, V> {

    private ApplyProducerRecord<K, V> producerRecord;

    private ApplyRecordMetadata recordMetadata;

    public ApplySendResult(ApplyProducerRecord<K, V> producerRecord, ApplyRecordMetadata recordMetadata) {
        this.producerRecord = producerRecord;
        this.recordMetadata = recordMetadata;
    }

    public ApplySendResult(SendResult<K, V> sendResult) {
        this(new ApplyProducerRecord<>(sendResult.getProducerRecord()),
                new ApplyRecordMetadata(sendResult.getRecordMetadata()));
    }

    @Override
    public String toString() {
        return "ApplySendResult{" +
                "producerRecord=" + producerRecord +
                ", recordMetadata=" + recordMetadata +
                '}';
    }
}
