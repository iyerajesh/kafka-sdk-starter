package com.xylia.platform.events.health;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PartitionOffsets {

    private long endOffset;
    private long currentOffset;
    private long offsetLag;
    private int partition;
    private String topic;

    public PartitionOffsets(long endOffset, long currentOffset, int partition, String topic) {
        this.endOffset = endOffset;
        this.currentOffset = currentOffset;
        this.offsetLag = (endOffset - currentOffset);
        this.partition = partition;
        this.topic = topic;
    }
}
