package com.xylia.platform.events.partitioner;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class PartitionAllocation {

    private Integer partition;
    private String applicationId;
}
