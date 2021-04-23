package com.xylia.platform.events.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class ApplyKeyedPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        if (keyBytes == null)
            return 0;

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        return this.generateHashCode(new String(keyBytes)) % numPartitions;
    }

    private final int generateHashCode(String keyToBeHashed) {
        int hash = 0;

        for (int i = 0; i < keyToBeHashed.length(); i++) {
            hash = ((hash * 31)) + keyToBeHashed.charAt(i) & 0x7fffffff;
        }

        if (hash == 0)
            return 1;
        else
            return hash;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
