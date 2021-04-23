package com.xylia.platform.events.health;

import com.xylia.platform.events.config.KafkaConsumerConfig;
import com.xylia.platform.events.configuration.SDKConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

@Configuration
@Slf4j
public class KafkaConsumerMonitor {

    @Value("${spring.kafka.bootstrap-servers}")
    private String primaryBootstrapServers;

    @Autowired
    private KafkaConsumerConfig kafkaConsumerConfig;

    ReentrantLock MONITOR_LOCK = new ReentrantLock();

    private static final String monitoringConsumerGroupID = "monitoring_consumer_" + UUID.randomUUID().toString();

    public Map<TopicPartition, PartitionOffsets> getConsumerGroupOffsets(String topic, String groupId) {

        if (groupId == null)
            groupId = SDKConfiguration.getConsumerGroup().get();

        Map<TopicPartition, Long> logEndOffset = getLogEndOffset(topic, primaryBootstrapServers);
        KafkaConsumer consumer = createNewConsumer(groupId);

        BinaryOperator<PartitionOffsets> mergeFunction = (a, b) -> {
            throw new IllegalStateException();
        };

        Map<TopicPartition, PartitionOffsets> result = logEndOffset.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        entry -> (entry.getKey()),
                        entry -> {

                            OffsetAndMetadata committed = consumer.committed(entry.getKey());

                            if (committed != null)
                                return new PartitionOffsets(entry.getValue(), committed.offset(), entry.getKey().partition(), topic);
                            else /** this is only for the tests **/
                                return new PartitionOffsets(entry.getValue(), 0, entry.getKey().partition(), topic);
                        }, mergeFunction));

        return result;
    }

    public Map<TopicPartition, Long> getLogEndOffset(String topic, String host) {

        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
        KafkaConsumer<?, ?> consumer = createNewConsumer(monitoringConsumerGroupID);

        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = partitionInfoList.stream()
                .map(pi -> new TopicPartition(topic, pi.partition()))
                .collect(Collectors.toList());

        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);

        topicPartitions.forEach(topicPartition -> endOffsets.put(topicPartition, consumer.position(topicPartition)));
        consumer.close();

        return endOffsets;
    }

    private KafkaConsumer<?, ?> createNewConsumer(String monitoringConsumerGroupID) {

        MONITOR_LOCK.lock();
        KafkaConsumer kafkaConsumer = null;

        try {
            Properties properties;
            kafkaConsumer = new KafkaConsumer(kafkaConsumerConfig.monitoringConsumerConfigs(monitoringConsumerGroupID));
        } catch (Exception e) {
            log.error("Could not create a KafkaConsumer in the consumer monitor!", e);
        } finally {
            MONITOR_LOCK.unlock();
        }

        return kafkaConsumer;
    }
}
