package com.xylia.platform.events.health;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xylia.platform.events.config.KafkaConsumerConfig;
import com.xylia.platform.events.configuration.SDKConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class KafkaHealthIndicator {

    public static final int TIMEOUT_MS = 2000;

    @Autowired
    KafkaConsumerConfig kafkaConsumerConfig;

    /**
     * Returns an indication of the health of the Kafka broker.
     *
     * @return the health of the Kafka broker.
     */
    public Health health() {

        Health primaryClusterHealth = primaryClusterHealth();

        if (SDKConfiguration.isMultiDCEnabled()) {
            final Health secondaryClusterHealth = secondaryClusterHealth();
            if (primaryClusterHealth.getStatus().getCode().equals("DOWN"))
                return secondaryClusterHealth;
        }
        return primaryClusterHealth;
    }

    public Health primaryClusterHealth() {

        Properties props;
        try (AdminClient adminClient = AdminClient.create(kafkaConsumerConfig.primaryAdminConfigs())) {

            DescribeClusterOptions describeClusterOptions = new DescribeClusterOptions().timeoutMs(TIMEOUT_MS);
            DescribeClusterResult describeClusterResult = adminClient.describeCluster(describeClusterOptions);

            return Health.up()
                    .withDetails(getNodeDetails(describeClusterResult))
                    .build();
        } catch (Exception e) {
            log.error("Kafka health check failed!", e);
            return Health.down()
                    .withDetail("Kafka health check failed", "Exception occured during health check").build();
        }
    }

    public Health secondaryClusterHealth() {

        Properties props;
        try (AdminClient adminClient = AdminClient.create(kafkaConsumerConfig.secondaryAdminConfigs())) {

            DescribeClusterOptions describeClusterOptions = new DescribeClusterOptions().timeoutMs(TIMEOUT_MS);
            DescribeClusterResult describeClusterResult = adminClient.describeCluster(describeClusterOptions);

            return Health.up()
                    .withDetails(getNodeDetails(describeClusterResult))
                    .build();
        } catch (Exception e) {
            log.error("Kafka health check failed!", e);
            return Health.down()
                    .withDetail("Kafka health check failed", "Exception occured during health check").build();
        }
    }

    private final Map<String, List<NodeDetails>> getNodeDetails(DescribeClusterResult describeClusterResult)
            throws ExecutionException, InterruptedException {

        final List<NodeDetails> nodeDetailsList = Lists.newArrayList();
        final Map<String, List<NodeDetails>> clusterDetails = Maps.newConcurrentMap();

        final Collection<Node> nodes = describeClusterResult.nodes().get();
        for (final Node node : nodes)
            nodeDetailsList.add(new NodeDetails(node.id(), node.host(), node.port(), node.rack()));

        clusterDetails.put("nodes", nodeDetailsList);
        return clusterDetails;
    }

    /**
     * Used only for the tests
     *
     * @param adminClient
     * @return
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public Health health(AdminClient adminClient) throws ExecutionException, InterruptedException {

        DescribeClusterOptions describeClusterOptions = new DescribeClusterOptions().timeoutMs(TIMEOUT_MS);
        return Health.up().build();
    }
}
