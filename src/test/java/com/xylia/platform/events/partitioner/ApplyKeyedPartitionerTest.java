package com.xylia.platform.events.partitioner;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.Integer.valueOf;
import static java.util.Arrays.asList;
import static org.springframework.test.util.AssertionErrors.assertEquals;

@Slf4j
public class ApplyKeyedPartitionerTest {

    final int lower = 00001;
    final int upper = 99999;
    public static final String DATA = "data";

    final String julianDate = "2020260";
    final String prospectCode = "69";
    String countryCodes[] = new String[]{"022", "USD", "IND"};

    private static final Node[] NODES = new Node[]{
            new Node(0, "localhost", 99),
            new Node(1, "localhost", 100),
            new Node(12, "localhost", 101)
    };

    private static final String TOPIC = "test";

    private static final List<PartitionInfo> PARTITIONS = asList(
            new PartitionInfo(TOPIC, 1, null, NODES, NODES),
            new PartitionInfo(TOPIC, 2, NODES[1], NODES, NODES),
            new PartitionInfo(TOPIC, 3, null, NODES, NODES),
            new PartitionInfo(TOPIC, 4, NODES[1], NODES, NODES),
            new PartitionInfo(TOPIC, 5, null, NODES, NODES),
            new PartitionInfo(TOPIC, 0, NODES[0], NODES, NODES));

    @Test
    public void testKeyPartitionIsStable() throws IOException {

        List<PartitionAllocation> allocationList = Lists.newArrayList();
        Set<String> pcnList = readFileContents();
        for (String pcn : pcnList) {

            byte[] applicationIdBytes = pcn.getBytes();

            final Partitioner partitioner = new ApplyKeyedPartitioner();
            final Cluster cluster = new Cluster("clusterId", asList(NODES), PARTITIONS,
                    Collections.emptySet(), Collections.emptySet());

            int partition = partitioner.partition("test", null, applicationIdBytes, null, null, cluster);
            final PartitionAllocation partitionAllocation = new PartitionAllocation();

            partitionAllocation.setPartition(valueOf(partition));
            partitionAllocation.setApplicationId(new String(applicationIdBytes));

            allocationList.add(partitionAllocation);

            assertEquals("Same key should yield same partition", partition,
                    partitioner.partition("test", null, applicationIdBytes, null, null, cluster));
        }

        List<PartitionAllocation> partition0Allocation = allocationList.stream()
                .filter(partitionInfo -> partitionInfo.getPartition().equals(valueOf(0)))
                .collect(Collectors.toList());

        List<PartitionAllocation> partition1Allocation = allocationList.stream()
                .filter(partitionInfo -> partitionInfo.getPartition().equals(valueOf(1)))
                .collect(Collectors.toList());

        List<PartitionAllocation> partition2Allocation = allocationList.stream()
                .filter(partitionInfo -> partitionInfo.getPartition().equals(valueOf(2)))
                .collect(Collectors.toList());

        List<PartitionAllocation> partition3Allocation = allocationList.stream()
                .filter(partitionInfo -> partitionInfo.getPartition().equals(valueOf(3)))
                .collect(Collectors.toList());

        List<PartitionAllocation> partition4Allocation = allocationList.stream()
                .filter(partitionInfo -> partitionInfo.getPartition().equals(valueOf(4)))
                .collect(Collectors.toList());

        List<PartitionAllocation> partition5Allocation = allocationList.stream()
                .filter(partitionInfo -> partitionInfo.getPartition().equals(valueOf(5)))
                .collect(Collectors.toList());

        System.out.println("Partition 0 allocation: " + partition0Allocation.size());
        System.out.println("Partition 1 allocation: " + partition1Allocation.size());
        System.out.println("Partition 2 allocation: " + partition2Allocation.size());
        System.out.println("Partition 3 allocation: " + partition3Allocation.size());
        System.out.println("Partition 4 allocation: " + partition4Allocation.size());
        System.out.println("Partition 5 allocation: " + partition5Allocation.size());
    }


    @Test
    public void testPartitionAllocationWithSingleKey() throws IOException {

        List<PartitionAllocation> allocationList = Lists.newArrayList();

        byte[] applicationIdBytes = "2020856250000IUOJM".getBytes();

        final Partitioner partitioner = new ApplyKeyedPartitioner();
        final Cluster cluster = new Cluster("clusterId", asList(NODES), PARTITIONS,
                Collections.emptySet(), Collections.emptySet());

        int partition = partitioner.partition("test", null, applicationIdBytes, null, null, cluster);

        System.out.println("partition allocated is: " + partition);

        final PartitionAllocation partitionAllocation = new PartitionAllocation();

        partitionAllocation.setPartition(valueOf(partition));
        partitionAllocation.setApplicationId(new String(applicationIdBytes));

        allocationList.add(partitionAllocation);

        assertEquals("Same key should yield same partition", partition,
                partitioner.partition("test", null, applicationIdBytes, null, null, cluster));
    }

    private byte[] generateApplicationId() {
        StringBuffer applicationId = new StringBuffer();
        applicationId.append(julianDate)
                .append(prospectCode)
                .append((int) (Math.random() * (upper - lower)) + lower)
                .append(countryCodes[0]);

        return applicationId.toString().getBytes();
    }

    private Set<String> readFileContents() throws IOException {

        InputStream resource = Thread.currentThread()
                .getContextClassLoader().getResourceAsStream("DATA" + "/" + "pcn-data.txt");
        BufferedReader reader = new BufferedReader(new InputStreamReader(resource));

        return reader.lines()
                .collect(Collectors.toSet());
    }

}