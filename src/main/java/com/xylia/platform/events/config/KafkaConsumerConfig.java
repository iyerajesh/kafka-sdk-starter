package com.xylia.platform.events.config;

import com.google.common.collect.Maps;
import com.xylia.platform.events.configuration.SDKConfiguration;
import com.xylia.platform.events.consumer.ConsumerErrorHandler;
import com.xylia.platform.events.partitioner.ApplyKeyedPartitioner;
import com.xylia.platform.events.serialization.CloudEventKafkaDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConsumerConfig {

    @Autowired
    ConsumerErrorHandler consumerErrorHandler;

    @Value("${spring.kafka.bootstrap-servers}")
    private String primaryBootstrapServers;

    @Value("${spring.kafka.secondary-bootstrap-servers:}")
    private String secondaryBootstrapServers;

    @Value("${spring.kafka.properties.ssl.endpoint.identification.algorithm}")
    private String sslEndpointIdentificationAlgorithm;

    @Value("${spring.kafka.properties.ssl.keystore.location.config:}")
    private String sslKeystoreLocationConfig;

    @Value("${spring.kafka.properties.ssl.keystore.password:}")
    private String sslKeystorePass;

    @Value("${spring.kafka.properties.ssl.truststore.location.config:}")
    private String sslTruststoreLocationConfig;

    @Value("${spring.kafka.properties.ssl.truststore.password:}")
    private String sslTruststorePass;

    @Value("${spring.kafka.properties.sasl.mechanism}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.retries}")
    private String numberOfRetries;

    @Value("${spring.kafka.properties.request.timeout.ms}")
    private String requestTimeoutMs;

    @Value("${spring.kafka.properties.retry.backoff.ms}")
    private String retryBackoffMs;

    @Value("${spring.kafka.properties.security.protocol}")
    private String securityProtocol;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    public Map<String, Object> primaryConsumerConfigs() {
        String root = System.getProperty("user.dir");
        Map<String, Object> props = Maps.newHashMap();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, primaryBootstrapServers);

        if (secondaryBootstrapServers != null && !secondaryBootstrapServers.equalsIgnoreCase(""))
            SDKConfiguration.setEnableMultiDC(true);

        addCommonConsumerConfigs(root, props);
        return props;
    }

    public Map<String, Object> monitoringConsumerConfigs(String monitoringConsumerGroupID) {
        String root = System.getProperty("user.dir");
        Map<String, Object> props = Maps.newHashMap();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, primaryBootstrapServers);

        if (secondaryBootstrapServers != null && !secondaryBootstrapServers.equalsIgnoreCase(""))
            SDKConfiguration.setEnableMultiDC(true);

        addCommonConsumerConfigs(root, props);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, monitoringConsumerGroupID);

        return props;
    }

    public Map<String, Object> primaryAdminConfigs() {

        String root = System.getProperty("user.dir");
        Map<String, Object> props = Maps.newHashMap();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, primaryBootstrapServers);

        if (secondaryBootstrapServers != null && !secondaryBootstrapServers.equalsIgnoreCase(""))
            SDKConfiguration.setEnableMultiDC(true);

        addCommonConsumerConfigs(root, props);
        return props;
    }

    public Map<String, Object> secondaryAdminConfigs() {

        String root = System.getProperty("user.dir");
        Map<String, Object> props = Maps.newHashMap();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, secondaryBootstrapServers);

        addCommonConsumerConfigs(root, props);
        return props;
    }

    public Map<String, Object> secondaryConsumerConfigs() {

        String root = System.getProperty("user.dir");
        Map<String, Object> props = Maps.newHashMap();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, secondaryBootstrapServers);

        addCommonConsumerConfigs(root, props);
        return props;
    }

    private void addCommonConsumerConfigs(String root, Map<String, Object> props) {

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, CloudEventKafkaDeserializer.class);

        if (SDKConfiguration.getConsumerGroup().isPresent())
            props.put(ConsumerConfig.GROUP_ID_CONFIG, SDKConfiguration.getConsumerGroup().get());
        else
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        /** enable manual acknowledgement **/
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);

        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, root + sslKeystoreLocationConfig);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, root + sslKeystorePass);

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, root + sslTruststoreLocationConfig);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, root + sslTruststorePass);
    }

    public ConsumerFactory<String, Message> primaryConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(primaryConsumerConfigs());
    }

    public ConsumerFactory<String, Message> secondaryConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(secondaryConsumerConfigs());
    }

    @Bean("primaryConsumerCluster")
    public ConcurrentKafkaListenerContainerFactory<String, Message> primaryKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();

        // TODO: Remove, if you want Kafka to automatically acknowledge.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(primaryConsumerFactory());

        /** set error handler for consumer read failure **/
        factory.setErrorHandler(consumerErrorHandler);

        return factory;
    }

    @Bean("secondaryConsumerCluster")
    public ConcurrentKafkaListenerContainerFactory<String, Message> secondaryKafkaListenerContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, Message> factory = new ConcurrentKafkaListenerContainerFactory<>();

        // TODO: Remove, if you want Kafka to automatically acknowledge.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(secondaryConsumerFactory());

        /** set error handler for consumer read failure **/
        factory.setErrorHandler(consumerErrorHandler);

        return factory;
    }

}

class OnMultiDCConditionEnabled implements Condition {

    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        return SDKConfiguration.isMultiDCEnabled();
    }
}
