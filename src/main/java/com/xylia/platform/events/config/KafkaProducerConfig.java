package com.xylia.platform.events.config;

import com.xylia.platform.events.partitioner.ApplyKeyedPartitioner;
import com.xylia.platform.events.serialization.CloudEventKafkaSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String primaryBootstrapServers;

    @Value("${spring.kafka.secondary-bootstrap-servers:}")
    private String secondaryBootstrapServers;

    @Value("${spring.kafka.producer.enable-idempotence-config}")
    private String enableIdempotence;

    @Value("${spring.kafka.producer.acks-config}")
    private String acksConfig;

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

    @Bean
    public Map<String, Object> primaryProducerConfigs() {

        String root = System.getProperty("user.dir");

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, primaryBootstrapServers);

        addCommonConfigs(root, props);
        return props;
    }

    @Bean
    public Map<String, Object> deadLetterProducerConfigs() {

        String root = System.getProperty("user.dir");

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, primaryBootstrapServers);

        addCommonConfigs(root, props);
        return props;
    }


    private void addCommonConfigs(String root, Map<String, Object> props) {

        /** Kakfa producer retries and timeout settings **/
        props.put(ProducerConfig.RETRIES_CONFIG, numberOfRetries);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventKafkaSerializer.class);

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence);
        props.put(ProducerConfig.ACKS_CONFIG, acksConfig);

        /** setting the apply custom partitioner **/
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, ApplyKeyedPartitioner.class);

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointIdentificationAlgorithm);

        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, root + sslKeystoreLocationConfig);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, root + sslKeystorePass);

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, root + sslTruststoreLocationConfig);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, root + sslTruststorePass);
    }

    @Bean
    public ProducerFactory<String, Object> primaryProducerFactory() {
        return new DefaultKafkaProducerFactory<>(primaryProducerConfigs());
    }

    @Bean("primaryCluster")
    public KafkaTemplate<String, Object> primaryClusterKafkaTemplate() {
        return new KafkaTemplate<String, Object>(primaryProducerFactory());
    }

    @Bean("secondaryCluster")
    public KafkaTemplate<String, Object> secondaryClusterKafkaTemplate() {
        return new KafkaTemplate<String, Object>(primaryProducerFactory());
    }

    @Bean
    public ProducerFactory<Object, Object> deadLetterProducerFactory() {
        return new DefaultKafkaProducerFactory<>(deadLetterProducerConfigs());
    }

    @Bean("deadLetterProducer")
    public KafkaTemplate<Object, Object> deadLetterProducer() {
        return new KafkaTemplate<>(deadLetterProducerFactory());
    }
}
