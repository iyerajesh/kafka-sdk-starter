package com.xylia.platform.events.configuration;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Optional;

@Builder
@Getter
@Setter
@ToString
public class SDKConfiguration {

    private Optional<String> sslKeyStoreLocation;
    private Optional<String> sslKeystorePassword;
    private Optional<String> sslTruststoreLocation;
    private Optional<String> sslTruststorePassword;

    public static final String SDK_CLIENT = "sdk-client";

    @Builder.Default
    private static Optional<String> consumerGroup = Optional.ofNullable(SDK_CLIENT);

    private static boolean disableDecryption = false;
    private static boolean enableMultiDC = false;

    private static long producerConnectTimeoutMs = 10000;

    public static Optional<String> getConsumerGroup() {
        return consumerGroup;
    }

    public static void setConsumerGroup(String consumerGroupString) {
        consumerGroup = Optional.ofNullable(consumerGroupString);
    }

    public static boolean isMultiDCEnabled() {
        return enableMultiDC;
    }

    public static void setEnableMultiDC(boolean multiDCFlag) {
        enableMultiDC = multiDCFlag;
    }

    public static long getProducerConnectTimeoutMs() {
        return producerConnectTimeoutMs;
    }
}
