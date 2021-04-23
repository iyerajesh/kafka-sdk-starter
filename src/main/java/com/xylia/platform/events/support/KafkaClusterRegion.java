package com.xylia.platform.events.support;

public enum KafkaClusterRegion {

    PRIMARY("GSO"),
    SECONDARY("PHX");

    private String dataCenter;

    KafkaClusterRegion(String dataCenter) {
        this.dataCenter = dataCenter;
    }

    public String getDataCenter() {
        return this.dataCenter;
    }
}
