package com.xylia.platform.events.producer;

import com.xylia.platform.events.exception.ApplyPublishingException;
import com.xylia.platform.events.sendresult.ApplyProducerCallback;
import com.xylia.platform.events.sendresult.ApplySendResult;
import com.xylia.platform.events.support.KafkaClusterRegion;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MockProducerCallback implements ApplyProducerCallback {

    private KafkaClusterRegion kafkaClusterRegion;

    @Override
    public void onSuccess(ApplySendResult onSuccess) {
        log.info("Publish success");

    }

    @Override
    public void onFailure(ApplyPublishingException ex) {
        log.info("Publish failure!", ex);

    }

    @Override
    public void setKafkaClusterRegion(KafkaClusterRegion kafkaClusterRegion) {
        this.kafkaClusterRegion = kafkaClusterRegion;
    }

    @Override
    public KafkaClusterRegion getKafkaClusterRegion() {
        return kafkaClusterRegion;
    }
}
