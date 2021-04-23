package com.xylia.platform.events.sendresult;

import com.xylia.platform.events.exception.ApplyPublishingException;
import com.xylia.platform.events.support.KafkaClusterRegion;

public interface ApplyProducerCallback {

    void onSuccess(ApplySendResult onSuccess);

    void onFailure(ApplyPublishingException ex);

    void setKafkaClusterRegion(KafkaClusterRegion kafkaClusterRegion);

    KafkaClusterRegion getKafkaClusterRegion();
}
