package com.xylia.platform.events.producer;

import com.xylia.platform.events.model.ApplyEventTypes;
import com.xylia.platform.events.sendresult.ApplyProducerCallback;
import io.cloudevents.v1.CloudEventImpl;

/**
 * Kafka producer interface
 *
 * @author Rajesh Iyer
 */
public interface Producer {

    /**
     * Publish cloud event message to Kafka, with a callback.
     *
     * @param payload - message payload or the message which needs to be sent to the Kafka topic.
     */
    void publish(CloudEventImpl payload);

    /**
     * Publish cloud event message, to Kafka with a call back.
     *
     * @param payload  - The cloud event message payload.
     * @param callback - callback function, that executes the message after successfully sent.
     */
    void publish(CloudEventImpl payload, ApplyProducerCallback callback);

    /**
     * Publish cloud event message, to Kafka with a call back.
     *
     * @param payload  - The cloud event message payload.
     * @param callback - callback function, that executes the message after successfully sent.
     */
    void publish(ApplyEventTypes applyEventTypes, CloudEventImpl payload, ApplyProducerCallback callback);
}
