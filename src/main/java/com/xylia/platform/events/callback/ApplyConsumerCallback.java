package com.xylia.platform.events.callback;

import com.xylia.platform.events.client.DeadLetterPublisher;
import io.cloudevents.v1.CloudEventImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.Acknowledgment;

/**
 * Event callback interface, which will be invoked when a new CloudEvent arrives.
 *
 * @author Rajesh Iyer
 */
public interface ApplyConsumerCallback {

    void processEvent(ConsumerRecord<Object, Object> consumerRecord, CloudEventImpl payload,
                      Acknowledgment acknowledgment, DeadLetterPublisher deadLetterPublisher);
}
