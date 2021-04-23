package com.xylia.platform.events.producer;

import com.xylia.platform.events.configuration.SDKConfiguration;
import com.xylia.platform.events.exception.ApplyPublishingException;
import com.xylia.platform.events.model.ApplyEventTypes;
import com.xylia.platform.events.sendresult.ApplyProducerCallback;
import com.xylia.platform.events.sendresult.ApplySendResult;
import com.xylia.platform.events.support.KafkaClusterRegion;
import io.cloudevents.v1.CloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class ApplyEventProducer implements Producer {

    public static final String APPLICATION_ID = "applicationId";
    @Autowired
    @Qualifier("primaryCluster")
    private KafkaTemplate kafkaTemplate;

    @Autowired
    @Qualifier("secondaryCluster")
    private KafkaTemplate secondaryKafkaTemplate;

    @Autowired
    @Qualifier("deadLetterProducer")
    private KafkaTemplate deadLetterProducer;


    @Override
    public void publish(CloudEventImpl payload) throws ApplyPublishingException {

        String topic = ApplyEventTypes.getTopicByType(payload.getAttributes().getType());
        if (topic == null)
            throw new ApplyPublishingException("Cannot find topic for the CloudEvent type!");

        sendMessageToPrimaryCluster(topic, (String) payload.getExtensions().get(APPLICATION_ID), null, payload, null);
    }

    @Override
    public void publish(CloudEventImpl payload, ApplyProducerCallback callback) {

        String topic = ApplyEventTypes.getTopicByType(payload.getAttributes().getType());
        if (topic == null)
            throw new ApplyPublishingException("Cannot find topic for the CloudEvent type!");

        sendMessageToPrimaryCluster(topic, (String) payload.getExtensions().get(APPLICATION_ID),
                null, payload, callback);
    }

    @Override
    public void publish(ApplyEventTypes applyEventTypes, CloudEventImpl payload, ApplyProducerCallback callback) {

        String topic = applyEventTypes.getTopic();
        if (topic == null)
            throw new ApplyPublishingException("Cannot find topic for the CloudEvent type!");

        sendMessageToPrimaryCluster(topic, (String) payload.getExtensions().get(APPLICATION_ID),
                null, payload, callback);

    }

    private final void sendMessageToPrimaryCluster(String topicName, String messageKey, Map<String, String> headerMap,
                                                   CloudEventImpl cloudEvent, ApplyProducerCallback callback) {

        try {
            ProducerRecord<String, CloudEventImpl> producerRecord = new ProducerRecord<>(topicName, messageKey, cloudEvent);

            log.info(String.format("Attempting to publish event to the primary cluster: %s, " +
                    "to: topicName = %s, with payload: %s", cloudEvent.getAttributes().getType(), topicName, cloudEvent.toString()));

            ListenableFuture<SendResult> futureSendResult = kafkaTemplate.send(producerRecord);
            futureSendResult.addCallback(new ListenableFutureCallback<SendResult>() {

                @Override
                public void onFailure(Throwable throwable) {

                    log.error("Failed publishing to the primary cluster with: topic = {}, error = {}",
                            topicName, throwable.getMessage(), new Exception(throwable));

                    if (callback != null) {
                        callback.onFailure(new ApplyPublishingException(
                                String.format("Failed publishing to the primary cluster with: topic = %s, error = %s",
                                        topicName, throwable.getMessage(), new Exception(throwable))));

                        callback.setKafkaClusterRegion(KafkaClusterRegion.PRIMARY);
                    }
                }

                @Override
                public void onSuccess(SendResult result) {

                    log.info(String.format("Message published to the primary cluster to: topic: %s, message: %s, " +
                                    "in partition: %s, at offset: %s", topicName, cloudEvent,
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));

                    if (callback != null) {
                        callback.onSuccess(new ApplySendResult(result));
                        callback.setKafkaClusterRegion(KafkaClusterRegion.PRIMARY);
                    }


                }
            });

            futureSendResult.get(SDKConfiguration.getProducerConnectTimeoutMs(), TimeUnit.MILLISECONDS);

        } catch (Exception exception) {

            if (exception instanceof InterruptedException) // fix for sonar violation
                Thread.currentThread().interrupt();

            log.error(String.format("Failed in publishing message to the primary cluster with: topic: %s, error: %s",
                    topicName, exception.getMessage()), exception);

            /** sending message to the secondary cluster **/
            log.warn("Going to try to publish to the secondary Kafka cluster!");
            sendMessageToSecondaryCluster(topicName, messageKey, headerMap, cloudEvent, callback);
        }
    }

    private final void sendMessageToSecondaryCluster(String topicName, String messageKey, Map<String, String> headerMap,
                                                     CloudEventImpl cloudEvent, ApplyProducerCallback callback) {

        try {
            ProducerRecord<String, CloudEventImpl> producerRecord = new ProducerRecord<>(topicName, messageKey, cloudEvent);

            log.info(String.format("Attempting to publish event to the primary cluster: %s, " +
                    "to: topicName = %s, with payload: %s", cloudEvent.getAttributes().getType(), topicName, cloudEvent.toString()));

            ListenableFuture<SendResult> futureSendResult = kafkaTemplate.send(producerRecord);
            futureSendResult.addCallback(new ListenableFutureCallback<SendResult>() {

                @Override
                public void onFailure(Throwable throwable) {

                    log.error("Failed publishing to the primary cluster with: topic = {}, error = {}",
                            topicName, throwable.getMessage(), new Exception(throwable));

                    if (callback != null) {
                        callback.onFailure(new ApplyPublishingException(
                                String.format("Failed publishing to the primary cluster with: topic = %s, error = %s",
                                        topicName, throwable.getMessage(), new Exception(throwable))));

                        callback.setKafkaClusterRegion(KafkaClusterRegion.SECONDARY);
                    }
                }

                @Override
                public void onSuccess(SendResult result) {

                    log.info(String.format("Message published to the primary cluster to: topic: %s, message: %s, " +
                                    "in partition: %s, at offset: %s", topicName, cloudEvent,
                            result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));

                    if (callback != null) {
                        callback.onSuccess(new ApplySendResult(result));
                        callback.setKafkaClusterRegion(KafkaClusterRegion.SECONDARY);
                    }
                }
            });
            futureSendResult.get(SDKConfiguration.getProducerConnectTimeoutMs(), TimeUnit.MILLISECONDS);

        } catch (Exception exception) {

            if (exception instanceof InterruptedException) // fix for sonar violation
                Thread.currentThread().interrupt();

            log.error(String.format("Failed in publishing message to the primary cluster with: topic: %s, error: %s",
                    topicName, exception.getMessage()), exception);
        }
    }

}
