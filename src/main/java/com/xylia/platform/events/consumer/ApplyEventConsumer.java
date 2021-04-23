package com.xylia.platform.events.consumer;

import com.xylia.platform.events.callback.ApplyConsumerCallback;
import com.xylia.platform.events.callback.EventCallbackRegistry;
import com.xylia.platform.events.client.DeadLetterPublisher;
import com.xylia.platform.events.exception.ApplyConsumerException;
import com.xylia.platform.events.support.DeadLetterFailureReasons;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.CloudEventImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.xylia.platform.events.callback.EventCallbackRegistry.getCallbackClass;
import static com.xylia.platform.events.configuration.SDKConfiguration.isMultiDCEnabled;

@Component
@Slf4j
public class ApplyEventConsumer {

    private ApplyKafkaListener applyKafkaListenerPrimary;
    private ApplyKafkaListener applyKafkaListenerSecondary;

    @Autowired
    @Qualifier("deadLetterProducer")
    private KafkaTemplate kafkaProducer;

    @Autowired
    private EventCallbackRegistry eventCallbackRegistry;

    public ApplyEventConsumer(@Qualifier("primaryConsumerCluster") ConcurrentKafkaListenerContainerFactory<String, Message> primaryKafkaListenerContainerFactory,
                              @Qualifier("secondaryConsumerCluster") ConcurrentKafkaListenerContainerFactory<String, Message> secondaryKafkaListenerContainerFactory) {

        this.applyKafkaListenerPrimary =
                new ApplyKafkaListener(primaryKafkaListenerContainerFactory);

        this.applyKafkaListenerSecondary =
                new ApplyKafkaListener(secondaryKafkaListenerContainerFactory);
    }

    public void listenToRegisteredTopics() {

        Set<String> registeredTopics = eventCallbackRegistry.getRegisteredTopics();

        log.info("Topics registered for consumption with the primary cluster: {}", registeredTopics);

        applyKafkaListenerPrimary.register(() -> registeredTopics, () -> messageListener());
        applyKafkaListenerPrimary.start();

        if (isMultiDCEnabled()) {

            log.info("Topics registered for consumption with the secondary cluster: {}", registeredTopics);
            applyKafkaListenerSecondary.register(() -> registeredTopics, () -> messageListener());
            applyKafkaListenerSecondary.start();
        }
    }

    public void deRegisterAndShutdown() {

        Set<String> registeredTopics = eventCallbackRegistry.getRegisteredTopics();
        log.info("Topics de-registered for consumption: {}", registeredTopics);

        applyKafkaListenerPrimary.deRegister(() -> registeredTopics);
        applyKafkaListenerPrimary.stop();

        if (isMultiDCEnabled()) {
            applyKafkaListenerSecondary.deRegister(() -> registeredTopics);
            applyKafkaListenerSecondary.stop();
        }
    }

    private AcknowledgingConsumerAwareMessageListener<Object, Object> messageListener() {

        return (record, acknowledgment, consumer) -> {

            log.info("ApplyEventConsumer listened to offset: {}, for record: {}, acknowledgement: {}, consumer: {}",
                    record.offset(), record, acknowledgment, consumer);

            Optional<CloudEventImpl> payload = Optional.empty();

            try {
                payload = Optional.ofNullable((CloudEventImpl) record.value());
            } catch (ClassCastException cce) {

                log.info("This payload is not a Cloud Event type: {}", cce.fillInStackTrace());
                ApplyConsumerException consumerException =
                        new ApplyConsumerException("This payload is not a Cloud Event type!", cce.fillInStackTrace());

                /** publish to the DLT topic, acknowledge once the processing of the event is complete **/
                deadLetterPublisher().publish(record, acknowledgment, consumerException,
                        DeadLetterFailureReasons.INCORRECT_EVENT_TYPE_FAILURE);
            }

            CloudEventImpl actualPayload = payload.get();

            if (actualPayload.getAttributes().getType() != null &&
                    eventCallbackRegistry.isEventRegistered(actualPayload.getAttributes().getType())) {

                log.info("The event is registered for consumption, and the Cloud Event type in the payload is: {}",
                        actualPayload.getAttributes().getType());

                Class<ApplyConsumerCallback> eventCallbackClass = null;

                try {
                    eventCallbackClass = getCallbackClass(actualPayload.getAttributes().getType());
                } catch (Throwable throwable) {

                    log.info("Failed to lookup callback class, for event type: {}", actualPayload.getAttributes().getType());
                    ApplyConsumerException consumerException =
                            new ApplyConsumerException("Failed to lookup callback class, for event type!", throwable.fillInStackTrace());

                    /** publish to the DLT topic, acknowledge once the processing of the event is complete **/
                    deadLetterPublisher().publish(record, acknowledgment, consumerException,
                            DeadLetterFailureReasons.CALLBACK_CLASS_LOOKUP_FAILURE);
                }

                invokeCallbackClass(eventCallbackClass, record, actualPayload, acknowledgment);
            } else {

                /** there is a possible message on this topic, which is not registered.
                 * Acknowledging, since we are not giving control back to the consumer, and we need to move the offset.
                 */

                acknowledgment.acknowledge();
                log.info("Acknowledged receipt at timestamp: {} with offset: {}", record.timestamp(), record.offset());
            }
        };
    }

    private final void invokeCallbackClass(Class<ApplyConsumerCallback> eventCallbackClass, ConsumerRecord record,
                                           CloudEventImpl actualPayload, Acknowledgment acknowledgment) {

        try {
            Optional.ofNullable(eventCallbackClass).orElseThrow(new Supplier<Throwable>() {
                @Override
                public Throwable get() {
                    return new IllegalArgumentException("Callback class is not registered!");
                }
            })
                    .getDeclaredConstructor().newInstance()
                    .processEvent(record, actualPayload, acknowledgment, deadLetterPublisher());

        } catch (Throwable throwable) {

            log.error("Failed to invoke callback class, for event type: {}", actualPayload.getAttributes().getType(), throwable);

            ApplyConsumerException consumerException =
                    new ApplyConsumerException(String.format("Failed to invoke callback class, for event type!: %s",
                            actualPayload.getAttributes().getType()), throwable.fillInStackTrace());

            /** publish to the DLT topic, acknowledge once the processing of the event is complete **/
            deadLetterPublisher().publish(record, acknowledgment, consumerException,
                    DeadLetterFailureReasons.INVOKE_CALLBACK_CLASS_FAILURE);
        }
    }

    /**
     * Get a handle to the DeadLetter Publisher inside the consumer
     *
     * @return DeadLetterPubisher {@link DeadLetterPublisher}
     */
    public DeadLetterPublisher deadLetterPublisher() {
        return new DeadLetterPublisher(kafkaProducer);
    }
}
