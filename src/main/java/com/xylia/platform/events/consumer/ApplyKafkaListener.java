package com.xylia.platform.events.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.Message;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import static org.springframework.util.Assert.notNull;

/**
 * Kafka Listener, that listens to the registered topics.
 * It provides new topic registration, de-registration, start and stop capabilities at runtime.
 *
 * @author Rajesh Iyer
 */

@Slf4j
public class ApplyKafkaListener {

    private final KafkaListenerContainerFactory kafkaListenerContainerFactory;
    private final Map<String, MessageListenerContainer> registeredTopicMap;
    ReentrantLock LISTENER_LOCK = new ReentrantLock();

    public ApplyKafkaListener(ConcurrentKafkaListenerContainerFactory<String, Message> concurrentKafkaListenerContainerFactory) {
        notNull(concurrentKafkaListenerContainerFactory, "KafkaListenerContainer Factory must not be null!");

        this.kafkaListenerContainerFactory = concurrentKafkaListenerContainerFactory;
        this.registeredTopicMap = new ConcurrentHashMap<>();
    }

    public void register(final Supplier<Set<String>> topicSupplier, final Supplier<MessageListener> messageListenerSupplier) {

        notNull(topicSupplier, "topicSupplier must not be null!");
        notNull(messageListenerSupplier, "messageListenerSupplier must not be null!");

        LISTENER_LOCK.lock();

        try {
            final Set<String> registeredTopics = getRegisteredTopics();
            final Set<String> topics = topicSupplier.get();

            if (topics.isEmpty())
                return;

            topics.stream()
                    .filter(topic -> !registeredTopics.contains(topic))
                    .forEach(topic -> doRegister(topic, messageListenerSupplier.get()));

        } catch (Exception e) {
            log.error("ApplyKafkaListener could not register the topics to be listened for!");
        } finally {
            LISTENER_LOCK.unlock();
        }
    }

    /**
     * Kafka listener re-registration at runtime
     *
     * @return
     */
    public void deRegister(final Supplier<Set<String>> topicSupplier) {

        notNull(topicSupplier, "topicSupplier must not be null!");
        LISTENER_LOCK.lock();

        try {
            final Set<String> registeredTopics = getRegisteredTopics();
            final Set<String> topics = topicSupplier.get();

            if (topics.isEmpty())
                return;

            topics.stream()
                    .filter(registeredTopics::contains)
                    .forEach(this::doDeregister);
        } catch (Exception e) {
            log.error("ApplyKafkaListener could not deregister the topics to be listened for!");
        } finally {
            LISTENER_LOCK.unlock();
        }
    }


    /**
     * Kafka listener start at runtime
     *
     * @return
     */
    public void start() {

        LISTENER_LOCK.lock();
        try {
            final Collection<MessageListenerContainer> registeredMessageListenerContainers = getRegisteredMessageListenerContainers();
            registeredMessageListenerContainers.forEach(container -> {
                if (container.isRunning())
                    return;

                container.start();
            });
        } catch (Exception e) {
            log.error("ApplyKafkaListener could not start the registered listeners!");
        } finally {
            LISTENER_LOCK.unlock();
        }
    }

    /**
     * Kafka listener stop at runtime
     *
     * @return
     */
    public void stop() {

        LISTENER_LOCK.lock();
        try {
            final Collection<MessageListenerContainer> registeredMessageListenerContainers = getRegisteredMessageListenerContainers();
            registeredMessageListenerContainers.forEach(container -> {
                if (!container.isRunning())
                    return;

                container.stop();
            });
        } catch (Exception e) {
            log.error("ApplyKafkaListener could not stop the registered listeners!");
        } finally {
            LISTENER_LOCK.unlock();
        }
    }

    public Map<String, MessageListenerContainer> getRegisteredTopicMap() {
        return Collections.unmodifiableMap(registeredTopicMap);
    }

    private void doRegister(final String topic, final MessageListener messageListener) {

        Assert.hasLength(topic, "topic must not be empty!");
        notNull(messageListener, "messageListener must not be null!");

        final MessageListenerContainer messageListenerContainer = kafkaListenerContainerFactory.createContainer(topic);
        messageListenerContainer.setupMessageListener(messageListener);
        messageListenerContainer.start();

        registeredTopicMap.put(topic, messageListenerContainer);
    }

    private void doDeregister(final String topic) {

        Assert.hasLength(topic, "topic must not be empty!");

        final MessageListenerContainer messageListenerContainer = registeredTopicMap.get(topic);
        messageListenerContainer.stop();

        registeredTopicMap.remove(topic);
    }

    private Set<String> getRegisteredTopics() {
        return registeredTopicMap.keySet();
    }

    private Collection<MessageListenerContainer> getRegisteredMessageListenerContainers() {
        return registeredTopicMap.values();
    }
}
