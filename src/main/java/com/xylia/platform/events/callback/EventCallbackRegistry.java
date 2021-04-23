package com.xylia.platform.events.callback;

import com.google.common.collect.Maps;
import com.xylia.platform.events.exception.ApplyConsumerException;
import com.xylia.platform.events.model.ApplyEventTypes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Event callback registry - which stores the mapping between, the Apply event type, and the registered callback class.
 *
 * @author Rajesh Iyer
 */

@Component
@Slf4j
public class EventCallbackRegistry {

    private static final Map<String, TopicCallbackClassMapping> eventCallbackRegistry = Maps.newLinkedHashMap();

    protected static final void addEvent(ApplyEventTypes applyEventTypes, Class<ApplyConsumerCallback> eventCallbackClass) {

        eventCallbackRegistry.put(applyEventTypes.getType(), TopicCallbackClassMapping.builder()
                .topic(applyEventTypes.getTopic())
                .consumerCallbackClass(eventCallbackClass)
                .build());

        log.info("Successfully added event type: {}, with callback class: {}", applyEventTypes.getType(), eventCallbackClass);
    }

    public static final Class<ApplyConsumerCallback> getCallbackClass(String eventType) throws Throwable {
        if (eventCallbackRegistry.containsKey(eventType))
            return eventCallbackRegistry.get(eventType)
                    .getConsumerCallbackClass();
        else
            throw new ApplyConsumerException("Event callback registry does not have event type: " + eventType + "registered!");
    }

    public boolean isEventRegistered(String eventType) {
        return eventCallbackRegistry.keySet().contains(eventType);
    }

    public Set<String> getRegisteredEventTypes() {
        Assert.notNull(eventCallbackRegistry, "eventCallbackRegisry must not be null!");

        return eventCallbackRegistry.keySet().stream().collect(Collectors.toSet());
    }

    public Set<String> getRegisteredTopics() {
        Assert.notNull(eventCallbackRegistry, "eventCallbackRegistry must not br null!");

        return eventCallbackRegistry.values().stream()
                .map(eventType -> eventType.getTopic())
                .collect(Collectors.toSet());
    }
}
