package com.xylia.platform.events.callback;

import com.google.common.collect.Maps;
import com.xylia.platform.events.model.ApplyEventTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.xylia.platform.events.callback.EventCallbackRegistry.addEvent;

/**
 * Class that registers the callback contract, for a particular event name, and adds that to the registry.
 *
 * @author Rajesh Iyer
 */
public class RegisterCallbackContract {

    private ApplyEventTypes eventName;
    private Class<ApplyConsumerCallback> eventCallbackClass;

//    private Map<ApplyEventTypes, Class<ApplyConsumerCallback>> registrationList = Maps.newHashMap();

    private Map<ApplyEventTypes, Class<ApplyConsumerCallback>> registrationList = Maps.newHashMap();

    public static RegisterCallbackContract builder() {
        return new RegisterCallbackContract();
    }

    public RegisterCallbackContract forEventName(ApplyEventTypes eventType) {
        this.eventName = eventType;
        return this;
    }

    public RegisterCallbackContract withCallback(Class eventCallbackClass) {
        this.eventCallbackClass = eventCallbackClass;
        registrationList.put(eventName, eventCallbackClass);

        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisterCallbackContract that = (RegisterCallbackContract) o;
        return eventName == that.eventName && Objects.equals(eventCallbackClass, that.eventCallbackClass) && Objects.equals(registrationList, that.registrationList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventName, eventCallbackClass, registrationList);
    }

    public void build() {
        registrationList.entrySet()
                .stream()
                .forEach(registrationEntry -> addEvent(registrationEntry.getKey(), registrationEntry.getValue()));
    }
}
