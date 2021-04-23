package com.xylia.platform.events.callback;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Stores the mapping between the topic, and the registered call back class
 *
 * @author Rajesh Iyer
 */

@Builder
@Getter
@Setter
public class TopicCallbackClassMapping {

    private String topic;
    private Class<ApplyConsumerCallback> consumerCallbackClass;

    @Override
    public String toString() {
        return "TopicCallbackClassMapping{" +
                "topic='" + topic + '\'' +
                ", consumerCallbackClass=" + consumerCallbackClass +
                '}';
    }
}
