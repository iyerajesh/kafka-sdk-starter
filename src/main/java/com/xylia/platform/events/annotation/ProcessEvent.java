package com.xylia.platform.events.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ProcessEvent annotation, it is used to decorate the Kafka listener, to allow it to callback the registered call back class,
 * for processing a specific event type.
 *
 * @author Rajesh Iyer
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ProcessEvent {
}
