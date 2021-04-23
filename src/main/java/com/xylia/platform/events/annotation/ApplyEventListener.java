package com.xylia.platform.events.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** ApplyEventHeader annotation - returns a specific value o the header in the CloudEvent specification.
 *
 * @author Rajesh Iyer
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ApplyEventListener {
    String header() default "";
}
