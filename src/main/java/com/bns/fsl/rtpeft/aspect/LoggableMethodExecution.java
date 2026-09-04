package com.bns.fsl.rtpeft.aspect;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method for entry/exit/timing logging by {@link LoggingAspect}.
 * Apply to service and processor methods on the hot path (ingestion, sweeper,
 * publish) so operational timing shows up in logs without cluttering method bodies.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LoggableMethodExecution {
    String value() default "";
}
