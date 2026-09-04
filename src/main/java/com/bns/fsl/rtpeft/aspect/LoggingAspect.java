package com.bns.fsl.rtpeft.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

/**
 * Wraps every {@link LoggableMethodExecution} annotated method with
 * entry/exit/duration/exception logging. This is for operational visibility,
 * not distributed tracing (see TracingConfig for that).
 */
@Aspect
@Component
@Slf4j
public class LoggingAspect {

    @Around("@annotation(loggable)")
    public Object logExecution(ProceedingJoinPoint joinPoint, LoggableMethodExecution loggable) throws Throwable {
        String label = loggable.value().isBlank() ? joinPoint.getSignature().toShortString() : loggable.value();
        long start = System.currentTimeMillis();
        log.info("START {}", label);
        try {
            Object result = joinPoint.proceed();
            log.info("END {} durationMs={}", label, System.currentTimeMillis() - start);
            return result;
        } catch (Throwable t) {
            log.error("FAILED {} durationMs={} error={}", label, System.currentTimeMillis() - start, t.getMessage());
            throw t;
        }
    }
}
