package com.bns.fsl.rtpeft.config;

import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Correlation-id-aware tracing. Each EFT transaction's correlation_id is
 * attached as a trace tag at ingestion time (see EftInboundKafkaConsumer)
 * so a single transaction's path - Kafka in, ACI request, sweeper pickup,
 * Kafka/NRTMQ out - can be followed end to end in the tracing backend.
 */
@Configuration
public class TracingConfig {

    @Value("${spring.application.name}")
    private String serviceName;

    @Bean
    public String tracingServiceName(Tracer tracer) {
        return serviceName;
    }
}
