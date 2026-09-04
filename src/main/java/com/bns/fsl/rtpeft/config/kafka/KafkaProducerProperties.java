package com.bns.fsl.rtpeft.config.kafka;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "fsl.kafka.producer")
public class KafkaProducerProperties {
    private String bootstrapServers;
    private int retries = 3;
    private String acks = "all";
}
