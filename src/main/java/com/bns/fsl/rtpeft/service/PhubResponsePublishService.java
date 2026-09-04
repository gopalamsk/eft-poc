package com.bns.fsl.rtpeft.service;

import com.bns.fsl.rtpeft.aspect.LoggableMethodExecution;
import com.bns.fsl.rtpeft.constants.AppConstants;
import com.bns.fsl.rtpeft.model.EftDecisionOutcome;
import com.bns.fsl.rtpeft.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes the final decision back to Optimus payment hub via the Kafka
 * outbound topic. Called BEFORE the terminal row is written to eft_status
 * (publish-first design, see README "Publish-first, record-after") so a
 * database outage never blocks delivery to the payment hub.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PhubResponsePublishService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final JsonUtil jsonUtil;

    @LoggableMethodExecution("PhubResponsePublishService.publish")
    public void publish(EftDecisionOutcome outcome) {
        String body = jsonUtil.toJson(outcome);
        kafkaTemplate.send(AppConstants.EFT_OUTBOUND_TOPIC, outcome.getCorrelationId(), body)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish decision to payment hub for correlationId={}", outcome.getCorrelationId(), ex);
                    }
                });
    }
}
