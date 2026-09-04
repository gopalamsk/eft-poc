package com.bns.fsl.rtpeft.service;

import com.bns.fsl.rtpeft.aspect.LoggableMethodExecution;
import com.bns.fsl.rtpeft.constants.AppConstants;
import com.bns.fsl.rtpeft.exception.MqPublishException;
import com.bns.fsl.rtpeft.mapper.EFTNRTMapper;
import com.bns.fsl.rtpeft.model.EftDecisionOutcome;
import lombok.RequiredArgsConstructor;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes the same decision to the NRT audit queue (NRTMQ), for downstream
 * fraud-ops/audit consumers - mirrors the existing EMT RT flow's NRT
 * publish. Also called publish-first, same as PhubResponsePublishService.
 */
@Service
@RequiredArgsConstructor
public class FodAuditPublishService {

    private final JmsTemplate jmsTemplate;
    private final EFTNRTMapper eftnrtMapper;

    @LoggableMethodExecution("FodAuditPublishService.publish")
    public void publish(EftDecisionOutcome outcome) {
        try {
            String body = eftnrtMapper.map(outcome);
            jmsTemplate.convertAndSend(AppConstants.NRT_AUDIT_QUEUE, body);
        } catch (Exception e) {
            throw new MqPublishException("Failed to publish to NRT audit queue for correlationId=" + outcome.getCorrelationId(), e);
        }
    }
}
