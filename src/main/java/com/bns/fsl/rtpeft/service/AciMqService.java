package com.bns.fsl.rtpeft.service;

import com.bns.fsl.rtpeft.aspect.LoggableMethodExecution;
import com.bns.fsl.rtpeft.constants.AppConstants;
import com.bns.fsl.rtpeft.exception.MqPublishException;
import com.bns.fsl.rtpeft.model.AciRequestMessage;
import com.bns.fsl.rtpeft.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

/**
 * Publishes to the ACI request queue only. This service never reads ACI's
 * response queue - that is consumed exclusively by Fraud Decision Service.
 * The eventual decision is discovered by EftDecisionSweepJob reading Fraud
 * Decision Service's table, not by listening to any queue here.
 */
@Service
@RequiredArgsConstructor
public class AciMqService {

    private final JmsTemplate jmsTemplate;
    private final JsonUtil jsonUtil;

    @LoggableMethodExecution("AciMqService.submitForScoring")
    public void submitForScoring(AciRequestMessage message) {
        try {
            String body = jsonUtil.toJson(message);
            jmsTemplate.convertAndSend(AppConstants.ACI_REQUEST_QUEUE, body);
        } catch (Exception e) {
            throw new MqPublishException("Failed to publish to ACI request queue for correlationId=" + message.getCorrelationId(), e);
        }
    }
}
