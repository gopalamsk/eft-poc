package com.bns.fsl.rtpeft.controller;

import com.bns.fsl.rtpeft.context.EftTransactionContext;
import com.bns.fsl.rtpeft.model.EftTransactionPayload;
import com.bns.fsl.rtpeft.processor.EftTransactionProcessor;
import com.bns.fsl.rtpeft.service.EftDecisionSweepService;
import com.bns.fsl.rtpeft.util.CommonUtil;
import com.bns.fsl.rtpeft.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

/**
 * Manual test endpoints only - not part of the production flow. Lets a
 * developer push a single transaction through ingestion and manually
 * trigger a sweep cycle without waiting for Kafka or the 2s schedule, for
 * local/IST verification. Disable or secure behind a profile check before
 * this is exposed anywhere beyond local/ist.
 */
@RestController
@RequestMapping("/internal/eft/test")
@RequiredArgsConstructor
public class EftEndToEndTestController {

    private final EftTransactionProcessor eftTransactionProcessor;
    private final EftDecisionSweepService eftDecisionSweepService;
    private final JsonUtil jsonUtil;

    @PostMapping("/ingest")
    public String ingest(@RequestBody String rawJson) {
        EftTransactionPayload payload = jsonUtil.fromJson(rawJson, EftTransactionPayload.class);
        String correlationId = CommonUtil.isBlank(payload.getCorrelationId())
                ? CommonUtil.generateCorrelationId()
                : payload.getCorrelationId();
        eftTransactionProcessor.process(EftTransactionContext.of(correlationId, payload.getFileId(), rawJson), payload);
        return correlationId;
    }

    @PostMapping("/sweep-now")
    public String sweepNow() {
        int resolved = eftDecisionSweepService.sweepOnce();
        return "resolved=" + resolved;
    }
}
