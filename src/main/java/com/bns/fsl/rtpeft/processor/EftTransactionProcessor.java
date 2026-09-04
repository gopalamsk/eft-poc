package com.bns.fsl.rtpeft.processor;

import com.bns.fsl.rtpeft.aspect.LoggableMethodExecution;
import com.bns.fsl.rtpeft.context.EftTransactionContext;
import com.bns.fsl.rtpeft.entity.EftStatus;
import com.bns.fsl.rtpeft.entity.EftStatusValue;
import com.bns.fsl.rtpeft.mapper.EFTRTMapper;
import com.bns.fsl.rtpeft.model.AciRequestMessage;
import com.bns.fsl.rtpeft.model.EftTransactionPayload;
import com.bns.fsl.rtpeft.repository.EftStatusRepository;
import com.bns.fsl.rtpeft.service.AciMqService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Ingestion-time processing for a single EFT transaction:
 *   1. write the PENDING row to eft_status (full payload included)
 *   2. publish the scoring request to ACI
 *
 * Both happen inside one transaction boundary for step 1, then step 2 is
 * fired only after that commit succeeds - if ACI publish fails, the row is
 * still PENDING and will simply have no ACI answer, which the sweeper's
 * auto-approve path already handles safely. No thread here waits for a
 * score; nothing beyond this method holds the transaction context in memory.
 */
@Component
@RequiredArgsConstructor
public class EftTransactionProcessor {

    private final EftStatusRepository eftStatusRepository;
    private final EFTRTMapper eftrtMapper;
    private final AciMqService aciMqService;

    @LoggableMethodExecution("EftTransactionProcessor.process")
    public void process(EftTransactionContext context, EftTransactionPayload payload) {
        writePendingRow(context, payload);

        AciRequestMessage request = eftrtMapper.map(payload);
        aciMqService.submitForScoring(request);
    }

    @Transactional("pfmTransactionManager")
    void writePendingRow(EftTransactionContext context, EftTransactionPayload payload) {
        EftStatus pending = EftStatus.builder()
                .correlationId(context.getCorrelationId())
                .status(EftStatusValue.PENDING)
                .originalRequest(payload.getRawJson())
                .createdAt(Instant.now())
                .build();
        eftStatusRepository.save(pending);
    }
}
