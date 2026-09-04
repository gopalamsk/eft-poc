package com.bns.fsl.rtpeft.service;

import com.bns.fsl.rtpeft.aspect.LoggableMethodExecution;
import com.bns.fsl.rtpeft.config.db.FraudConfigRead;
import com.bns.fsl.rtpeft.constants.AppConstants;
import com.bns.fsl.rtpeft.entity.EftStatus;
import com.bns.fsl.rtpeft.entity.EftStatusValue;
import com.bns.fsl.rtpeft.model.EftDecisionOutcome;
import com.bns.fsl.rtpeft.model.PendingEftRow;
import com.bns.fsl.rtpeft.repository.EftStatusRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The core business logic behind EftDecisionSweepJob (see that class for the
 * scheduling wrapper). Per pending row:
 *
 *   - if ACI has already answered -> resolve as RESPONDED with ACI's decision
 *   - if not, and the configurable auto-approve window has elapsed -> resolve
 *     as AUTO_APPROVED (the payment has already released by this point, a
 *     late ACI answer after this is intentionally never looked at again)
 *   - otherwise -> leave PENDING, re-checked next cycle
 *
 * Publish happens BEFORE the terminal row is recorded (see README
 * "Publish-first, record-after") - this guarantees delivery even during a
 * database outage, at the cost of moving from exactly-once to at-least-once
 * delivery. Downstream consumers of the Kafka outbound topic and NRTMQ must
 * therefore be idempotent on correlationId.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class EftDecisionSweepService {

    private final EftStatusRepository eftStatusRepository;
    private final FraudConfigRead fraudConfigRead;
    private final PhubResponsePublishService phubResponsePublishService;
    private final FodAuditPublishService fodAuditPublishService;

    public int sweepOnce() {
        int autoApproveMinutes = fraudConfigRead.getAutoApproveTimeoutMinutes();
        Instant cutoff = Instant.now().minusSeconds(autoApproveMinutes * 60L);

        List<PendingEftRow> rows = eftStatusRepository.findPendingBatch(AppConstants.SWEEP_BATCH_SIZE);
        int resolved = 0;
        for (PendingEftRow row : rows) {
            if (resolveRow(row, cutoff)) {
                resolved++;
            }
        }
        return resolved;
    }

    @LoggableMethodExecution("EftDecisionSweepService.resolveRow")
    private boolean resolveRow(PendingEftRow row, Instant cutoff) {
        Optional<EftDecisionOutcome> outcome = toOutcome(row, cutoff);
        if (outcome.isEmpty()) {
            return false; // still within window, no ACI answer yet - leave PENDING
        }

        EftDecisionOutcome resolved = outcome.get();

        // Publish first - must succeed (or the payment hub / audit trail must
        // receive it) even if the database is unreachable right now.
        phubResponsePublishService.publish(resolved);
        fodAuditPublishService.publish(resolved);

        recordTerminalRowBestEffort(resolved);
        return true;
    }

    private Optional<EftDecisionOutcome> toOutcome(PendingEftRow row, Instant cutoff) {
        if (row.getAciDecision() != null) {
            return Optional.of(EftDecisionOutcome.builder()
                    .correlationId(row.getCorrelationId())
                    .originalRequest(row.getOriginalRequest())
                    .decision(row.getAciDecision())
                    .terminalStatus(EftStatusValue.RESPONDED)
                    .build());
        }
        if (row.getCreatedAt().isBefore(cutoff)) {
            return Optional.of(EftDecisionOutcome.builder()
                    .correlationId(row.getCorrelationId())
                    .originalRequest(row.getOriginalRequest())
                    .decision(AppConstants.DECISION_AUTO_APPROVE)
                    .terminalStatus(EftStatusValue.AUTO_APPROVED)
                    .build());
        }
        return Optional.empty();
    }

    /**
     * Best-effort - if this insert fails (DB down, transient error) the
     * message has already gone out, so we log and move on rather than
     * retry inline and risk a second publish. The unique constraint on
     * (correlation_id, status) means a retry-driven duplicate pickup of the
     * same row on a later cycle still can't produce a second DB row here -
     * but it CAN produce a second publish, which is why downstream
     * consumers must dedupe. See README "Failure scenarios".
     */
    @Transactional("pfmTransactionManager")
    void recordTerminalRowBestEffort(EftDecisionOutcome outcome) {
        try {
            EftStatus terminal = EftStatus.builder()
                    .correlationId(outcome.getCorrelationId())
                    .status(outcome.getTerminalStatus())
                    .decision(outcome.getDecision())
                    .createdAt(Instant.now())
                    .build();
            eftStatusRepository.save(terminal);
        } catch (DataIntegrityViolationException dup) {
            log.info("Terminal row already recorded for correlationId={}, this cycle's publish was a retry", outcome.getCorrelationId());
        } catch (Exception e) {
            log.error("Failed to record terminal row for correlationId={} after publish already succeeded - "
                    + "will be retried on next sweep cycle since the row remains PENDING", outcome.getCorrelationId(), e);
        }
    }
}
