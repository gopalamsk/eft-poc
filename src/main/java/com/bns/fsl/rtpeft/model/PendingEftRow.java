package com.bns.fsl.rtpeft.model;

import java.time.Instant;

/** Projection interface backing EftStatusRepository#findPendingBatch. */
public interface PendingEftRow {
    String getCorrelationId();
    String getOriginalRequest();
    Instant getCreatedAt();
    String getAciDecision();
}
