package com.bns.fsl.eft.context;

import com.bns.fsl.eft.EftFraudPaymentRequest;
import com.bns.fsl.eft.EftFraudPaymentResponse;
import com.bns.fsl.schema.rtp.aci.model.XFRqst;
import lombok.Builder;
import lombok.Data;

/**
 * Pipeline context object that carries all data for a single EFT transaction
 * as it flows through consumer → processor → services.
 *
 * Avoids passing multiple parameters between layers and provides
 * a single source of truth for the current transaction state.
 */
@Data
@Builder
public class EftTransactionContext {

    // ── Identifiers ───────────────────────────────────────────────────────────
    private String transactionId;
    private String correlationId;

    // ── Inbound ───────────────────────────────────────────────────────────────
    /** Original request received from PHUB */
    private EftFraudPaymentRequest phubPaymentRequest;

    // ── ACI ───────────────────────────────────────────────────────────────────
    /** Mapped ACI request payload sent to MQ */
    private XFRqst aciRTPaymentRequest;
    private XFRqst aciNRTPaymentRequest;

    // ── Outbound ──────────────────────────────────────────────────────────────
    /** Response built from ACI RT reply, published back to PHUB */
    private EftFraudPaymentResponse phubPaymentResponse;


    // ── State ─────────────────────────────────────────────────────────────────
    private String processingStatus;
    private String errorMessage;

}
