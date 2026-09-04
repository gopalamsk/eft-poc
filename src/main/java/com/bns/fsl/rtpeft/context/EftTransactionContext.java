package com.bns.fsl.rtpeft.context;

import lombok.Data;

/**
 * Thread-local-scoped request context for a single EFT transaction while it
 * is being ingested. This deliberately does NOT survive past the ingestion
 * call - unlike the old EMT RT pattern, nothing here is held in memory
 * waiting for a score. Once EftTransactionProcessor finishes writing the
 * PENDING row and publishing to ACI, this context is discarded; the row in
 * eft_status is the only thing that "remembers" the transaction afterwards.
 */
@Data
public class EftTransactionContext {
    private String correlationId;
    private String fileId;
    private String rawJson;

    public static EftTransactionContext of(String correlationId, String fileId, String rawJson) {
        EftTransactionContext ctx = new EftTransactionContext();
        ctx.setCorrelationId(correlationId);
        ctx.setFileId(fileId);
        ctx.setRawJson(rawJson);
        return ctx;
    }
}
