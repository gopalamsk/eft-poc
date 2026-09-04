-- eft_status: single append-only table backing the whole EFT flow.
-- One row per (correlation_id, status). The PENDING row carries the full
-- payload; the terminal row (RESPONDED / AUTO_APPROVED) carries only the
-- decision. The unique constraint is the idempotency gate that makes the
-- terminal insert atomic and safe across two GCP regions sharing this DB.

CREATE TABLE eft_status (
    id                BIGINT IDENTITY PRIMARY KEY,
    correlation_id    VARCHAR(64) NOT NULL,
    status            VARCHAR(20) NOT NULL,        -- PENDING / RESPONDED / AUTO_APPROVED
    original_request  NVARCHAR(MAX) NULL,
    decision          VARCHAR(20) NULL,
    created_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_eft_status_corr_status UNIQUE (correlation_id, status)
);
GO

CREATE INDEX ix_eft_status_pending ON eft_status(status, created_at)
    WHERE status = 'PENDING';
GO
