-- Reference only - the live version of this query is EftStatusRepository#findPendingBatch.
-- Kept here for the architect review / DBA sign-off, matched exactly to the
-- native query in code so the two never drift.

SELECT p.correlation_id, p.original_request, p.created_at, f.decision AS aci_decision
FROM eft_status p WITH (UPDLOCK, READPAST)
LEFT JOIN fraud_decision_table f ON f.correlation_id = p.correlation_id
WHERE p.status = 'PENDING'
AND NOT EXISTS (
    SELECT 1 FROM eft_status t
    WHERE t.correlation_id = p.correlation_id
    AND t.status IN ('RESPONDED','AUTO_APPROVED')
)
ORDER BY p.created_at ASC
OFFSET 0 ROWS FETCH NEXT 500 ROWS ONLY;

-- Terminal insert (best-effort, executed AFTER publish - see EftDecisionSweepService)
INSERT INTO eft_status (correlation_id, status, decision, created_at)
VALUES (@correlationId, @status, @decision, SYSUTCDATETIME());
