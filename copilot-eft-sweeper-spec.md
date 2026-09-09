# Copilot Implementation Spec — EFT Decision Sweeper (Batch Component)

**How to use this file:** Paste this entire document into Copilot Chat in IntelliJ, in the `fsl-rtp-eft-service` project, on the `feature/initial-eft-config` branch. Ask Copilot to read the existing files referenced below *before* generating anything, so new code matches existing conventions exactly rather than introducing a second style.

---

## 0. Read these existing files first, before writing anything

- `com.bns.fsl.rtpeft.consumer.EftInboundKafkaConsumer` — see the try/catch pattern: `RtpEftException` is rethrown as non-retryable (container skips + commits offset), plain `Exception` is rethrown so `DefaultErrorHandler` applies configured retry/backoff. **The sweeper must follow the same non-swallowing philosophy** — never catch-and-ignore an exception that should surface.
- `com.bns.fsl.rtpeft.context.EftTransactionContext` — Lombok `@Builder` pattern, fields `transactionId`, `correlationId`, `phubPaymentRequest`. Reuse this same builder style for any new context/DTO objects.
- `com.bns.fsl.rtpeft.aspect.LoggableMethodExecution` / `LoggingAspect` — every new service/processor method that does meaningful work should carry this annotation, matching how it's already used elsewhere.
- `com.bns.fsl.rtpeft.exception.RtpEftException`, `InternalSystemException`, `MqPublishException` — new failure modes should extend `RtpEftException`, not throw raw `RuntimeException`.
- `com.bns.fsl.rtpeft.entity.FraudDecisionCache` and `com.bns.fsl.rtpeft.repository.FraudDecisionRepository` — confirm whether this is a read-only JPA mapping onto the Fraud Decision Service's own table, or a locally-synced cache table. **This spec assumes it's a read-only mapping (no writes, ever) — if that's wrong, stop and flag it before implementing the sweeper's read query below.**
- `com.bns.fsl.rtpeft.config.db.AzureSqlCloudPfmDataSourceConfig`, `FraudConfigRead`, `PfmDataSourceConfigProperties` — reuse this exact datasource/config pattern for anything new; do not introduce a second `DataSource` bean.
- `com.bns.fsl.rtpeft.mapper.EFTRTMapper` / `BaseMapper` — reuse this MapStruct convention for any new mapping needs (don't hand-write mapping code where a mapper interface would match the existing pattern).
- `AppConstants` — add new topic/queue/config-key names here, don't hardcode strings in new classes.
- `application.yml` + the four profile overlays (`-local`, `-ist`, `-uat`, `-prd`) — new config properties go in all five files with sensible per-environment defaults, following whatever pattern is already there for existing properties.

---

## 1. What this component does

A scheduled sweeper that resolves EFT transactions once they either (a) receive a real fraud decision from ACI, or (b) exceed a configurable auto-approve timeout with no decision. It is the second half of the flow — `EftInboundKafkaConsumer` → `EftTransactionProcessor` already handle ingestion (writing the transaction and firing the ACI scoring request); this component is what closes the loop back to phub and to ACI's NRT audit queue.

---

## 2. Non-negotiable production requirements

These are not optional/nice-to-have — treat each as a hard constraint on the implementation, not a suggestion:

1. **Idempotent by database constraint, not by application logic.** A unique constraint must make a duplicate terminal write structurally impossible, not just unlikely. Do not rely on "check rows affected" or in-memory locks — this must survive two pods/regions racing.
2. **Publish before recording.** The outbound message (to phub and to ACI NRT) must go out even if the database is briefly unreachable when the sweeper tries to record the terminal outcome. Recording the terminal row happens *after* the publish succeeds, and its failure must not roll back or suppress the publish. This is a deliberate at-least-once tradeoff — document it in the class Javadoc, and note that downstream consumers must dedupe on the transaction/correlation id.
3. **Row claiming must happen inside the same transaction as the select**, not after it commits. If you select-then-commit-then-publish-then-write, a second pod/region can select the same rows in the gap between your commit and your write. The claim (whatever marks a row as "being handled") must be written in the same transaction as the read that found it.
4. **No holding a DB lock across an external call.** Do not keep a transaction open while publishing to Kafka/MQ — that risks connection pool exhaustion and long lock waits under load. Claim rows and close the transaction, publish outside it, then open a new transaction to record the terminal state.
5. **Every exception path is explicit.** No empty catch blocks, no swallowed exceptions "just to keep the loop going." If a single row fails to resolve, log with enough context to find it (transaction id, correlation id, what stage failed) and move on to the next row — don't let one bad row silently vanish or silently kill the whole sweep cycle.
6. **Batch size and cadence must be config-driven**, not hardcoded — same pattern as `FraudConfigRead`'s cached-with-refresh approach, so a production tuning change doesn't require a redeploy.
7. **Logging must be structured and consistent with the rest of the codebase** — use the existing `LoggableMethodExecution` aspect, log the transaction/correlation id on every line (matching the `key`, `transactionId` pattern already used in `EftInboundKafkaConsumer`'s log statements), and never log the full payload at INFO level (payloads may contain PII/payment data — DEBUG only, and confirm with the team whether even DEBUG is acceptable before logging raw payloads).
8. **No changes, ever, to tables owned by other services.** The Fraud Decision Service's table stays strictly read-only from this codebase — confirmed by the `FraudDecisionRepository` convention already in place.

---

## 3. New package: `com.bns.fsl.rtpeft.batch`

Mirrors the existing `processor`/`consumer` split: a thin scheduling class here, actual logic lives in `service`.

### `EftDecisionSweepJob`
- `@Component` (or `@Configuration` if using Spring Batch `Job`/`Step`/`Tasklet` — match whatever pattern the team has agreed on; if undecided, prefer a plain `@Scheduled` method calling into the service, since a polling sweeper doesn't benefit from Spring Batch's chunk/restart machinery and that machinery's automatic retry-on-failure is actively dangerous here — see requirement #2 above).
- `@Scheduled(fixedDelay = ${eft.sweep.interval-ms})` — **use `fixedDelay`, not `fixedRate`.** `fixedRate` can queue up overlapping executions if a cycle runs long; `fixedDelay` waits for one cycle to finish before scheduling the next, which is what you want for a payment-critical sweeper. Document this choice in the class Javadoc so nobody "fixes" it to `fixedRate` later without understanding why.
- Catches and logs any exception from the service call so one failed cycle doesn't kill the `@Scheduled` executor for all future cycles (a scheduled method that throws stops future invocations in some configurations — this must not happen).
- Logs cycle summary at INFO: rows resolved, cycle duration. Logs nothing at INFO if zero rows were resolved (avoid log spam every 2 seconds in a quiet period) — but do log a WARN if the cycle itself failed.

### `EftDecisionSweepService` (goes in `com.bns.fsl.rtpeft.service`, alongside `AciMqService`/`FodAuditPublishService`/`PhubResponsePublishService`)
- `sweepOnce()` — reads a batch of claimable rows, resolves each one, returns count resolved.
- Per-row resolution:
  - If ACI has a decision (via read-only join/lookup against `FraudDecisionRepository`) → resolve as a real decision.
  - Else if the row's age exceeds the configured auto-approve timeout → resolve as auto-approved.
  - Else → leave as-is, will be picked up again next cycle.
- Publish via the **existing** `PhubResponsePublishService` and `FodAuditPublishService`/`AciMqService` as appropriate — do not create parallel publish logic; reuse what `EftTransactionProcessor` already established for ACI, and whatever pattern the ingestion side uses for phub/audit publishing.
- Record terminal state via a new repository call, wrapped so a `DataIntegrityViolationException` (duplicate/already-claimed) is treated as an expected no-op and logged at INFO, not ERROR — everything else is a real failure logged at ERROR with full context, row left unresolved for the next cycle to retry.

---

## 4. New entity + repository (propose name — confirm/rename before merging)

### `EftPaymentStatus` entity (`com.bns.fsl.rtpeft.entity`)
Tracks lifecycle per transaction. Suggested shape — adjust field names to match whatever convention `EftTransactionContext` already established (`transactionId` vs `correlationId` — confirm which one is the natural join key against `FraudDecisionCache`/`FraudDecisionRepository` before finalizing):

- `id` (surrogate PK)
- `transactionId` / `correlationId` (whichever is the actual join key — **confirm this**, since `EftInboundKafkaConsumer` extracts both a `transactionId` from the payload and a `correlationId` from the Kafka key, and it's not yet clear which one ACI/Fraud Decision Service key their table on)
- `status` (enum: `PENDING`, `RESPONDED`, `AUTO_APPROVED`)
- `originalRequest` (payload, set only on the PENDING row — needed so the sweeper can republish without a second lookup)
- `decision` (set only on the terminal row)
- `createdAt`
- Unique constraint on `(transactionId or correlationId, status)` — this is what makes the terminal insert idempotent.

### `EftPaymentStatusRepository`
- `findClaimableBatch(int batchSize)` — native query, `WITH (UPDLOCK, READPAST)`, filtered to `PENDING` rows with no existing terminal row for the same key, ordered oldest-first, capped at the configured batch size. Left-join against the Fraud Decision read entity to pull the decision if one exists.
- Terminal-row `save()` — relies on the unique constraint for idempotency; caller (the service) handles the constraint-violation case as described above.

### Indexing (raise with the DBA once the entity is finalized)
- Filtered index on `(status, created_at) WHERE status = 'PENDING'` — serves the sweeper's hot read.
- Composite index on `(transactionId/correlationId, status)` — serves the "does a terminal row already exist" lookup inside the same query.

---

## 5. Config additions (add to `AppConstants` + all five `application*.yml` files)

- `eft.sweep.interval-ms` (default 2000)
- `eft.sweep.batch-size` (default 500)
- `eft.sweep.auto-approve-timeout-minutes` — if a DB-backed, cache-refreshed value is wanted (matching `FraudConfigRead`'s pattern against the existing `eft_config`/`FraudConfig` table), reuse that table/entity rather than adding a new one. Confirm with whoever owns `FraudConfig` whether this key should live there.

---

## 6. Tests to generate alongside the implementation

- Unit tests for `EftDecisionSweepService`: real decision found → publishes + records `RESPONDED`; no decision, past cutoff → publishes + records `AUTO_APPROVED`; no decision, within cutoff → no publish, row untouched; terminal insert throws `DataIntegrityViolationException` → logged at INFO, no exception propagates, row still counted correctly.
- Repository test (against a test DB or H2/Testcontainers matching however the rest of the project tests JPA queries) verifying the `UPDLOCK`/`READPAST` query actually excludes rows that already have a terminal row.
- A test for the `EftDecisionSweepJob` scheduling wrapper confirming that a thrown exception from one cycle does not prevent the next scheduled invocation.

---

## 7. Explicit instruction to Copilot

Implement this in the order above (entity → repository → config → service → batch job → tests), and after each file, briefly explain which existing convention it matched and which assumption (if any) it made — so Naari can correct anything that guessed wrong about the real join key, the `FraudDecisionCache` semantics, or naming before it goes further.
