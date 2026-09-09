# Copilot Implementation Spec — fsl-rtp-eft-service (Master)

**How to use this file:** Paste this entire document into Copilot Chat in IntelliJ, on the `feature/initial-eft-config` branch. Ask Copilot to read the "read first" files below before writing anything, and to implement in the order given in section 8 — not all at once. Section 1 lists decisions that must be confirmed by a human before implementation starts; Copilot should not resolve these on its own.

---

## 0. Read these existing files first, before writing anything

- `com.bns.fsl.rtpeft.consumer.EftInboundKafkaConsumer` — the established exception pattern: `RtpEftException` is rethrown as non-retryable (container skips + commits offset), plain `Exception` is rethrown so `DefaultErrorHandler` applies configured retry/backoff. Every new component follows this same non-swallowing philosophy.
- `com.bns.fsl.rtpeft.context.EftTransactionContext` — Lombok `@Builder`, fields `transactionId`, `correlationId`, `phubPaymentRequest`. Reuse this builder style for new context/DTO objects.
- `com.bns.fsl.rtpeft.aspect.LoggableMethodExecution` / `LoggingAspect` — apply to every new service/processor method doing meaningful work.
- `com.bns.fsl.rtpeft.exception.RtpEftException`, `InternalSystemException`, `MqPublishException` — new failure modes extend `RtpEftException`.
- `com.bns.fsl.rtpeft.entity.FraudDecisionCache` and `com.bns.fsl.rtpeft.repository.FraudDecisionRepository` — **open question, see section 1**.
- `com.bns.fsl.rtpeft.service.FodAuditPublishService` — **open question, see section 1**.
- `com.bns.fsl.rtpeft.config.db.AzureSqlCloudPfmDataSourceConfig`, `FraudConfigRead`, `PfmDataSourceConfigProperties` — reuse this exact datasource/config pattern; no second `DataSource` bean.
- `com.bns.fsl.rtpeft.mapper.EFTRTMapper` / `BaseMapper` — reuse this MapStruct convention.
- `AppConstants` — new topic/queue/config-key names go here, never hardcoded in a new class.
- `application.yml` + the four profile overlays (`-local`, `-ist`, `-uat`, `-prd`).

---

## 1. Decisions to confirm before Copilot implements anything

Do not let Copilot guess these — resolve them first, then delete this section or mark each item resolved.

1. **Join key: `transactionId` or `correlationId`?** `EftInboundKafkaConsumer` extracts both. Confirm which one `FraudDecisionCache`/ACI's response is actually keyed on — the sweeper's core query depends on this being right.
2. **What does `FraudDecisionCache` actually contain, and is it read-only?** Confirm it's a read-only mapping onto the Fraud Decision Service's own table (no writes, ever, from this codebase), not a locally-synced copy this service is expected to populate.
3. **What does `FodAuditPublishService` currently publish to, in the real code?** Prior design intent was: FOD is fed only at ingestion time (audit copy of the inbound request), never from the resolution/sweeper side — the sweeper's resolution-side audit publish goes to ACI's NRT queue instead. Confirm the existing `FodAuditPublishService` implementation actually matches this, since the class name alone is ambiguous. If it currently does something else, reconcile the naming/behavior before extending it.
4. **New tracking table/entity name.** No status-tracking entity exists yet in the real `entity` package. Prior design sessions used `EftStatus`/`correlation_id`, but that was never checked against this actual repo. Pick a final name (`EftStatus` is a reasonable default given the existing naming pattern) and confirm the join key from #1 before generating the entity.
5. **Row-claiming vs. publish-first — accepted tradeoff, not a solved problem.** See section 3, requirement 2. This is not something Copilot can resolve better than the design already has — it needs to be accepted as-is, not "fixed" by holding a transaction open across a publish call.

---

## 2. Scope

This spec covers the full service, at these levels of detail:

| Area | Status | Detail level here |
|---|---|---|
| Kafka inbound consumer | Already implemented | Conventions only (section 0) |
| Transaction processor (ingestion) | Already implemented | Conventions only (section 0) |
| ACI / Phub / FOD publish services | Already implemented, one open question (#3 above) | Reuse as-is once confirmed |
| DB entity + repository (status tracking) | **Not yet built** | Full spec, section 5 |
| Sweeper / batch component | **Not yet built** | Full spec, section 4 |
| DLQ for poison messages | **Not yet built** | Full spec, section 6 |
| Logging & observability | Partially established (aspect exists) | Full spec, section 7 |
| Metrics | Not yet built | Full spec, section 7 |
| Health/readiness probes | Not yet built | Full spec, section 7 |
| Graceful shutdown | Not yet built | Full spec, section 7 |
| Security | Not yet built | Full spec, section 7 |
| Testing | Partial | Full spec, section 9 |

---

## 3. Non-negotiable production requirements

1. **Idempotent by database constraint, not application logic.** A unique constraint makes a duplicate *terminal database row* structurally impossible. It does **not**, by itself, prevent a duplicate *publish* — see requirement 2.

2. **Publish-first, record-after — an accepted tradeoff, not a race condition to eliminate.** The correct sequence is:
   - Transaction A: select + claim a batch of rows (`UPDLOCK`, `READPAST`), commit. Lock is released here.
   - Outside any transaction: publish to phub / ACI NRT.
   - Transaction B: record the terminal row. Protected by the unique constraint.

   Between Transaction A's commit and Transaction B's commit, another pod/region *can* re-claim and re-publish the same row — this window is real and is **not eliminated** by claiming inside the select transaction, because closing that transaction (required, since holding a DB lock across a Kafka/MQ call risks connection pool exhaustion) releases the claim. The unique constraint stops a second *database row*, not a second *publish*. **The actual safety net is downstream dedupe on the transaction/correlation id** — this must be true for every consumer of the outbound Kafka topic and NRT queue. State this explicitly in the class Javadoc so nobody "fixes" it later by holding a lock across the publish call, which would trade a rare double-publish for routine connection exhaustion — a worse trade.

3. **No holding a DB lock across an external call.** Follows directly from requirement 2 — claim and close the transaction, publish outside it, record in a new transaction.

4. **Every exception path is explicit.** No empty catch blocks. One bad row logs with full context (transaction/correlation id, failing stage) and the sweep continues to the next row — one failure never silently kills the cycle or vanishes a row.

5. **Batch size and cadence are config-driven**, following `FraudConfigRead`'s cached-with-refresh pattern — no redeploy needed to tune in production.

6. **Structured, consistent logging.** Use `LoggableMethodExecution` on every meaningful method. Log the transaction/correlation id on every line. Never log full payloads at INFO (may contain PII/payment data) — DEBUG only, and confirm with the team whether even DEBUG is acceptable before logging raw payloads.

7. **No changes, ever, to tables owned by other services.** The Fraud Decision Service's table stays strictly read-only.

---

## 4. New package: `com.bns.fsl.rtpeft.batch`

### `EftDecisionSweepJob`
- `@Component`, `@Scheduled(fixedDelay = ${eft.sweep.interval-ms})`. **`fixedDelay`, not `fixedRate`** — `fixedRate` can queue overlapping executions if a cycle runs long; `fixedDelay` waits for the current cycle to finish first. This matters directly for requirement 3 above (an overlapping cycle would mean two claims racing within the same pod, not just across pods). Document the choice in the class Javadoc.
- Prefer a plain scheduled method calling into the service over Spring Batch `Job`/`Step`/`Tasklet` machinery — a polling sweeper doesn't need chunk/restart support, and Spring Batch's automatic retry-on-failure is actively dangerous here (risks an unintended duplicate publish on top of the already-accepted window in requirement 2).
- Catches and logs any exception from the service call so one failed cycle doesn't stop the `@Scheduled` executor from running future cycles.
- Logs a cycle summary at INFO only when rows were resolved (avoid log spam every 2 seconds in a quiet period); logs WARN if the cycle itself failed.

### `EftDecisionSweepService` (in `com.bns.fsl.rtpeft.service`, alongside the existing `*PublishService` classes)
- `sweepOnce()` — reads a claimable batch, resolves each row, returns count resolved.
- Per row: real ACI decision found → resolve `RESPONDED`; no decision and past the auto-approve cutoff → resolve `AUTO_APPROVED`; otherwise leave untouched for next cycle.
- Publishes via the **existing** publish services — do not create parallel publish logic. Resolution-side audit publish goes to ACI NRT, not FOD (pending confirmation of open question #3).
- Records the terminal row in a second transaction; a `DataIntegrityViolationException` there is an expected no-op (log INFO, another pod/region already recorded it) — everything else is a real failure (log ERROR, row stays unresolved for retry).

---

## 5. DB entity + repository

### Entity (name/join-key per section 1, defaulting to `EftStatus`/`correlationId` pending confirmation)
- `id` (surrogate PK)
- join key field (per open question #1)
- `status` enum: `PENDING`, `RESPONDED`, `AUTO_APPROVED`
- `originalRequest` — set only on the `PENDING` row
- `decision` — set only on the terminal row
- `createdAt`
- `UNIQUE (join_key, status)` — the idempotency gate for requirement 1

### Repository
- `findClaimableBatch(int batchSize)` — native query, `WITH (UPDLOCK, READPAST)`, `WHERE status = 'PENDING'` with no existing terminal row for the same key, left-joined against the read-only fraud-decision entity, ordered oldest-first, capped at the configured batch size.
- Terminal-row `save()` — relies on the unique constraint; the service handles the constraint-violation case per requirement 2/section 4.

### Indexing (confirm with DBA once the entity is finalized)
- Filtered: `(status, created_at) WHERE status = 'PENDING'` — serves the sweeper's hot read.
- Composite: `(join_key, status)` — serves the "does a terminal row already exist" check inside the same query.

---

## 6. Dead-letter handling for poison messages

- Add a `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` on the Kafka consumer side so a message that repeatedly fails processing (malformed payload, permanently unscoreable) routes to a DLQ topic after N retries instead of blocking the partition indefinitely.
- DLQ topic name goes in `AppConstants` + all profile YAMLs.
- Log at ERROR when a message is routed to DLQ, with the transaction/correlation id and failure reason — this needs an alert, not just a log line, since a DLQ message means a real payment transaction stalled.
- Out of scope for this pass: automated reprocessing from DLQ. Note it as a manual/ops process for now unless the team wants it built.

---

## 7. Logging, metrics, health, graceful shutdown, security

**Logging** — per requirement 6 above. Additionally: every sweep cycle logs its own duration; every individual row failure logs which stage failed (claim / ACI lookup / publish / terminal record).

**Metrics** (Micrometer, matching whatever registry the project already exports to — check `build.gradle`/`application.yml` for an existing Actuator/Prometheus setup before adding a new one):
- Counter: rows resolved, split by outcome (`RESPONDED` / `AUTO_APPROVED`).
- Gauge: current `PENDING` backlog size.
- Gauge or timer: age of the oldest `PENDING` row — this is the single most useful production signal (a growing number here means the sweeper is falling behind or stuck).
- Timer: sweep cycle duration.
- Counter: publish failures, DLQ routes, terminal-record constraint-violation no-ops (this last one distinguishes "expected duplicate-claim collision" from a real bug — worth watching if it spikes).

**Health/readiness**: expose whether the sweeper's last cycle completed within some multiple of its configured interval (e.g. if the last successful cycle was more than 3x the interval ago, report unhealthy) — a stalled sweeper should be visibly unhealthy, not silently doing nothing.

**Graceful shutdown**: on `SIGTERM`/context close, let an in-flight sweep cycle finish rather than being killed mid-cycle (avoids leaving rows claimed-but-unpublished). Confirm `server.shutdown: graceful` / equivalent Spring Boot config is set, and that the `@Scheduled` executor's shutdown behavior doesn't abandon a running task.

**Security**: `EftEndToEndTestController`'s manual test endpoints must not be reachable outside `local`/`ist` profiles — gate with `@Profile` or a config-driven toggle, not left open by default. Confirm no payload/PII ends up in exception messages that could surface via an actuator endpoint or error response.

---

## 8. Implementation order

1. Resolve section 1's open questions.
2. Entity + repository (section 5).
3. Config properties (`eft.sweep.interval-ms`, `eft.sweep.batch-size`, auto-approve timeout — reuse the existing `FraudConfig`/`FraudConfigRead` mechanism if the timeout should be DB-configurable).
4. `EftDecisionSweepService`.
5. `EftDecisionSweepJob` scheduling wrapper.
6. DLQ wiring on the consumer side (section 6).
7. Metrics, health check, graceful shutdown (section 7).
8. Tests (section 9).

After each file, Copilot should briefly state which existing convention it matched and flag anything it had to assume.

---

## 9. Tests

- `EftDecisionSweepService`: real decision found → publish + `RESPONDED`; no decision, past cutoff → publish + `AUTO_APPROVED`; no decision, within cutoff → no publish, row untouched; terminal insert throws `DataIntegrityViolationException` → logged INFO, no exception propagates, row still counted correctly.
- Repository test (H2/Testcontainers, matching however the project already tests JPA queries) confirming the `UPDLOCK`/`READPAST` query excludes rows that already have a terminal row.
- `EftDecisionSweepJob`: a thrown exception in one cycle doesn't prevent the next scheduled invocation.
- DLQ: a message that fails N times lands on the DLQ topic and the original partition is not blocked.
- Health check: reports unhealthy when the last successful cycle exceeds the staleness threshold.
