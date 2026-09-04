# fsl-rtp-eft-service

Spring Boot microservice implementing the EFT (batch file) fraud decisioning
flow, as a companion to the existing EMT real-time (RT) flow. This service
receives batched EFT transactions from the Optimus payment hub, submits each
one to the ACI scoring engine, and reconciles the decision on a fixed
schedule — without holding any per-request thread or context object in
memory while waiting for a score.

---

## 1. Why this looks different from the EMT RT flow

EMT RT bridges an async MQ call to a synchronous REST response by polling a
database from the calling thread for up to two seconds. That works because
the wait is short and bounded.

EFT's SLA is minutes, not seconds (98% resolved fast, hard ceiling on an
auto-approve window — see §4), and a single file can contain thousands of
transactions. Holding a thread and an in-memory context object per
transaction for that long would mean file volume directly drives memory
consumption. Instead, EFT is fully event-driven on both ends:

- **Inbound**: Kafka topic, not a synchronous API call.
- **Outbound**: Kafka topic + an audit queue (NRTMQ), published by a
  scheduled sweeper — not a thread waiting on the request that came in.

All in-flight state lives in one database table (`eft_status`), never in
application memory. A crash, restart, or redeploy loses nothing — the next
sweep cycle picks up exactly where the last one left off.

---

## 2. High-level flow

```
Optimus payment hub
      │  (batch file, split into one Kafka message per transaction)
      ▼
Kafka inbound topic
      │
      ▼
EftInboundKafkaConsumer → EftTransactionProcessor
      │  1. INSERT eft_status (status=PENDING, full payload)
      │  2. publish to ACI request queue
      ▼
ACI (scoring engine)
      │  responds via ACI response queue
      ▼
Fraud Decision Service (a different team's service — UNCHANGED)
      │  consumes ACI's response, saves score to its own table
      ▼
fraud_decision_table  (read-only to this service)

      ┌─────────────────────────────────────────────┐
      │  EftDecisionSweepJob — every 2 seconds        │
      │  1. SELECT pending rows, LEFT JOIN fraud_decision_table
      │  2. branch: ACI answered  →  RESPONDED
      │             no answer, past cutoff → AUTO_APPROVED
      │  3. PUBLISH FIRST to Kafka outbound topic + NRTMQ
      │  4. best-effort INSERT the terminal row
      └─────────────────────────────────────────────┘
```

---

## 3. Data model

One new database (Azure SQL Server) owned entirely by this service, holding
two tables. `fraud_decision_table` is **not** owned by this service — it's
mapped read-only via a separate entity manager (`FraudDecisionReadRepository`)
and this service never runs a schema change or write against it.

### `eft_status` — single append-only table

| column | notes |
|---|---|
| `id` | surrogate PK |
| `correlation_id` | ties a row back to the original transaction |
| `status` | `PENDING` / `RESPONDED` / `AUTO_APPROVED` |
| `original_request` | full payload — **only set on the PENDING row** |
| `decision` | the resolved decision — **only set on the terminal row** |
| `created_at` | drives both the sweep ordering and the auto-approve cutoff |

`UNIQUE (correlation_id, status)` is the whole idempotency mechanism: a
second attempt to insert the same terminal row fails at the database level,
rather than depending on application code correctly checking an update's
affected-row count.

**Why one table, not three:** an earlier design split this into a request
table, a status table, and a separate idempotency ledger. The team's lead
consolidated it — the PENDING row already holds the payload, so the sweeper
can pull `original_request` from the same table it's already querying for
status, and the unique constraint on the terminal insert does the same job
the separate ledger did. See §6 for the trade-off this consolidation
introduced.

### `eft_config` — tunable parameters

| column | notes |
|---|---|
| `config_key` | e.g. `auto_approve_timeout_minutes` |
| `environment` | `local` / `ist` / `uat` / `prd` |
| `config_value` | string, parsed by the reader |
| `updated_at` | audit only |

Read via `FraudConfigRead`, cached with a 30-second refresh — a value
change (e.g. widening 10 minutes to 30) takes effect within 30 seconds,
**no application restart required**.

DDL: `DBScripts/01_create_eft_status.sql`, `DBScripts/02_create_eft_config.sql`.

---

## 4. The business rule this flow encodes

| Time since ingestion | ACI has responded | ACI hasn't responded |
|---|---|---|
| within the configured window (default 10 min) | publish ACI's real decision (approve / reject / pending), mark `RESPONDED` | leave `PENDING`, re-checked next cycle |
| past the window | irrelevant — **the payment has already released** | publish `AUTO_APPROVE`, mark `AUTO_APPROVED` |

A late ACI response after the cutoff is intentionally never looked at again
— this was confirmed as an accepted business trade-off (payment funds are
already gone by that point), so no reconciliation job exists for it. If that
assumption ever changes, this is the paragraph to revisit.

---

## 5. The sweeper — `EftDecisionSweepJob`

- Spring Batch `Tasklet`, triggered by a `@Scheduled(fixedDelay = 2000)`
  method — **one job, one query, one 2-second cadence**. There is no
  separate timeout job; the auto-approve branch and the "ACI answered"
  branch are both resolved from the same query result, in
  `EftDecisionSweepService`.
- A `Tasklet` (not a chunk-oriented step) was chosen deliberately — this is
  a repeating poll over an unbounded stream of work, not a one-off batch
  over a fixed dataset, which is what Spring Batch's chunk model targets.
- The query (`EftStatusRepository#findPendingBatch`) uses
  `WITH (UPDLOCK, READPAST)` — SQL Server's equivalent of Postgres'
  `SKIP LOCKED` — so two application instances (both GCP regions, one
  shared Azure SQL database) never grab the same batch of rows in the same
  cycle.
- **Publish-first, record-after**: `PhubResponsePublishService` and
  `FodAuditPublishService` are called *before* the terminal row is written.
  This was an explicit requirement — the outbound message must go out even
  if the database is down at that moment. See §6 for what this trades away.

---

## 6. Failure scenarios (for the architect review)

| Scenario | Behavior | Notes |
|---|---|---|
| App instance crashes mid-sweep | In-flight batch simply isn't marked terminal | Next cycle, any instance, picks the same rows back up via `READPAST` |
| Both GCP regions down | New ingestion blocked; nothing lost | State lives in `eft_status`, not memory — resumes exactly where it left off |
| Azure SQL unreachable | Ingestion: don't commit Kafka offset until the DB write succeeds — message redelivers. Sweeper: query fails, logs, retries next 2s tick — publish is not blocked since it doesn't depend on a prior successful DB read within the same cycle for messages already resolved in memory | The DB's own HA/failover story becomes this service's effective RTO |
| ACI down or slow | Rows accumulate as PENDING; auto-approve cutoff is the safety net | Business-approved: payment releases regardless |
| **Terminal insert fails after publish already succeeded** | Row stays PENDING → picked up again next cycle → **published again** | This is the real cost of publish-first: exactly-once becomes at-least-once. **Downstream Kafka/NRTMQ consumers must dedupe on `correlation_id`.** |
| Poison message (malformed row ACI can never score) | Ages out via the auto-approve cutoff | Recommend a DLQ topic for ingestion-time validation failures rather than relying solely on the cutoff |
| Late ACI response after auto-approve | Silently ignored | Confirmed acceptable — funds already released |

---

## 7. Ownership boundaries — do not cross these

- **`fraud_decision_table`** belongs to Fraud Decision Service. This service
  only ever reads it (`FraudDecisionReadRepository`, a bare `Repository`
  with no write methods). No schema changes, no writes, ever.
- **ACI's response queue** is consumed exclusively by Fraud Decision
  Service. This service never listens to it — the eventual decision is
  discovered by querying `fraud_decision_table`, not by consuming a queue.
- **`eft_status` / `eft_config`** are separate from any EMT table by design
  — EFT's bursty, file-driven load must never contend with EMT's live
  2-second SLA path on the same table.

---

## 8. Project structure

```
com.bns.fsl.rtpeft
├── aspect        LoggableMethodExecution, LoggingAspect — method timing logs
├── batch         EftDecisionSweepJob — the sweeper's Spring Batch + schedule wiring
├── config
│   ├── db        Azure SQL datasource, cached eft_config reader
│   ├── kafka     consumer/producer config
│   └── mq        IBM MQ connection (ACI request queue, NRT audit queue)
├── constants     AppConstants — topic/queue names, config keys
├── consumer      EftInboundKafkaConsumer
├── context       EftTransactionContext — ingestion-time only, discarded after processing
├── controller    EftEndToEndTestController — manual test endpoints, non-production
├── entity        EftStatus, EftConfig, FraudDecisionRecord (read-only)
├── exception     RtpEftException hierarchy + GlobalExceptionHandler
├── mapper        MapStruct mappers: EFTRTMapper (→ ACI request), EFTNRTMapper (→ audit)
├── model         DTOs / projections
├── processor     EftTransactionProcessor — ingestion write + ACI publish
├── repository    EftStatusRepository, FraudConfigRepository, FraudDecisionReadRepository
├── service       AciMqService, PhubResponsePublishService, FodAuditPublishService,
│                 EftDecisionSweepService (the sweeper's actual per-row logic)
└── util          Base64Utils, CommonUtil, JsonUtil
```

---

## 9. Configuration

Set per environment via `application-{profile}.yml` and environment
variables (`PFM_DB_URL`, `PFM_DB_USERNAME`, `PFM_DB_PASSWORD`,
`KAFKA_BOOTSTRAP_SERVERS`, `MQ_HOST`, `MQ_PORT`, `MQ_CHANNEL`,
`MQ_QUEUE_MANAGER`, `MQ_USERNAME`, `MQ_PASSWORD`). Secrets are injected via
vault in deployed environments — `.bns/security.yaml` is a placeholder only,
nothing sensitive is committed to the repo.

Business-tunable value (`auto_approve_timeout_minutes`) lives in the
`eft_config` table, not in YAML — change it with an `UPDATE` statement, no
deploy required (30-second cache refresh).

---

## 10. Running locally

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

Requires a local SQL Server instance (or point `PFM_DB_URL` at a dev Azure
SQL DB), Kafka, and an MQ instance reachable per `application-local.yml`.

Manual test endpoints (local/IST only — see `EftEndToEndTestController`):

```bash
# push one transaction through ingestion without Kafka
curl -X POST localhost:8080/internal/eft/test/ingest -d '{"correlationId":"test-1", ...}'

# trigger a sweep cycle immediately, without waiting for the 2s schedule
curl -X POST localhost:8080/internal/eft/test/sweep-now
```

---

## 11. Open items / not yet decided

- **DLQ topic** for ingestion-time validation failures (poison messages) —
  referenced in `EftInboundKafkaConsumer` but not yet wired.
- **Downstream idempotency** on the Kafka outbound topic and NRTMQ
  consumers — required given the publish-first design (§6), owned by
  those consuming teams, not this service.
- **Region-to-region network path** between the two GCP app regions and the
  shared Azure SQL database — affects whether the 2-second sweep cadence
  holds up under real cross-cloud latency; worth confirming with
  infrastructure before load testing.
