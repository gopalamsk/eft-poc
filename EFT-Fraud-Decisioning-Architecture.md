# EFT Fraud Decisioning Flow — Architecture Overview

## Purpose

This document describes the new EFT (batch file) fraud decisioning flow, built as `fsl-rtp-eft-service`. It is the batch-file companion to the existing EMT real-time fraud flow, sharing the same downstream fraud scoring engine (ACI) but operating on a much longer SLA — minutes, not seconds — because EFT files arrive in bulk (~10,000 transactions/file) rather than as single real-time requests.

*(Insert the architecture diagram here.)*

---

## Components

| Component | Description | Ownership |
|---|---|---|
| **phub** | Upstream payment hub. Originates EFT requests and receives final decisions. | External |
| **EDS (Req Topic / Res Topic)** | Kafka-based messaging layer between phub and our service. Req Topic carries inbound requests; Res Topic carries outbound decisions. | Shared platform |
| **kafka consumer component** | Ingestion component inside `fsl-rtp-eft-service`. Consumes EFT requests, persists them, and fans out to scoring, cache, and audit. | Owned by us |
| **pfm cache DB** | Azure SQL table (`eft_status`) tracking every request's fraud-check state — pending, responded, or auto-approved. | Owned by us |
| **Batch job component** | Scheduled sweeper inside `fsl-rtp-eft-service`. Polls the cache on a fixed cadence and resolves any request that now has a decision, or has aged past the auto-approve cutoff. | Owned by us |
| **FOD Topic** | Audit topic. Receives a copy of every inbound request at ingestion time, independent of the fraud outcome. | Shared platform |
| **fsl-fraud-decision service** | Existing, pre-built fraud scoring orchestrator. Reads ACI's response and writes the decision to `pfm cache DB`. **Not modified by this project** — read-only dependency. | Existing service (external to this project) |
| **ACI (RT MQ / NRT MQ)** | External fraud scoring engine. RT MQ carries scoring requests; NRT MQ carries the final decision event back out. | External |

---

## End-to-end flow

**Ingestion (steps 1 → 2.x)**

1. **Step 1** — phub publishes a request to Req Topic; the kafka consumer component picks it up.
2. **Steps 2.1 / 2.2 / 2.3 — fired in parallel, not in sequence:**
   - **2.1** — writes a `PENDING` row to `pfm cache DB`, carrying the full original request payload.
   - **2.2** — publishes a scoring request to ACI's RT MQ.
   - **2.3** — publishes an audit copy to the FOD Topic.

   All three happen at ingestion time, off the same inbound message. None waits on the others.

**Scoring (steps 3 → 4 — asynchronous, decoupled from ingestion)**

3. **Step 3** — ACI scores the request in the background and returns its result on RT MQ. The existing `fsl-fraud-decision service` (unowned, unmodified) consumes this.
4. **Step 4** — that service writes the decision back to `pfm cache DB`.

   This can land seconds or several minutes after step 2, depending entirely on ACI's own processing time. There is no direct call from our ingestion path to this step — it happens independently.

**Resolution (steps 5 → 7 — driven by the sweeper's own clock, not by ingestion)**

5. **Step 5** — the Batch job component (sweeper) runs on a fixed cadence (currently every 2 seconds), reading pending rows from `pfm cache DB` and checking whether ACI has answered yet.
6. **Step 6** — for every row it resolves (real decision, or auto-approved because the configurable timeout has elapsed), it publishes the outcome to ACI's NRT MQ.
7. **Step 7** — it also publishes the outcome to Res Topic, which phub consumes as the final answer.

---

## Why this shape (key design decisions)

- **A single scheduled sweeper, not per-request polling.** Holding thousands of in-flight requests in memory to poll individually doesn't scale to file-batch volume; one query per cycle does.
- **Auto-approve on timeout.** If ACI hasn't answered within a configurable window (default 10 minutes), the sweeper auto-approves — because the bank already releases the payment after that window regardless. A late ACI answer after that point is intentionally ignored; this was confirmed as an acceptable trade-off by the business.
- **Publish-first, then record.** The sweeper publishes the outcome to Kafka/NRT *before* writing the terminal row to the DB, so a message still goes out even during a DB outage. This trades exactly-once delivery for at-least-once — downstream consumers must dedupe on `correlation_id`.
- **Cross-region safety without blocking.** Both GCP regions share one Azure SQL DB. The sweeper's read query uses `WITH (UPDLOCK, READPAST)` so a second region's cycle simply skips rows the first region is already mid-processing, rather than waiting on them — no cross-region contention, and a unique DB constraint is the backstop against any retry-induced duplicate.
- **FOD is ingestion-only.** The audit copy is written once, at intake (2.3). It is deliberately not re-published from the resolution side (steps 5–7) — the audit trail records what came in, not what was decided.

---

## Known failure handling

| Scenario | Behavior |
|---|---|
| DB is down at ingestion | Kafka offset for the inbound message is not committed until the DB write succeeds — message is retried, not lost. |
| ACI is down or slow | Auto-approve cutoff is the safety net; the request still resolves within the configured window. |
| Two regions' sweepers overlap | `READPAST` means the second region picks a different batch of rows — no blocking, no duplicate pick under normal operation. |
| Sweeper crashes after publish, before DB commit | Row remains `PENDING`, gets reprocessed next cycle — can cause a duplicate publish. Accepted trade-off; downstream must dedupe. |
| ACI answers after the auto-approve cutoff | Intentionally ignored — no reconciliation. Business-confirmed. |

---

## Open items

- DLQ topic for poison/malformed inbound messages — not yet built.
- Downstream consumer idempotency on `correlation_id` — owned by the teams consuming NRT MQ / Res Topic, not by this service.
