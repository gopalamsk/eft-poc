package com.bns.fsl.rtpeft.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Maps to eft_status - the single append-only table backing the whole EFT
 * flow (see README, "Data model").
 *
 * One row per (correlation_id, status):
 *  - the PENDING row is written at ingestion and carries the full request
 *    payload
 *  - the RESPONDED / AUTO_APPROVED row is written by the sweeper and
 *    carries only the decision - the payload is re-read from the PENDING row
 *
 * The unique constraint on (correlation_id, status) is what makes the
 * terminal insert an atomic, database-enforced idempotency gate: a second
 * attempt to insert the same terminal row fails, rather than relying on
 * application code to check an update's affected-row count.
 */
@Entity
@Table(name = "eft_status", uniqueConstraints = @UniqueConstraint(
        name = "uq_eft_status_corr_status", columnNames = {"correlation_id", "status"}))
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EftStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "correlation_id", length = 64, nullable = false)
    private String correlationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 20, nullable = false)
    private EftStatusValue status;

    @Lob
    @Column(name = "original_request")
    private String originalRequest;

    @Column(name = "decision", length = 20)
    private String decision;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
