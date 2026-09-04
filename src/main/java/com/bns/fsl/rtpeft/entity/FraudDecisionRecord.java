package com.bns.fsl.rtpeft.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * READ-ONLY mapping onto the Fraud Decision Service's own table.
 *
 * This service never writes to this entity and never runs a schema change
 * against it - it is owned entirely by Fraud Decision Service. Do not add
 * @Version, cascade, or write operations here. See README "Ownership
 * boundaries" before touching this class.
 */
@Entity
@Table(name = "fraud_decision_table")
@Getter
@Setter
@NoArgsConstructor
public class FraudDecisionRecord {

    @Id
    @Column(name = "correlation_id", length = 64)
    private String correlationId;

    @Column(name = "score")
    private Double score;

    @Column(name = "decision", length = 20)
    private String decision;
}
