package com.bns.fsl.rtpeft.repository;

import com.bns.fsl.rtpeft.entity.FraudDecisionRecord;
import org.springframework.data.repository.Repository;

import java.util.Optional;

/**
 * Deliberately extends the bare {@link Repository} marker interface, not
 * JpaRepository - this repository is read-only by construction, no save()
 * or delete() methods exist. Registered against the Fraud Decision Service's
 * own datasource/entity-manager, kept fully separate from the pfm
 * entity manager that owns eft_status and eft_config.
 */
public interface FraudDecisionReadRepository extends Repository<FraudDecisionRecord, String> {
    Optional<FraudDecisionRecord> findByCorrelationId(String correlationId);
}
