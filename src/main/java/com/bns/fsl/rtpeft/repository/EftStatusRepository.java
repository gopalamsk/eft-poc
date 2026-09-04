package com.bns.fsl.rtpeft.repository;

import com.bns.fsl.rtpeft.entity.EftStatus;
import com.bns.fsl.rtpeft.model.PendingEftRow;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.data.repository.query.Param;

import jakarta.persistence.QueryHint;
import java.util.List;

/**
 * All access to eft_status. The sweeper query intentionally uses a native
 * query so the SQL Server row-locking hint (UPDLOCK, READPAST) is explicit
 * and visible here, rather than relying on JPA's LockModeType translating to
 * the right dialect-specific hint.
 */
public interface EftStatusRepository extends JpaRepository<EftStatus, Long> {

    /**
     * Fetches up to {@code batchSize} PENDING rows that have no terminal
     * (RESPONDED / AUTO_APPROVED) row yet, left-joined against the Fraud
     * Decision Service table so the caller can see whether ACI has already
     * answered. UPDLOCK + READPAST is what lets two application instances
     * (both GCP regions, one shared Azure SQL database) run this
     * concurrently without picking the same batch of rows - see README
     * "Region safety".
     */
    @Query(value = """
        SELECT p.correlation_id AS correlationId,
               p.original_request AS originalRequest,
               p.created_at AS createdAt,
               f.decision AS aciDecision
        FROM eft_status p WITH (UPDLOCK, READPAST)
        LEFT JOIN fraud_decision_table f ON f.correlation_id = p.correlation_id
        WHERE p.status = 'PENDING'
        AND NOT EXISTS (
            SELECT 1 FROM eft_status t
            WHERE t.correlation_id = p.correlation_id
            AND t.status IN ('RESPONDED','AUTO_APPROVED')
        )
        ORDER BY p.created_at ASC
        OFFSET 0 ROWS FETCH NEXT :batchSize ROWS ONLY
        """, nativeQuery = true)
    List<PendingEftRow> findPendingBatch(@Param("batchSize") int batchSize);

    boolean existsByCorrelationIdAndStatus(String correlationId, com.bns.fsl.rtpeft.entity.EftStatusValue status);
}
