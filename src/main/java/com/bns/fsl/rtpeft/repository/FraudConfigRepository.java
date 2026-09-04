package com.bns.fsl.rtpeft.repository;

import com.bns.fsl.rtpeft.entity.FraudConfig;
import com.bns.fsl.rtpeft.entity.FraudConfigCompositeId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;

public interface FraudConfigRepository extends JpaRepository<FraudConfig, FraudConfigCompositeId> {

    @Query("select f.configValue from FraudConfig f where f.configKey = :key and f.environment = :#{@activeProfile}")
    Optional<String> findValueByKey(@Param("key") String key);
}
