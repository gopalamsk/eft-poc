package com.bns.fsl.rtpeft.config.db;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Binds the fsl.datasource.pfm.* properties for the Azure SQL Server
 * database that owns eft_status and eft_config. "pfm" = payment fraud
 * management, this service's own schema - distinct from the Fraud Decision
 * Service database, which is accessed read-only via a separate datasource
 * (see FraudConfigRead).
 */
@Data
@ConfigurationProperties(prefix = "fsl.datasource.pfm")
public class PfmDataSourceConfigProperties {
    private String jdbcUrl;
    private String username;
    private String password;
    private int maxPoolSize = 10;
    private int minIdle = 2;
    private long connectionTimeoutMs = 30000;
}
