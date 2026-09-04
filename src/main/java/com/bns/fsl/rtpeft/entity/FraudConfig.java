package com.bns.fsl.rtpeft.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

/**
 * Maps to eft_config. Holds tunable business parameters for the EFT flow -
 * currently just the auto-approve timeout, but structured to hold more
 * key/value pairs per environment without a schema change.
 */
@Entity
@Table(name = "eft_config")
@IdClass(FraudConfigCompositeId.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FraudConfig {

    @Id
    @Column(name = "config_key", length = 64)
    private String configKey;

    @Id
    @Column(name = "environment", length = 16)
    private String environment;

    @Column(name = "config_value", length = 64, nullable = false)
    private String configValue;

    @Column(name = "updated_at")
    private Instant updatedAt;
}
