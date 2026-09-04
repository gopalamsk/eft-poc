package com.bns.fsl.rtpeft.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Composite key for eft_config: a config key scoped to an environment, so
 * the same key (e.g. auto_approve_timeout_minutes) can hold a different
 * value per profile (ist / uat / prd) in one shared table if desired.
 */
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class FraudConfigCompositeId implements Serializable {
    private String configKey;
    private String environment;
}
