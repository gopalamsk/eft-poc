package com.bns.fsl.rtpeft.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Result of evaluating one pending row in the sweeper: either ACI answered
 * in time, or the configured auto-approve window elapsed with no answer.
 * See EftDecisionSweepJob for how this is derived.
 */
@Getter
@Builder
@AllArgsConstructor
public class EftDecisionOutcome {
    private String correlationId;
    private String originalRequest;
    private String decision;
    private com.bns.fsl.rtpeft.entity.EftStatusValue terminalStatus;
}
