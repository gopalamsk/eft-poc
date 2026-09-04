package com.bns.fsl.rtpeft.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Message body published to the ACI request MQ. */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AciRequestMessage {
    private String correlationId;
    private String flow;
    private String payload;
}
