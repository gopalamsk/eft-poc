package com.bns.fsl.rtpeft.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/** One EFT transaction as read off the inbound Kafka topic. */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EftTransactionPayload {
    private String correlationId;
    private String fileId;
    private String accountNumber;
    private BigDecimal amount;
    private String currency;
    private String payeeName;
    private String rawJson;
}
