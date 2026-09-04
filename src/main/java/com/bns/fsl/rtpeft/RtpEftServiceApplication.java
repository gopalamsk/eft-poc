package com.bns.fsl.rtpeft;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the FSL RTP EFT service.
 *
 * Consumes batched EFT transactions from Kafka, submits each transaction to the
 * ACI scoring engine over MQ, and reconciles the resulting decision on a fixed
 * schedule via {@link com.bns.fsl.rtpeft.batch.EftDecisionSweepJob}.
 *
 * Fraud Decision Service (owned by another team) is never written to by this
 * service - it is read only, via {@link com.bns.fsl.rtpeft.repository.FraudDecisionReadRepository}.
 */
@SpringBootApplication
@EnableScheduling
public class RtpEftServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RtpEftServiceApplication.class, args);
    }
}
