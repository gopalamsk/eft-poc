package com.bns.fsl.rtpeft.constants;

public final class AppConstants {

    private AppConstants() {}

    // Kafka
    public static final String EFT_INBOUND_TOPIC = "eft.inbound.topic";
    public static final String EFT_OUTBOUND_TOPIC = "eft.outbound.topic";

    // MQ
    public static final String ACI_REQUEST_QUEUE = "ACI.REQUEST.Q";
    public static final String NRT_AUDIT_QUEUE = "NRT.AUDIT.Q";

    // Flow identifier sent to ACI - distinguishes EFT from the existing EMT RT flow
    public static final String FLOW_EFT = "EFT";

    // Config keys (eft_config)
    public static final String CONFIG_KEY_AUTO_APPROVE_TIMEOUT_MINUTES = "auto_approve_timeout_minutes";
    public static final int DEFAULT_AUTO_APPROVE_TIMEOUT_MINUTES = 10;

    // Decisions
    public static final String DECISION_AUTO_APPROVE = "AUTO_APPROVE";

    public static final int SWEEP_BATCH_SIZE = 500;
}
