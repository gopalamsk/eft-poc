package com.bns.fsl.rtpeft.exception;

/** Thrown when a publish to the ACI request queue or the NRT audit queue fails. */
public class MqPublishException extends RtpEftException {
    public MqPublishException(String message, Throwable cause) { super(message, cause); }
}
