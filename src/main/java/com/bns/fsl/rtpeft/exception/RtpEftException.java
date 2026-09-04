package com.bns.fsl.rtpeft.exception;

/** Base unchecked exception for this service. */
public class RtpEftException extends RuntimeException {
    public RtpEftException(String message) { super(message); }
    public RtpEftException(String message, Throwable cause) { super(message, cause); }
}
