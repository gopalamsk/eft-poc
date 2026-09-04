package com.bns.fsl.rtpeft.exception;

/** Thrown for unexpected internal failures (DB, mapping, config) that don't
 *  fit a more specific exception type. */
public class InternalSystemException extends RtpEftException {
    public InternalSystemException(String message, Throwable cause) { super(message, cause); }
    public InternalSystemException(String message) { super(message); }
}
