package com.bns.fsl.rtpeft.util;

import java.util.UUID;

public final class CommonUtil {

    private CommonUtil() {}

    /** Generates a correlation id when the inbound file row has none of its own. */
    public static String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    public static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
