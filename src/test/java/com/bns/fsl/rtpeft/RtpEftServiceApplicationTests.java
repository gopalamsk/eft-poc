package com.bns.fsl.rtpeft;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class RtpEftServiceApplicationTests {

    @Test
    void contextLoads() {
        // Verifies both entity managers (pfm + fraud decision read-only)
        // and the Kafka/MQ bean wiring come up cleanly.
    }
}
