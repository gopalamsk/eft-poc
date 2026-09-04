package com.bns.fsl.rtpeft.config.db;

import com.bns.fsl.rtpeft.repository.FraudConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cached reader for eft_config. Values are refreshed on a fixed interval
 * rather than queried on every sweep tick, so a config change (e.g. widening
 * the auto-approve window from 10 to 30 minutes) takes effect within
 * REFRESH_INTERVAL without needing an application restart.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class FraudConfigRead {

    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(30);
    private static final String AUTO_APPROVE_KEY = "auto_approve_timeout_minutes";
    private static final int DEFAULT_AUTO_APPROVE_MINUTES = 10;

    private final FraudConfigRepository fraudConfigRepository;

    private final AtomicReference<Integer> cachedAutoApproveMinutes = new AtomicReference<>(DEFAULT_AUTO_APPROVE_MINUTES);
    private volatile Instant lastRefreshed = Instant.EPOCH;

    public int getAutoApproveTimeoutMinutes() {
        if (Duration.between(lastRefreshed, Instant.now()).compareTo(REFRESH_INTERVAL) > 0) {
            refresh();
        }
        return cachedAutoApproveMinutes.get();
    }

    @Scheduled(fixedDelay = 30000)
    public void refresh() {
        try {
            fraudConfigRepository.findValueByKey(AUTO_APPROVE_KEY)
                    .map(Integer::parseInt)
                    .ifPresent(cachedAutoApproveMinutes::set);
        } catch (Exception e) {
            log.warn("Failed to refresh eft_config, keeping previous value={}", cachedAutoApproveMinutes.get(), e);
        } finally {
            lastRefreshed = Instant.now();
        }
    }
}
