package com.bns.fsl.rtpeft.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Exposes the active Spring profile as a bean so FraudConfigRepository's
 *  SpEL query (@activeProfile) can scope eft_config lookups per environment. */
@Configuration
public class ActiveProfileHolder {

    @Value("${spring.profiles.active:local}")
    private String activeProfile;

    @Bean("activeProfile")
    public String activeProfile() {
        return activeProfile;
    }
}
