package com.flowfleet.api.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
class AppConfig {

    /** Injected everywhere a timestamp is needed, so tests can pin "now". */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
