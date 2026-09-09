package com.flowfleet.generator;

import com.flowfleet.generator.config.GeneratorProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Simulates a fleet of drivers and streams their GPS positions to Kafka as Avro
 * {@code DriverLocation} events.
 *
 * <p>This is the platform's high-volume source. Order / driver / delivery / shift changes
 * are <em>not</em> produced here — those arrive via Debezium CDC in Phase 3.
 */
@SpringBootApplication
@EnableConfigurationProperties(GeneratorProperties.class)
public class LocationGeneratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(LocationGeneratorApplication.class, args);
    }
}
