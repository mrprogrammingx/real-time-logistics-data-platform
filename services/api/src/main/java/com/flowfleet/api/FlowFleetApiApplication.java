package com.flowfleet.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * FlowFleet operational API.
 *
 * <p>Phase 1 scope: CRUD-ish endpoints for orders and drivers on top of PostgreSQL, so the
 * rest of the platform has a realistic OLTP source of truth to stream from. No Kafka here
 * yet — that arrives in Phase 2/3 once Debezium is put in front of this database.
 */
@SpringBootApplication
public class FlowFleetApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(FlowFleetApiApplication.class, args);
    }
}
