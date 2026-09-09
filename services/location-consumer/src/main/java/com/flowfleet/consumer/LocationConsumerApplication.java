package com.flowfleet.consumer;

import com.flowfleet.consumer.config.ConsumerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * A single member of the {@code flowfleet.location-consumer} group. Start several
 * instances (different {@code SERVER_PORT}) and kill one to watch a rebalance:
 *
 * <pre>
 *   6 partitions, 3 consumers  -> 2 partitions each
 *   kill one                   -> its 2 partitions reassigned to the survivors
 * </pre>
 */
@SpringBootApplication
@EnableConfigurationProperties(ConsumerProperties.class)
public class LocationConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(LocationConsumerApplication.class, args);
    }
}
