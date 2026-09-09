/**
 * Framework-free domain model for the FlowFleet platform.
 *
 * <p>Everything here is a plain {@code record} or {@code enum} with no dependency on Spring,
 * Jackson, JPA, Flink or Kafka. That keeps the model reusable in three very different
 * runtimes: the Spring Boot API, the load-generating services, and the Flink jobs.
 *
 * <p>Persistence mapping, JSON mapping and Avro mapping all live in the modules that need
 * them, not here.
 */
package com.flowfleet.common.domain;
