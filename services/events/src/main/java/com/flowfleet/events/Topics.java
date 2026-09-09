package com.flowfleet.events;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The FlowFleet topic catalogue.
 *
 * <p>Naming: {@code flowfleet.<domain>.<kind>} — {@code .cdc} = raw Debezium,
 * {@code .events} = business events, {@code .alerts} = actionable, {@code .dlq} = dead
 * letters. Keys: driver-scoped topics use {@code driver_id}, order-scoped use
 * {@code order_id}; {@code deliveries.cdc} is keyed by {@code order_id} so an order and its
 * delivery co-partition.
 *
 * <p>The {@link #local()} specs are sized for a single-broker dev cluster (RF 1, small
 * partition counts). Production values are documented in
 * {@code architecture/kafka-design.md} and set by the Kubernetes topic operator later.
 */
public final class Topics {

    private Topics() {}

    // --- produced by the platform ------------------------------------------------
    public static final String DRIVER_LOCATIONS = "flowfleet.driver.locations";
    public static final String DRIVER_GEOFENCE_EVENTS = "flowfleet.driver.geofence-events";
    public static final String DELIVERY_EVENTS = "flowfleet.delivery.events";
    public static final String DELIVERY_ALERTS = "flowfleet.delivery.alerts";
    public static final String DRIVER_LOCATIONS_DLQ = "flowfleet.driver.locations.dlq";

    // --- produced by Debezium (Phase 3): flowfleet.public.<table> routed to flowfleet.<table>.cdc
    public static final String ORDERS_CDC = "flowfleet.orders.cdc";
    public static final String ORDER_ITEMS_CDC = "flowfleet.order_items.cdc";
    public static final String DRIVERS_CDC = "flowfleet.drivers.cdc";
    public static final String DELIVERIES_CDC = "flowfleet.deliveries.cdc";
    public static final String DRIVER_SHIFTS_CDC = "flowfleet.driver_shifts.cdc";

    public static List<String> cdcTopics() {
        return List.of(ORDERS_CDC, ORDER_ITEMS_CDC, DRIVERS_CDC, DELIVERIES_CDC, DRIVER_SHIFTS_CDC);
    }

    /** Consumer group id for the Phase 2 rebalance experiment. */
    public static final String LOCATION_CONSUMER_GROUP = "flowfleet.location-consumer";

    /** Topics the local dev cluster should have. CDC topics are created by Debezium. */
    public static List<TopicSpec> local() {
        return List.of(
                new TopicSpec(DRIVER_LOCATIONS, 6, (short) 1,
                        merge(TopicSpec.retention(Duration.ofHours(24)),
                              Map.of("cleanup.policy", "delete"))),
                new TopicSpec(DRIVER_GEOFENCE_EVENTS, 3, (short) 1,
                        TopicSpec.retention(Duration.ofDays(7))),
                new TopicSpec(DELIVERY_EVENTS, 3, (short) 1,
                        TopicSpec.retention(Duration.ofDays(30))),
                new TopicSpec(DELIVERY_ALERTS, 3, (short) 1,
                        TopicSpec.retention(Duration.ofDays(30))),
                new TopicSpec(DRIVER_LOCATIONS_DLQ, 3, (short) 1,
                        TopicSpec.retention(Duration.ofDays(14))));
    }

    private static Map<String, String> merge(Map<String, String> a, Map<String, String> b) {
        var m = new java.util.HashMap<>(a);
        m.putAll(b);
        return Map.copyOf(m);
    }
}
