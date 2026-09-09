-- ============================================================================
-- Prepare the captured tables for Debezium (Phase 3).
--
-- REPLICA IDENTITY FULL makes PostgreSQL write the *entire* pre-image of a row
-- into the WAL on UPDATE/DELETE, so the Debezium event carries a complete
-- `before` block (not just the primary key). The consumers that reconstruct an
-- order's history from the stream rely on this.
--
-- Cost: slightly larger WAL. Acceptable for these low-write OLTP tables; the
-- high-volume driver_locations stream is generated, not CDC'd, so it is unaffected.
-- ============================================================================

ALTER TABLE orders        REPLICA IDENTITY FULL;
ALTER TABLE order_items   REPLICA IDENTITY FULL;
ALTER TABLE drivers       REPLICA IDENTITY FULL;
ALTER TABLE deliveries    REPLICA IDENTITY FULL;
ALTER TABLE driver_shifts REPLICA IDENTITY FULL;

-- The `flowfleet` role is a superuser in local/dev (the postgres image default),
-- so Debezium creates its own publication and replication slot. In a managed
-- environment you would instead pre-create:
--
--   CREATE PUBLICATION flowfleet_pub FOR TABLE
--     orders, order_items, drivers, deliveries, driver_shifts;
--   -- and grant REPLICATION to a dedicated, non-superuser CDC role.
