package com.flowfleet.common.domain;

/** Operational status of a driver. Drives dispatch decisions and shift accounting. */
public enum DriverStatus {
    /** Not logged in / not accepting work. */
    OFFLINE,
    /** On shift and eligible for assignment. */
    AVAILABLE,
    /** Currently fulfilling a delivery. */
    ON_DELIVERY,
    /** On shift but temporarily paused. */
    ON_BREAK
}
