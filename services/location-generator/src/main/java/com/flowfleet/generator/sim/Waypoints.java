package com.flowfleet.generator.sim;

import com.flowfleet.common.domain.GeoPoint;
import java.util.List;

/**
 * Fixed points of interest in central Yerevan, matching the restaurants and geofences
 * seeded by the API's Flyway migration {@code V2__seed_reference_data.sql}. Keeping the
 * simulator's map aligned with the database means Phase 4's geofencing job will actually
 * see ENTER/EXIT transitions.
 */
final class Waypoints {

    private Waypoints() {}

    static final GeoPoint CITY_CENTER = new GeoPoint(40.1792, 44.5152);

    /** Restaurant pickup locations (lat, lon). */
    static final List<GeoPoint> RESTAURANTS = List.of(
            new GeoPoint(40.1776, 44.5133),  // Lavash, Republic Square
            new GeoPoint(40.1872, 44.5153),  // Ponchik, Opera
            new GeoPoint(40.1907, 44.5194)); // Tashir Pizza, Cascade

    /** Bounding box for randomly placed customers (Kentron district). */
    static final double LAT_MIN = 40.170, LAT_MAX = 40.195;
    static final double LON_MIN = 44.498, LON_MAX = 44.530;
}
