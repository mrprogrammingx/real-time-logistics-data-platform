package com.flowfleet.flink.geo;

/**
 * A geofence as it travels on the broadcast stream: a WKT polygon plus metadata. A Flink
 * POJO (public no-arg ctor + public fields) so it serializes without Kryo. The parsed /
 * prepared JTS geometry is rebuilt on each subtask from {@code polygonWkt}, not shipped.
 */
public class Geofence {

    public long id;
    public String name;
    public String type;
    public String polygonWkt;

    public Geofence() {}

    public Geofence(long id, String name, String type, String polygonWkt) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.polygonWkt = polygonWkt;
    }
}
