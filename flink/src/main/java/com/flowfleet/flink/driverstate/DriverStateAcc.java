package com.flowfleet.flink.driverstate;

/**
 * Keyed state for one driver (a Flink POJO: public no-arg ctor + public fields, so the
 * POJO serializer handles it and state stays schema-evolvable).
 */
public class DriverStateAcc {

    public double lat;
    public double lon;
    public double prevLat;
    public double prevLon;
    public boolean hasPrev;

    public Double reportedSpeedKph;
    public Integer headingDegrees;
    public String vehicleType;

    public long lastEventTime;
    public long sampleCount;
    public double tripDistanceMeters;

    /** Currently-registered GPS-gap timer deadline (event time); 0 = none. */
    public long gapTimer;

    public DriverStateAcc() {}
}
