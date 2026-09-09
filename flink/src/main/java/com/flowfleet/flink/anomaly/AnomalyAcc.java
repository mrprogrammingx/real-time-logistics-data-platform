package com.flowfleet.flink.anomaly;

/** Keyed state for the anomaly detector (Flink POJO). */
public class AnomalyAcc {

    public double prevLat;
    public double prevLon;
    public long lastEventTime;
    public boolean hasPrev;
    /** Registered GPS-gap timer deadline (event time); 0 = none. */
    public long gapTimer;

    public AnomalyAcc() {}
}
