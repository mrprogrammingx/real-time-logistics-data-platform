package com.flowfleet.flink.geo;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.WKTReader;

/** JTS helpers. Coordinates are (x = longitude, y = latitude), SRID 4326. */
public final class Geometries {

    private static final GeometryFactory GF = new GeometryFactory(new PrecisionModel(), 4326);

    private Geometries() {}

    public static PreparedGeometry prepareWkt(String wkt) {
        try {
            Geometry g = new WKTReader(GF).read(wkt);
            return PreparedGeometryFactory.prepare(g);
        } catch (Exception e) {
            throw new IllegalArgumentException("bad WKT: " + wkt, e);
        }
    }

    public static boolean contains(PreparedGeometry polygon, double latitude, double longitude) {
        return polygon.contains(GF.createPoint(new Coordinate(longitude, latitude)));
    }
}
