package ir.neo.common.geohash;

import ch.hsr.geohash.BoundingBox;
import ch.hsr.geohash.GeoHash;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

/**
 * Produces a geohash cover for polygonal WGS84 geometries.
 *
 * <p>A cover contains every geohash cell that intersects the supplied geometry. Therefore, cells
 * touching a polygon boundary are included, while cells entirely inside a polygon hole are not.
 * Coordinates must use longitude as {@code x} and latitude as {@code y}.
 */
public final class GeohashCoverageGenerator {

    private static final char[] BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();
    private static final int MIN_PRECISION = 1;
    private static final int MAX_PRECISION = 12;

    private GeohashCoverageGenerator() {
    }

    /**
     * Parses a WKT polygon and covers it with geohashes. Only Polygon and MultiPolygon geometries
     * are accepted.
     *
     * @param wkt a Polygon or MultiPolygon encoded as WKT
     * @param precision the geohash character precision, from 1 to 12
     * @return an immutable, insertion-ordered set of intersecting geohashes
     */
    public static Set<String> cover(String wkt, int precision) {
        return coverGeometry(parseWkt(wkt), precision);
    }

    /**
     * Covers a polygon with geohashes of the requested character precision.
     *
     * @param polygon the WGS84 polygon to cover
     * @param precision the geohash character precision, from 1 to 12
     * @return an immutable, insertion-ordered set of intersecting geohashes
     */
    public static Set<String> cover(Polygon polygon, int precision) {
        return coverGeometry(polygon, precision);
    }

    /**
     * Covers every polygon in a multipolygon with geohashes of the requested character precision.
     *
     * @param multiPolygon the WGS84 multipolygon to cover
     * @param precision the geohash character precision, from 1 to 12
     * @return an immutable, insertion-ordered set of intersecting geohashes
     */
    public static Set<String> cover(MultiPolygon multiPolygon, int precision) {
        return coverGeometry(multiPolygon, precision);
    }

    private static Geometry parseWkt(String wkt) {
        if (wkt == null || wkt.isBlank()) {
            throw new IllegalArgumentException("WKT must not be blank");
        }

        Geometry geometry;
        try {
            geometry = new WKTReader().read(wkt.trim());
        } catch (ParseException exception) {
            throw new IllegalArgumentException("geometry is not valid WKT", exception);
        }
        if (!(geometry instanceof Polygon) && !(geometry instanceof MultiPolygon)) {
            throw new IllegalArgumentException("geometry must be a Polygon or MultiPolygon");
        }
        return geometry;
    }

    private static Set<String> coverGeometry(Geometry geometry, int precision) {
        validate(geometry, precision);
        if (geometry.isEmpty()) {
            return Set.of();
        }

        PreparedGeometry preparedGeometry = PreparedGeometryFactory.prepare(geometry);
        GeometryFactory geometryFactory = geometry.getFactory();
        Set<String> geohashes = new LinkedHashSet<>();
        for (char firstCharacter : BASE32) {
            addIntersectingHashes(
                    String.valueOf(firstCharacter),
                    precision,
                    preparedGeometry,
                    geometryFactory,
                    geohashes
            );
        }
        return Collections.unmodifiableSet(geohashes);
    }

    private static void addIntersectingHashes(
            String geohash,
            int precision,
            PreparedGeometry geometry,
            GeometryFactory geometryFactory,
            Set<String> result
    ) {
        Geometry cell = createCell(geohash, geometryFactory);
        if (!geometry.intersects(cell)) {
            return;
        }
        if (geohash.length() == precision) {
            result.add(geohash);
            return;
        }

        for (char nextCharacter : BASE32) {
            addIntersectingHashes(
                    geohash + nextCharacter,
                    precision,
                    geometry,
                    geometryFactory,
                    result
            );
        }
    }

    private static Geometry createCell(String geohash, GeometryFactory geometryFactory) {
        BoundingBox bounds = GeoHash.fromGeohashString(geohash).getBoundingBox();
        Envelope envelope = new Envelope(
                bounds.getWestLongitude(),
                bounds.getEastLongitude(),
                bounds.getSouthLatitude(),
                bounds.getNorthLatitude()
        );
        return geometryFactory.toGeometry(envelope);
    }

    private static void validate(Geometry geometry, int precision) {
        Objects.requireNonNull(geometry, "geometry must not be null");
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("precision must be between 1 and 12");
        }
        if (!geometry.isValid()) {
            throw new IllegalArgumentException("geometry must be valid");
        }
        for (Coordinate coordinate : geometry.getCoordinates()) {
            if (!isValidLongitude(coordinate.getX()) || !isValidLatitude(coordinate.getY())) {
                throw new IllegalArgumentException(
                        "geometry coordinates must be WGS84 longitude/latitude values"
                );
            }
        }
    }

    private static boolean isValidLongitude(double longitude) {
        return Double.isFinite(longitude) && longitude >= -180.0 && longitude <= 180.0;
    }

    private static boolean isValidLatitude(double latitude) {
        return Double.isFinite(latitude) && latitude >= -90.0 && latitude <= 90.0;
    }
}
