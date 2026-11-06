package ir.neo.common.geohash;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.hsr.geohash.GeoHash;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

class GeohashCoverageGeneratorTest {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
    private static final int PRECISION = 7;

    @Test
    void parsesWktPolygon() {
        String polygon = """
                POLYGON ((
                  51.380 35.690, 51.390 35.690, 51.390 35.700,
                  51.380 35.700, 51.380 35.690
                ))
                """;

        Set<String> result = GeohashCoverageGenerator.cover(polygon, PRECISION);

        assertTrue(result.contains(geohash(35.695, 51.385)));
    }

    @Test
    void parsesWktMultiPolygon() {
        String multiPolygon = """
                MULTIPOLYGON (
                  ((51.380 35.690, 51.390 35.690, 51.390 35.700,
                    51.380 35.700, 51.380 35.690)),
                  ((52.000 36.000, 52.010 36.000, 52.010 36.010,
                    52.000 36.010, 52.000 36.000))
                )
                """;

        Set<String> result = GeohashCoverageGenerator.cover(multiPolygon, PRECISION);

        assertTrue(result.contains(geohash(35.695, 51.385)));
        assertTrue(result.contains(geohash(36.005, 52.005)));
    }

    @Test
    void rejectsNonPolygonString() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> GeohashCoverageGenerator.cover("POINT (51.385 35.695)", PRECISION)
        );

        assertTrue(exception.getMessage().contains("Polygon or MultiPolygon"));
    }

    @Test
    void rejectsMalformedGeometryString() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> GeohashCoverageGenerator.cover("not a geometry", PRECISION)
        );

        assertTrue(exception.getMessage().contains("valid WKT"));
    }

    @Test
    void coversPolygonAndReturnsImmutableResult() {
        Polygon polygon = rectangle(51.380, 35.690, 51.390, 35.700);

        Set<String> result = GeohashCoverageGenerator.cover(polygon, PRECISION);

        assertTrue(result.contains(geohash(35.695, 51.385)));
        assertThrows(UnsupportedOperationException.class, () -> result.add("test"));
    }

    @Test
    void coversEveryPartOfMultiPolygonWithoutDuplicates() {
        Polygon first = rectangle(51.380, 35.690, 51.390, 35.700);
        Polygon second = rectangle(52.000, 36.000, 52.010, 36.010);
        MultiPolygon multiPolygon = GEOMETRY_FACTORY.createMultiPolygon(
                new Polygon[]{first, second}
        );

        Set<String> result = GeohashCoverageGenerator.cover(multiPolygon, PRECISION);

        assertTrue(result.contains(geohash(35.695, 51.385)));
        assertTrue(result.contains(geohash(36.005, 52.005)));
    }

    @Test
    void excludesCellsEntirelyInsidePolygonHole() {
        LinearRing shell = ring(51.380, 35.690, 51.400, 35.710);
        LinearRing hole = ring(51.387, 35.697, 51.393, 35.703);
        Polygon polygon = GEOMETRY_FACTORY.createPolygon(shell, new LinearRing[]{hole});

        Set<String> result = GeohashCoverageGenerator.cover(polygon, PRECISION);

        assertFalse(result.contains(geohash(35.700, 51.390)));
        assertTrue(result.contains(geohash(35.695, 51.385)));
    }

    @Test
    void rejectsPrecisionOutsideGeohashRange() {
        Polygon polygon = rectangle(51.380, 35.690, 51.390, 35.700);

        assertThrows(IllegalArgumentException.class, () -> GeohashCoverageGenerator.cover(polygon, 0));
        assertThrows(IllegalArgumentException.class, () -> GeohashCoverageGenerator.cover(polygon, 13));
    }

    private static String geohash(double latitude, double longitude) {
        return GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, PRECISION);
    }

    private static Polygon rectangle(
            double minimumLongitude,
            double minimumLatitude,
            double maximumLongitude,
            double maximumLatitude
    ) {
        return GEOMETRY_FACTORY.createPolygon(
                ring(minimumLongitude, minimumLatitude, maximumLongitude, maximumLatitude)
        );
    }

    private static LinearRing ring(
            double minimumLongitude,
            double minimumLatitude,
            double maximumLongitude,
            double maximumLatitude
    ) {
        return GEOMETRY_FACTORY.createLinearRing(new Coordinate[]{
                new Coordinate(minimumLongitude, minimumLatitude),
                new Coordinate(maximumLongitude, minimumLatitude),
                new Coordinate(maximumLongitude, maximumLatitude),
                new Coordinate(minimumLongitude, maximumLatitude),
                new Coordinate(minimumLongitude, minimumLatitude)
        });
    }
}
