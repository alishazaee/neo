package ir.neo.asyncjobexecutor.regionaltravel;

import static ir.neo.travelcube.db.Tables.TRAVEL_CUBE_;

import ir.neo.regionaltravel.model.RegionalTravelResult;
import ir.neo.travelcube.db.tables.TravelCube;
import ir.neo.travelcube.model.PointType;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import org.jooq.DSLContext;

/** Queries journeys between source and destination regions in the travel cube. */
public class RegionalTravelQueryRepository {

    private final DSLContext dslContext;

    public RegionalTravelQueryRepository(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public List<RegionalTravelResult> findTravels(
            Set<String> sourceRegionGeohashes,
            Set<String> destinationRegionGeohashes,
            LocalDateTime startTime,
            LocalDateTime endTime
    ) {
        if (sourceRegionGeohashes == null || sourceRegionGeohashes.isEmpty()
                || destinationRegionGeohashes == null || destinationRegionGeohashes.isEmpty()) {
            return List.of();
        }

        TravelCube startPresence = TRAVEL_CUBE_.as("start_presence");
        TravelCube endPresence = TRAVEL_CUBE_.as("end_presence");

        return dslContext.select(
                        startPresence.USERNAME,
                        startPresence.GEOHASH,
                        endPresence.GEOHASH,
                        startPresence.TIME_BUCKET,
                        endPresence.TIME_BUCKET
                )
                .from(startPresence)
                .join(endPresence)
                .on(startPresence.USERNAME.eq(endPresence.USERNAME))
                .and(startPresence.TIME_BUCKET.le(endPresence.TIME_BUCKET))
                .and(startPresence.GEOHASH.ne(endPresence.GEOHASH))
                .where(startPresence.GEOHASH.in(sourceRegionGeohashes))
                .and(endPresence.GEOHASH.in(destinationRegionGeohashes))
                .and(startPresence.TIME_BUCKET.ge(startTime))
                .and(endPresence.TIME_BUCKET.le(endTime))
                .and(startPresence.POINT_TYPE.eq(PointType.START.value()))
                .and(endPresence.POINT_TYPE.eq(PointType.END.value()))
                .fetch(databaseRecord -> new RegionalTravelResult(
                        databaseRecord.get(startPresence.USERNAME),
                        databaseRecord.get(startPresence.GEOHASH),
                        databaseRecord.get(endPresence.GEOHASH),
                        databaseRecord.get(startPresence.TIME_BUCKET),
                        databaseRecord.get(endPresence.TIME_BUCKET)
                ));
    }
}
