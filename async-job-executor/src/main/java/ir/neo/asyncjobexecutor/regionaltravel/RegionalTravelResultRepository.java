package ir.neo.asyncjobexecutor.regionaltravel;

import static ir.neo.asyncjobs.db.Tables.REGIONAL_TRAVEL_RESULTS;

import ir.neo.regionaltravel.model.RegionalTravelResult;
import java.util.List;
import org.jooq.DSLContext;

/** Persists regional-travel results to PostgreSQL. */
public class RegionalTravelResultRepository {

    private final DSLContext dslContext;

    public RegionalTravelResultRepository(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public void save(long jobId, List<RegionalTravelResult> results) {
        if (results.isEmpty()) {
            return;
        }
        var insert = dslContext.insertInto(
                REGIONAL_TRAVEL_RESULTS,
                REGIONAL_TRAVEL_RESULTS.JOB_ID,
                REGIONAL_TRAVEL_RESULTS.USERNAME,
                REGIONAL_TRAVEL_RESULTS.SOURCE_REGION_GEOHASH,
                REGIONAL_TRAVEL_RESULTS.DESTINATION_REGION_GEOHASH,
                REGIONAL_TRAVEL_RESULTS.DEPARTURE_TIME,
                REGIONAL_TRAVEL_RESULTS.ARRIVAL_TIME
        );
        for (RegionalTravelResult result : results) {
            insert.values(
                    jobId,
                    result.username(),
                    result.sourceRegionGeohash(),
                    result.destinationRegionGeohash(),
                    result.departureTime(),
                    result.arrivalTime()
            );
        }
        insert.execute();
    }
}
