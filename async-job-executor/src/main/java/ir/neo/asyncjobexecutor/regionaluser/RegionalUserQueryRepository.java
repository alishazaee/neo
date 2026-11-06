package ir.neo.asyncjobexecutor.regionaluser;

import static ir.neo.travelcube.db.Tables.TRAVEL_CUBE_;

import ir.neo.regionaluser.model.RegionalUserResult;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;

/** Aggregates user presence counts from the travel cube. */
public class RegionalUserQueryRepository {

    private final DSLContext dslContext;

    public RegionalUserQueryRepository(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public List<RegionalUserResult> findUsers(
            Set<String> regionGeohashes,
            LocalDateTime startTime,
            LocalDateTime endTime
    ) {
        if (regionGeohashes == null || regionGeohashes.isEmpty()) {
            return List.of();
        }

        Field<BigDecimal> presenceCount = DSL.sum(TRAVEL_CUBE_.NUMBER_OF_PRESENCE);
        return dslContext.select(TRAVEL_CUBE_.USERNAME, presenceCount)
                .from(TRAVEL_CUBE_)
                .where(TRAVEL_CUBE_.GEOHASH.in(regionGeohashes))
                .and(TRAVEL_CUBE_.TIME_BUCKET.ge(startTime))
                .and(TRAVEL_CUBE_.TIME_BUCKET.le(endTime))
                .groupBy(TRAVEL_CUBE_.USERNAME)
                .fetch(record -> new RegionalUserResult(
                        record.get(TRAVEL_CUBE_.USERNAME),
                        record.get(presenceCount).longValueExact()
                ));
    }
}
