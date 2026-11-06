package ir.neo.asyncjobexecutor.regionaluser;

import static ir.neo.asyncjobs.db.Tables.REGIONAL_USER;

import ir.neo.regionaluser.model.RegionalUserResult;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

/** Persists regional-user results to PostgreSQL. */
public class RegionalUserResultRepository {

    private final DSLContext dslContext;

    public RegionalUserResultRepository(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public void save(long jobId, List<RegionalUserResult> results) {
        if (results.isEmpty()) {
            return;
        }
        var insert = dslContext.insertInto(
                REGIONAL_USER,
                REGIONAL_USER.JOB_ID,
                REGIONAL_USER.USER_ID,
                REGIONAL_USER.PRESENSE_COUNT
        );
        for (RegionalUserResult result : results) {
            insert.values(jobId, result.userId(), result.presenceCount())
                    .onConflict(REGIONAL_USER.JOB_ID, REGIONAL_USER.USER_ID)
                    .doUpdate()
                    .set(REGIONAL_USER.PRESENSE_COUNT, REGIONAL_USER.PRESENSE_COUNT.plus(
                                    DSL.excluded(REGIONAL_USER.PRESENSE_COUNT)));
        }
        insert.execute();
    }
}
