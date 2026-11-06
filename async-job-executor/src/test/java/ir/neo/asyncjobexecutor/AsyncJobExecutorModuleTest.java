package ir.neo.asyncjobexecutor;

import static ir.neo.asyncjobs.db.Tables.ASYNC_JOBS;
import static ir.neo.asyncjobs.db.Tables.REGIONAL_TRAVEL_RESULTS;
import static ir.neo.travelcube.db.Tables.TRAVEL_CUBE_;
import static org.junit.jupiter.api.Assertions.assertEquals;

import ir.neo.asyncjobs.model.asyncjob.JobStatus;
import ir.neo.travelcube.model.PointType;
import org.junit.jupiter.api.Test;

class AsyncJobExecutorModuleTest {

    @Test
    void providesModelAndDatabaseDependencies() {
        assertEquals("SUBMITTED", JobStatus.SUBMITTED.name());
        assertEquals("async_jobs", ASYNC_JOBS.getName());
        assertEquals("regional_travel_results", REGIONAL_TRAVEL_RESULTS.getName());
        assertEquals("travel_cube", TRAVEL_CUBE_.getName());
        assertEquals("Transit", PointType.TRANSIT.value());
    }
}
