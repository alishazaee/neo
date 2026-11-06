package ir.neo.asyncjobs.repository;

import static ir.neo.asyncjobs.db.Tables.ASYNC_JOBS;
import static ir.neo.asyncjobs.model.asyncjob.JobStatus.SUBMITTED;

import ir.neo.asyncjobs.db.tables.records.AsyncJobsRecord;
import ir.neo.asyncjobs.model.asyncjob.JobStatus;
import ir.neo.asyncjobs.model.asyncjob.JobType;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

/** Provides the async-job lifecycle operations shared by job executors. */
public class CommonAsyncJobRepository {

    private final DSLContext dslContext;

    public CommonAsyncJobRepository(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public AsyncJobsRecord claimOldestSubmittedJob(JobType jobType) {
        return dslContext.transactionResult(configuration -> {
            DSLContext transaction = DSL.using(configuration);
            AsyncJobsRecord submittedJob = transaction.selectFrom(ASYNC_JOBS)
                    .where(ASYNC_JOBS.JOB_TYPE.eq(jobType.name()))
                    .and(ASYNC_JOBS.JOB_STATUS.eq(SUBMITTED.name()))
                    .orderBy(ASYNC_JOBS.CREATION_TIME.asc())
                    .limit(1)
                    .forUpdate()
                    .skipLocked()
                    .fetchOne();
            if (submittedJob == null) {
                return null;
            }

            transaction.update(ASYNC_JOBS)
                    .set(ASYNC_JOBS.JOB_STATUS, JobStatus.RUNNING.name())
                    .set(ASYNC_JOBS.START_TIME, DSL.currentOffsetDateTime())
                    .where(ASYNC_JOBS.ID.eq(submittedJob.getId()))
                    .execute();
            submittedJob.setJobStatus(JobStatus.RUNNING.name());
            return submittedJob;
        });
    }

    public void updateJobStatus(long jobId, JobStatus newStatus) {
        if (newStatus == JobStatus.FINISHED || newStatus == JobStatus.FAILED) {
            dslContext.update(ASYNC_JOBS)
                    .set(ASYNC_JOBS.JOB_STATUS, newStatus.name())
                    .set(ASYNC_JOBS.END_TIME, DSL.currentOffsetDateTime())
                    .where(ASYNC_JOBS.ID.eq(jobId))
                    .execute();
            return;
        }
        dslContext.update(ASYNC_JOBS)
                .set(ASYNC_JOBS.JOB_STATUS, newStatus.name())
                .where(ASYNC_JOBS.ID.eq(jobId))
                .execute();
    }

    public void updateMetadata(long jobId, String metadata) {
        dslContext.update(ASYNC_JOBS)
                .set(ASYNC_JOBS.METADATA, metadata)
                .where(ASYNC_JOBS.ID.eq(jobId))
                .execute();
    }
}
