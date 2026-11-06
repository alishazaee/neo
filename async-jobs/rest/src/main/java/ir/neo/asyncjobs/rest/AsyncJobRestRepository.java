package ir.neo.asyncjobs.rest;

import static ir.neo.asyncjobs.db.Tables.ASYNC_JOBS;
import static ir.neo.asyncjobs.model.asyncjob.JobStatus.SUBMITTED;
import static org.jooq.impl.DSL.noCondition;

import ir.neo.asyncjobs.db.tables.records.AsyncJobsRecord;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobRequest;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobResponse;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobSearchRequest;
import ir.neo.asyncjobs.model.asyncjob.JobType;
import ir.neo.common.pagination.Page;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

/** Provides async-job persistence operations used only by the REST API. */
@Repository
public class AsyncJobRestRepository {

    private final DSLContext dslContext;

    public AsyncJobRestRepository(DSLContext dslContext) {
        this.dslContext = dslContext;
    }

    public Long create(AsyncJobRequest request) {
        AsyncJobsRecord insertedJob = dslContext.insertInto(ASYNC_JOBS)
                .set(ASYNC_JOBS.JOB_TYPE, request.jobType().name())
                .set(ASYNC_JOBS.EXPIRED, false)
                .set(ASYNC_JOBS.REQUEST_PARAMETERS, request.requestParameter())
                .set(ASYNC_JOBS.JOB_STATUS, SUBMITTED.name())
                .returning()
                .fetchOne();
        if (insertedJob == null) {
            throw new IllegalStateException("The async job could not be created");
        }
        return insertedJob.getId();
    }

    public Page<AsyncJobResponse> search(AsyncJobSearchRequest request, int pageSize, int pageNumber) {
        int page = Page.getPage(pageNumber);
        int size = Page.getSize(pageSize);
        Condition condition = createSearchCondition(request);

        List<AsyncJobResponse> jobs = dslContext.selectFrom(ASYNC_JOBS)
                .where(condition)
                .orderBy(ASYNC_JOBS.CREATION_TIME.desc())
                .limit(size)
                .offset(page * size)
                .fetch(this::toResponse);
        int totalElements = dslContext.fetchCount(ASYNC_JOBS, condition);

        return Page.of(jobs, page, size, totalElements);
    }

    public AsyncJobResponse findById(long jobId) {
        return dslContext.selectFrom(ASYNC_JOBS)
                .where(ASYNC_JOBS.ID.eq(jobId))
                .fetchOne(this::toResponse);
    }

    public long countOpenJobs(JobType jobType) {
        return dslContext.selectCount()
                .from(ASYNC_JOBS)
                .where(
                        ASYNC_JOBS.JOB_TYPE.eq(jobType.name()),
                        ASYNC_JOBS.EXPIRED.isFalse()
                )
                .fetchSingle(0, Long.class);
    }

    public int updateTitle(long jobId, String jobTitle) {
        return dslContext.update(ASYNC_JOBS)
                .set(ASYNC_JOBS.TITLE, jobTitle)
                .where(ASYNC_JOBS.ID.eq(jobId))
                .execute();
    }

    private static Condition createSearchCondition(AsyncJobSearchRequest request) {
        Condition condition = noCondition();
        if (request == null) {
            return condition;
        }
        if (request.jobId() != null) {
            condition = condition.and(ASYNC_JOBS.ID.eq(request.jobId()));
        }
        if (request.jobTitle() != null) {
            condition = condition.and(ASYNC_JOBS.TITLE.eq(request.jobTitle()));
        }
        if (request.jobType() != null) {
            condition = condition.and(ASYNC_JOBS.JOB_TYPE.eq(request.jobType().name()));
        }
        if (request.jobStatus() != null) {
            condition = condition.and(ASYNC_JOBS.JOB_STATUS.eq(request.jobStatus().name()));
        }
        if (request.creationTimeFrom() != null) {
            condition = condition.and(ASYNC_JOBS.CREATION_TIME.ge(
                    Instant.ofEpochMilli(request.creationTimeFrom()).atOffset(ZoneOffset.UTC)
            ));
        }
        if (request.creationTimeTo() != null) {
            condition = condition.and(ASYNC_JOBS.CREATION_TIME.le(
                    Instant.ofEpochMilli(request.creationTimeTo()).atOffset(ZoneOffset.UTC)
            ));
        }
        return condition;
    }

    private AsyncJobResponse toResponse(AsyncJobsRecord record) {
        return new AsyncJobResponse(
                record.getId(),
                record.getTitle(),
                record.getJobStatus(),
                record.getJobType(),
                record.getCreationTime(),
                record.getStartTime(),
                record.getEndTime(),
                record.getExpired()
        );
    }
}
