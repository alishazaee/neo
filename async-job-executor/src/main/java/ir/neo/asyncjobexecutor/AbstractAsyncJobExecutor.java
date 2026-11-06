package ir.neo.asyncjobexecutor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ir.neo.asyncjobexecutor.config.ApplicationConfig;
import ir.neo.asyncjobs.model.asyncjob.JobStatus;
import ir.neo.asyncjobs.repository.CommonAsyncJobRepository;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.jooq.exception.DataAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared lifecycle, retry, query, and result-writing behavior for async-job executors. */
public abstract class AbstractAsyncJobExecutor implements AsyncJobExecutor {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected final CommonAsyncJobRepository jobRepository;
    protected final ApplicationConfig configuration;

    protected final ExecutorService queryExecutor;
    protected final ExecutorService writerExecutor;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile boolean closed;

    protected AbstractAsyncJobExecutor(
            CommonAsyncJobRepository jobRepository,
            ApplicationConfig configuration
    ) {
        this.jobRepository = jobRepository;
        this.configuration = configuration;

        this.queryExecutor = Executors.newFixedThreadPool(
                configuration.getQueryThreadCount()
        );

        this.writerExecutor = Executors.newFixedThreadPool(
                configuration.getWriterThreadCount()
        );
    }

    protected final <R> R parseRequest(String requestParameters, Class<R> requestType) {
        try {
            return OBJECT_MAPPER.readValue(requestParameters, requestType);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException(
                    "Could not parse the async-job request",
                    exception
            );
        }
    }

    protected final LocalDateTime toUtcDateTime(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
    }

    protected final void validateTimeRange(Long startTime, Long endTime) {
        if (startTime == null || endTime == null) {
            throw new IllegalArgumentException("startTime and endTime are required");
        }
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime must not be later than endTime");
        }
    }

    protected final void finishJob(long jobId, JobStatistics jobStatistics) {
        try {
            jobRepository.updateMetadata(jobId, OBJECT_MAPPER.writeValueAsString(jobStatistics));
            jobRepository.updateJobStatus(jobId, JobStatus.FINISHED);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException(
                    "Could not serialize async-job statistics",
                    exception
            );
        }
    }

    protected final void markJobFailed(long jobId) {
        logger.error("Async job {} failed", jobId);
        jobRepository.updateJobStatus(jobId, JobStatus.FAILED);
    }

    protected final <T> CompletableFuture<T> queryAsync(Supplier<T> query, JobStatistics jobStatistics) {
        return CompletableFuture.supplyAsync(() -> {
            long startedAt = System.currentTimeMillis();

            try {
                return executeQueryWithRetry(query);
            } finally {
                jobStatistics.incrementQueryDuration(
                        System.currentTimeMillis() - startedAt
                );
            }
        }, queryExecutor);
    }

    protected final <T> CompletableFuture<Void> saveResultsAsync(long jobId, List<T> results,
            BiConsumer<Long, List<T>> resultWriter, JobStatistics jobStatistics) {
        return CompletableFuture.runAsync(() -> {
            long startedAt = System.currentTimeMillis();

            try {
                resultWriter.accept(jobId, results);
                jobStatistics.incrementTotalRows(results.size());
            } finally {
                jobStatistics.incrementWriteDuration(
                        System.currentTimeMillis() - startedAt
                );
            }
        }, writerExecutor);
    }

    private <T> T executeQueryWithRetry(Supplier<T> query) {
        for (int attempt = 1;
                attempt <= configuration.getMaxAttempts();
                attempt++) {

            try {
                return query.get();

            } catch (DataAccessException exception) {

                if (attempt == configuration.getMaxAttempts()) {
                    throw exception;
                }

                logger.warn(
                        "Async-job query attempt {} of {} failed; retrying",
                        attempt,
                        configuration.getMaxAttempts(),
                        exception
                );

                waitBeforeRetry();
            }
        }

        throw new IllegalStateException(
                "Async-job retry loop ended unexpectedly"
        );
    }

    private void waitBeforeRetry() {
        try {
            Thread.sleep(configuration.getRetryDelayMillis());

        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();

            CancellationException cancellation = new CancellationException(
                    "Async-job query was interrupted"
            );

            cancellation.initCause(exception);
            throw cancellation;
        }
    }

    protected final boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }

        closed = true;

        queryExecutor.shutdown();
        writerExecutor.shutdown();
    }
}
