package ir.neo.asyncjobexecutor.config;

import ir.neo.asyncjobs.model.asyncjob.JobType;

/** Runtime configuration for an async-job executor process. */
public class ApplicationConfig {

    private JobType jobType;
    private int maxAttempts;
    private int geohashesPerBatch;
    private int queryThreadCount;
    private int writerThreadCount;
    private long retryDelayMillis;
    private long pollIntervalMillis;
    private DatabaseConfig postgres;
    private DatabaseConfig starRocks;

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public int getGeohashesPerBatch() {
        return geohashesPerBatch;
    }

    public void setGeohashesPerBatch(int geohashesPerBatch) {
        this.geohashesPerBatch = geohashesPerBatch;
    }

    public int getQueryThreadCount() {
        return queryThreadCount;
    }

    public void setQueryThreadCount(int queryThreadCount) {
        this.queryThreadCount = queryThreadCount;
    }

    public int getWriterThreadCount() {
        return writerThreadCount;
    }

    public void setWriterThreadCount(int writerThreadCount) {
        this.writerThreadCount = writerThreadCount;
    }

    public long getRetryDelayMillis() {
        return retryDelayMillis;
    }

    public void setRetryDelayMillis(long retryDelayMillis) {
        this.retryDelayMillis = retryDelayMillis;
    }

    public long getPollIntervalMillis() {
        return pollIntervalMillis;
    }

    public void setPollIntervalMillis(long pollIntervalMillis) {
        this.pollIntervalMillis = pollIntervalMillis;
    }

    public DatabaseConfig getPostgres() {
        return postgres;
    }

    public void setPostgres(DatabaseConfig postgres) {
        this.postgres = postgres;
    }

    public DatabaseConfig getStarRocks() {
        return starRocks;
    }

    public void setStarRocks(DatabaseConfig starRocks) {
        this.starRocks = starRocks;
    }

    /** Ensures all executor limits can be used safely. */
    public void validate() {
        if (jobType == null) {
            throw new IllegalArgumentException("jobType configuration is required");
        }
        requirePositive(maxAttempts, "maxAttempts");
        requirePositive(geohashesPerBatch, "geohashesPerBatch");
        requirePositive(queryThreadCount, "queryThreadCount");
        requirePositive(writerThreadCount, "writerThreadCount");
        if (retryDelayMillis < 0) {
            throw new IllegalArgumentException("retryDelayMillis must not be negative");
        }
        if (pollIntervalMillis < 0) {
            throw new IllegalArgumentException("pollIntervalMillis must not be negative");
        }
        requireDatabase(postgres, "postgres");
        requireDatabase(starRocks, "starRocks");
    }

    private static void requirePositive(int value, String propertyName) {
        if (value <= 0) {
            throw new IllegalArgumentException(propertyName + " must be positive");
        }
    }

    private static void requireDatabase(DatabaseConfig database, String propertyName) {
        if (database == null) {
            throw new IllegalArgumentException(propertyName + " configuration is required");
        }
        database.validate(propertyName);
    }
}
