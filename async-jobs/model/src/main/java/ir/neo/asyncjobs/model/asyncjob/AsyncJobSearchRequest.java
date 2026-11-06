package ir.neo.asyncjobs.model.asyncjob;

/** Search filters for asynchronous jobs. */
public record AsyncJobSearchRequest(
        Long jobId,
        JobType jobType,
        JobStatus jobStatus,
        String jobTitle,
        Long creationTimeFrom,
        Long creationTimeTo
) {
}
