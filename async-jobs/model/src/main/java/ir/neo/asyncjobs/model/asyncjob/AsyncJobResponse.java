package ir.neo.asyncjobs.model.asyncjob;

import java.time.OffsetDateTime;

/** Represents an asynchronous job exposed through the REST API. */
public record AsyncJobResponse(
        Long id,
        String title,
        String jobStatus,
        String jobType,
        OffsetDateTime creationTime,
        OffsetDateTime startTime,
        OffsetDateTime endTime,
        boolean expired
) {
}
