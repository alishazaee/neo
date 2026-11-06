package ir.neo.asyncjobs.model.asyncjob;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Represents an asynchronous job request. */
public record AsyncJobRequest(
        @NotBlank(message = "Request parameter is required")
        String requestParameter,
        @NotNull(message = "Job type is required")
        JobType jobType
) {
}
