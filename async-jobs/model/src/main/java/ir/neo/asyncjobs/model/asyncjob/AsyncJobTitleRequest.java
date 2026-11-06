package ir.neo.asyncjobs.model.asyncjob;

import jakarta.validation.constraints.NotBlank;

/** Request to update an asynchronous job title. */
public record AsyncJobTitleRequest(
        @NotBlank(message = "Job title is required")
        String title
) {
}
