package ir.neo.asyncjobs.rest;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobRequest;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobResponse;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobSearchRequest;
import ir.neo.asyncjobs.model.asyncjob.AsyncJobTitleRequest;
import ir.neo.common.pagination.Page;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Positive;
import java.net.URI;
import org.springdoc.core.annotations.ParameterObject;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/** Exposes asynchronous-job operations over HTTP. */
@Tag(name = "Async Jobs", description = "Submit, search, retrieve, and update asynchronous jobs")
@Validated
@RestController
@RequestMapping("/api/v1/async-jobs")
public class AsyncJobController {
    private static final int MAX_PAGE_SIZE = 100;

    private final AsyncJobService asyncJobService;

    public AsyncJobController(AsyncJobService asyncJobService) {
        this.asyncJobService = asyncJobService;
    }

    @Operation(
            summary = "Submit an asynchronous job",
            description = "Creates a submitted job when the configured open-job limit has not been reached."
    )
    @ApiResponse(
            responseCode = "201",
            description = "Job created",
            content = @Content(schema = @Schema(implementation = Long.class))
    )
    @ApiResponse(responseCode = "400", description = "Invalid request body", content = @Content)
    @ApiResponse(responseCode = "409", description = "Open-job limit reached", content = @Content)
    @ApiResponse(responseCode = "500", description = "Job could not be created", content = @Content)
    @PostMapping
    public ResponseEntity<Long> submit(
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "Job submission request",
                    required = true,
                    content = @Content(schema = @Schema(implementation = AsyncJobRequest.class))
            )
            @Valid @RequestBody AsyncJobRequest request
    ) {
        Long jobId = asyncJobService.submit(request);
        URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{jobId}")
                .buildAndExpand(jobId)
                .toUri();

        return ResponseEntity.created(location).body(jobId);
    }

    @Operation(
            summary = "Search asynchronous jobs",
            description = "Returns a page of jobs matching the supplied optional query filters."
    )
    @ApiResponse(responseCode = "200", description = "Jobs returned")
    @ApiResponse(responseCode = "400", description = "Invalid filter or pagination value", content = @Content)
    @GetMapping
    public Page<AsyncJobResponse> search(
            @ParameterObject @ModelAttribute AsyncJobSearchRequest searchRequest,
            @Parameter(
                    name = "size",
                    description = "Number of jobs per page, from 1 to 100",
                    in = ParameterIn.QUERY,
                    example = "20"
            )
            @RequestParam(defaultValue = "20") @Min(1) @Max(MAX_PAGE_SIZE) int size,
            @Parameter(
                    name = "page",
                    description = "Zero-based page number",
                    in = ParameterIn.QUERY,
                    example = "0"
            )
            @RequestParam(defaultValue = "0") @Min(0) int page
    ) {
        return asyncJobService.search(searchRequest, size, page);
    }

    @Operation(summary = "Retrieve an asynchronous job", description = "Returns a job by its identifier.")
    @ApiResponse(
            responseCode = "200",
            description = "Job returned",
            content = @Content(schema = @Schema(implementation = AsyncJobResponse.class))
    )
    @ApiResponse(responseCode = "400", description = "Invalid job identifier", content = @Content)
    @ApiResponse(responseCode = "404", description = "No job found with the given identifier", content = @Content)
    @GetMapping("/{jobId}")
    public AsyncJobResponse retrieve(
            @Parameter(
                    name = "jobId",
                    description = "Identifier of the job",
                    in = ParameterIn.PATH,
                    required = true,
                    example = "1"
            )
            @PathVariable @Positive long jobId
    ) {
        return asyncJobService.retrieve(jobId);
    }

    @Operation(summary = "Update an asynchronous job title", description = "Updates the title of an existing job.")
    @ApiResponse(responseCode = "204", description = "Job title updated")
    @ApiResponse(responseCode = "400", description = "Invalid job identifier or request body", content = @Content)
    @ApiResponse(responseCode = "404", description = "No job found with the given identifier", content = @Content)
    @PatchMapping("/{jobId}/title")
    public ResponseEntity<Void> updateTitle(
            @Parameter(
                    name = "jobId",
                    description = "Identifier of the job",
                    in = ParameterIn.PATH,
                    required = true,
                    example = "1"
            )
            @PathVariable @Positive long jobId,
            @io.swagger.v3.oas.annotations.parameters.RequestBody(
                    description = "New title for the job",
                    required = true,
                    content = @Content(schema = @Schema(implementation = AsyncJobTitleRequest.class))
            )
            @Valid @RequestBody AsyncJobTitleRequest request
    ) {
        asyncJobService.updateTitle(jobId, request.title());
        return ResponseEntity.noContent().build();
    }
}
