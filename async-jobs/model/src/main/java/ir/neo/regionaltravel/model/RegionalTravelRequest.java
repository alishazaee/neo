package ir.neo.regionaltravel.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Input parameters for finding travel between two WKT regions in a time range. */
public record RegionalTravelRequest(
        @NotNull
        Long startTime,
        @NotNull
        Long endTime,
        @NotBlank
        String sourceRegionWkt,
        @NotBlank
        String destinationRegionWkt
) {
}
