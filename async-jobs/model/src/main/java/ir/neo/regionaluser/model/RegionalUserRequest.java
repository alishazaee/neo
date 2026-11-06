package ir.neo.regionaluser.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/** Input parameters for finding users present in a WKT region during a time range. */
public record RegionalUserRequest(
        @NotNull
        Long startTime,
        @NotNull
        Long endTime,
        @NotBlank
        String regionWkt
) {
}
