package ir.neo.regionaltravel.model;

import java.time.LocalDateTime;

/** A journey between two geographic regions by the same user. */
public record RegionalTravelResult(
        String username,
        String sourceRegionGeohash,
        String destinationRegionGeohash,
        LocalDateTime departureTime,
        LocalDateTime arrivalTime
) {
}
