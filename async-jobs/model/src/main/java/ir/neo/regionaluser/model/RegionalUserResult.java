package ir.neo.regionaluser.model;

/** Aggregated presence count for a user inside a region. */
public record RegionalUserResult(
        String userId,
        long presenceCount
) {
}
