package ir.neo.travelcube.model;

/** Type of a point represented in the travel cube. */
public enum PointType {
    START("Start"),
    END("End"),
    TRANSIT("Transit");

    private final String value;

    PointType(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }
}
