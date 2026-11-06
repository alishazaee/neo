CREATE TABLE travel_cube (
    time_bucket DATETIME NOT NULL COMMENT 'Start of the aggregated time bucket',
    geohash VARCHAR(12) NOT NULL COMMENT 'Geographic hash of the aggregated location',
    username VARCHAR(255) NOT NULL COMMENT 'Username associated with the travel point',
    point_type VARCHAR(16) NOT NULL COMMENT 'Point type: Start, End, or Transit',
    number_of_presence BIGINT NOT NULL DEFAULT '0' COMMENT 'Number of presences represented by this row',
    INDEX idx_travel_cube_geohash (geohash) USING BITMAP COMMENT 'Lookup travel presence by geohash',
    INDEX idx_travel_cube_username (username) USING BITMAP COMMENT 'Lookup travel presence by username'
)
ENGINE=OLAP
PARTITION BY date_trunc('day', time_bucket)
DISTRIBUTED BY HASH(geohash)
ORDER BY (geohash)
PROPERTIES (
    'replication_num' = '1'
);
