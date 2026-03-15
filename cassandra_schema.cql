CREATE KEYSPACE IF NOT EXISTS pipeline
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE pipeline;

CREATE TABLE IF NOT EXISTS pageviews (
    user_id text,
    event_time timestamp,
    event_id text,
    page text,
    PRIMARY KEY (user_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
