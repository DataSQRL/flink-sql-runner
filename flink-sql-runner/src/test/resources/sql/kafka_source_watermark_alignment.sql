-- Two sources in one watermark alignment group. The fast topic's records are seconds ahead of the
-- slow topic's, more than the maximum drift, so Flink holds the fast source back until the slow
-- one has read its backlog and idle advancement moves its watermark on.
CREATE TABLE alignment_slow_events (
  id BIGINT,
  payload STRING,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'source-watermark-alignment-slow-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'source-watermark-alignment-slow-it',
  'scan.startup.mode' = 'earliest-offset',
  'scan.rate-limit.records-per-second' = '2',
  'scan.source-watermark.min-out-of-orderness' = '50 ms',
  'scan.source-watermark.idle-advance-timeout' = '500 ms',
  'scan.source-watermark.idle-advance-safety-margin' = '1 s',
  'scan.source-watermark.idle-advance-broker-check-timeout' = '1 s',
  'scan.source-watermark.idle-advance-broker-check-ttl' = '1 s',
  'scan.watermark.alignment.group' = 'source-watermark-alignment-it',
  'scan.watermark.alignment.max-drift' = '1 s',
  'scan.watermark.alignment.update-interval' = '100 ms',
  'format' = 'json'
);

CREATE TABLE alignment_fast_events (
  id BIGINT,
  payload STRING,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'source-watermark-alignment-fast-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'source-watermark-alignment-fast-it',
  'scan.startup.mode' = 'earliest-offset',
  'scan.rate-limit.records-per-second' = '1',
  'scan.source-watermark.min-out-of-orderness' = '50 ms',
  'scan.source-watermark.idle-advance-timeout' = '500 ms',
  'scan.source-watermark.idle-advance-safety-margin' = '1 s',
  'scan.source-watermark.idle-advance-broker-check-timeout' = '1 s',
  'scan.source-watermark.idle-advance-broker-check-ttl' = '1 s',
  'scan.watermark.alignment.group' = 'source-watermark-alignment-it',
  'scan.watermark.alignment.max-drift' = '1 s',
  'scan.watermark.alignment.update-interval' = '100 ms',
  'format' = 'json'
);

CREATE TABLE source_watermark_alignment_results (
  source STRING NOT NULL,
  id BIGINT NOT NULL
) WITH (
  'connector' = 'jdbc',
  'driver' = 'org.postgresql.Driver',
  'url' = 'jdbc:postgresql://${POSTGRES_AUTHORITY}',
  'username' = '${POSTGRES_USERNAME}',
  'password' = '${POSTGRES_PASSWORD}',
  'table-name' = 'source_watermark_alignment_results'
);

INSERT INTO source_watermark_alignment_results
SELECT CAST('slow' AS STRING), id FROM alignment_slow_events
UNION ALL
SELECT CAST('fast' AS STRING), id FROM alignment_fast_events;
