CREATE TABLE source_events (
  id BIGINT,
  payload STRING,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR ts AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'source-watermark-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'source-watermark-it',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE source_watermark_results (
  window_start TIMESTAMP(3) NOT NULL,
  window_end TIMESTAMP(3) NOT NULL,
  cnt BIGINT NOT NULL
) WITH (
  'connector' = 'jdbc',
  'driver' = 'org.postgresql.Driver',
  'url' = '${JDBC_URL}',
  'username' = '${JDBC_USERNAME}',
  'password' = '${JDBC_PASSWORD}',
  'table-name' = 'source_watermark_results'
);

INSERT INTO source_watermark_results
SELECT
  CAST(window_start AS TIMESTAMP(3)),
  CAST(window_end AS TIMESTAMP(3)),
  COUNT(*)
FROM TABLE(
  TUMBLE(TABLE source_events, DESCRIPTOR(ts), INTERVAL '1' SECOND)
)
GROUP BY window_start, window_end;
