CREATE TABLE source_watermark_records (
  record_id BIGINT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '300',
  'rows-per-second' = '1000',
  'fields.record_id.kind' = 'sequence',
  'fields.record_id.start' = '0',
  'fields.record_id.end' = '299'
);

CREATE TABLE source_watermark_input (
  id BIGINT,
  payload STRING,
  ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'source-watermark-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json'
);

INSERT INTO source_watermark_input
SELECT
  CAST(record_id AS BIGINT),
  CONCAT('record-', CAST(record_id AS STRING)),
  TO_TIMESTAMP_LTZ(1780272000000 + record_id * 100, 3) -- 2026-06-01 00:00:00
FROM source_watermark_records;
