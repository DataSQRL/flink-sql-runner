-- Commits a Kafka transaction at every checkpoint, so the records span several transactions and
-- the partition holds a commit marker after each.
SET 'execution.checkpointing.interval' = '1 s';

CREATE TABLE transactional_records (
  record_id BIGINT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '20',
  'rows-per-second' = '5',
  'fields.record_id.kind' = 'sequence',
  'fields.record_id.start' = '0',
  'fields.record_id.end' = '19'
);

CREATE TABLE transactional_output (
  id BIGINT,
  payload STRING
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'source-watermark-idle-advance-transactional-it',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.transaction.timeout.ms' = '60000',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'source-watermark-idle-advance-transactional-it',
  'format' = 'json'
);

INSERT INTO transactional_output
SELECT
  record_id,
  CONCAT('record-', CAST(record_id AS STRING))
FROM transactional_records;
