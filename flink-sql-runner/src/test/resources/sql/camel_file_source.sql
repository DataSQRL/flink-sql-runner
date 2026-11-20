CREATE TABLE people (
  id STRING,
  name STRING,
  file_name STRING METADATA FROM 'file'
) WITH (
  'connector' = 'camel',
  'source.class' = 'sample.camel.CsvDrop',
  'scan.bounded' = 'true',
  'file.ready' = 'none'
);

CREATE TABLE printed (
  id STRING,
  name STRING,
  file_name STRING
) WITH (
  'connector' = 'print'
);

INSERT INTO printed SELECT id, name, file_name FROM people;
