-- Updated Flink SQL using datagen connector and BIGINT for timestamps, with full pipeline and view updates

CREATE TEMPORARY TABLE `Transaction` (
  `transactionId` BIGINT NOT NULL,
  `cardNo` DOUBLE NOT NULL,
  `time` TIMESTAMP_LTZ(3) NOT NULL,
  `amount` DOUBLE NOT NULL,
  `merchantId` BIGINT NOT NULL,
  PRIMARY KEY (`transactionId`, `time`) NOT ENFORCED,
  WATERMARK FOR `time` AS `time` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '10',
  'fields.transactionId.kind' = 'sequence',
  'fields.transactionId.start' = '1',
  'fields.transactionId.end' = '1000000',
  'fields.cardNo.min' = '1000',
  'fields.cardNo.max' = '9999',
  'fields.amount.min' = '1',
  'fields.amount.max' = '5000',
  'fields.merchantId.min' = '1',
  'fields.merchantId.max' = '100'
);

CREATE TEMPORARY TABLE `CardAssignment` (
  `customerId` BIGINT NOT NULL,
  `cardNo` DOUBLE NOT NULL,
  `timestamp` TIMESTAMP_LTZ(3) NOT NULL,
  `cardType` VARCHAR(100) NOT NULL,
  PRIMARY KEY (`customerId`, `cardNo`, `timestamp`) NOT ENFORCED,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.customerId.kind' = 'sequence',
  'fields.customerId.start' = '1',
  'fields.customerId.end' = '10000',
  'fields.cardNo.min' = '1000',
  'fields.cardNo.max' = '9999',
  'fields.cardType.length' = '10'
);

CREATE TEMPORARY TABLE `Merchant` (
  `merchantId` BIGINT NOT NULL,
  `name` VARCHAR(100) NOT NULL,
  `category` VARCHAR(50) NOT NULL,
  `updatedTime` TIMESTAMP_LTZ(3) NOT NULL,
  PRIMARY KEY (`merchantId`, `updatedTime`) NOT ENFORCED,
  WATERMARK FOR `updatedTime` AS `updatedTime` - INTERVAL '1' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '2',
  'fields.merchantId.kind' = 'sequence',
  'fields.merchantId.start' = '1',
  'fields.merchantId.end' = '100',
  'fields.name.length' = '20',
  'fields.category.length' = '10'
);

CREATE VIEW `_Merchant`
AS
SELECT `merchantId`, `name`, `category`, `updatedTime`
FROM (SELECT `merchantId`, `name`, `category`, `updatedTime`, ROW_NUMBER() OVER (PARTITION BY `merchantId` ORDER BY `updatedTime` DESC NULLS LAST) AS `__sqrlinternal_rownum`
  FROM `default_catalog`.`default_database`.`Merchant`) AS `t`
WHERE `__sqrlinternal_rownum` = 1;
CREATE VIEW `_CardAssignment`
AS
SELECT `customerId`, `cardNo`, `timestamp`, `cardType`
FROM (SELECT `customerId`, `cardNo`, `timestamp`, `cardType`, ROW_NUMBER() OVER (PARTITION BY `cardNo` ORDER BY `timestamp` DESC NULLS LAST) AS `__sqrlinternal_rownum`
  FROM `default_catalog`.`default_database`.`CardAssignment`) AS `t`
WHERE `__sqrlinternal_rownum` = 1;
CREATE VIEW `CustomerTransaction`
AS
SELECT `t`.`transactionId`, `t`.`cardNo`, `t`.`time`, `t`.`amount`, `m`.`name` AS `merchantName`, `m`.`category`, `c`.`customerId`
FROM `default_catalog`.`default_database`.`Transaction` AS `t`
 INNER JOIN `_CardAssignment` FOR SYSTEM_TIME AS OF `t`.`time` AS `c` ON `t`.`cardNo` = `c`.`cardNo`
 INNER JOIN `_Merchant` FOR SYSTEM_TIME AS OF `t`.`time` AS `m` ON `t`.`merchantId` = `m`.`merchantId`;
CREATE VIEW `SpendingByCategory`
AS
SELECT `customerId`, `window_time` AS `timeWeek`, `category`, SUM(`amount`) AS `spending`
FROM TABLE(TUMBLE(TABLE `CustomerTransaction`, DESCRIPTOR(`time`), INTERVAL '7' DAY))
GROUP BY `customerId`, `window_start`, `window_end`, `window_time`, `category`;
CREATE VIEW `SpendingByDay`
AS
SELECT `customerId`, `window_time` AS `timeDay`, SUM(`amount`) AS `spending`
FROM TABLE(TUMBLE(TABLE `CustomerTransaction`, DESCRIPTOR(`time`), INTERVAL '1' DAY))
GROUP BY `customerId`, `window_start`, `window_end`, `window_time`;
CREATE TABLE `CustomerTransaction_1` (
  `transactionId` BIGINT NOT NULL,
  `cardNo` DOUBLE NOT NULL,
  `time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `amount` DOUBLE NOT NULL,
  `merchantName` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `category` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `customerId` BIGINT NOT NULL,
  PRIMARY KEY (`transactionId`, `time`) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc-sqrl',
  'driver' = 'org.postgresql.Driver',
  'password' = '${POSTGRES_PASSWORD}',
  'sink.on-conflict.action' = 'IGNORE',
  'table-name' = 'CustomerTransaction',
  'url' = 'jdbc:postgresql://${POSTGRES_AUTHORITY}',
  'username' = '${POSTGRES_USERNAME}'
);
CREATE TABLE `SpendingByCategory_2` (
  `customerId` BIGINT NOT NULL,
  `timeWeek` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `category` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `spending` DOUBLE NOT NULL,
  `__pk_hash` CHAR(32) CHARACTER SET `UTF-16LE`,
  PRIMARY KEY (`__pk_hash`) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc-sqrl',
  'driver' = 'org.postgresql.Driver',
  'password' = '${POSTGRES_PASSWORD}',
  'sink.on-conflict.action' = 'IGNORE',
  'table-name' = 'SpendingByCategory',
  'url' = 'jdbc:postgresql://${POSTGRES_AUTHORITY}',
  'username' = '${POSTGRES_USERNAME}'
);
CREATE TABLE `SpendingByDay_3` (
  `customerId` BIGINT NOT NULL,
  `timeDay` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `spending` DOUBLE NOT NULL,
  `__pk_hash` CHAR(32) CHARACTER SET `UTF-16LE`,
  PRIMARY KEY (`__pk_hash`) NOT ENFORCED
)
WITH (
  'connector' = 'jdbc-sqrl',
  'driver' = 'org.postgresql.Driver',
  'password' = '${POSTGRES_PASSWORD}',
  'sink.on-conflict.action' = 'IGNORE',
  'table-name' = 'SpendingByDay',
  'url' = 'jdbc:postgresql://${POSTGRES_AUTHORITY}',
  'username' = '${POSTGRES_USERNAME}'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO `default_catalog`.`default_database`.`CustomerTransaction_1`
  SELECT *
  FROM `default_catalog`.`default_database`.`CustomerTransaction`
  ON CONFLICT DO DEDUPLICATE
;
INSERT INTO `default_catalog`.`default_database`.`SpendingByCategory_2`
  SELECT `customerId`, `timeWeek`, `category`, `spending`, `hash_columns`(`customerId`, `timeWeek`, `category`, `spending`) AS `__pk_hash`
  FROM `default_catalog`.`default_database`.`SpendingByCategory`
  ON CONFLICT DO DEDUPLICATE
;
INSERT INTO `default_catalog`.`default_database`.`SpendingByDay_3`
  SELECT `customerId`, `timeDay`, `spending`, `hash_columns`(`customerId`, `timeDay`, `spending`) AS `__pk_hash`
  FROM `default_catalog`.`default_database`.`SpendingByDay`
  ON CONFLICT DO DEDUPLICATE
;
END
