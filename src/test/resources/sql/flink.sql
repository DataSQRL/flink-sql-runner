-- Updated Flink SQL using datagen connector and BIGINT for timestamps, with full pipeline and view updates

CREATE TEMPORARY TABLE `transaction_1` (
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

CREATE TEMPORARY TABLE `cardassignment_1` (
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

CREATE TEMPORARY TABLE `merchant_1` (
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

CREATE TEMPORARY TABLE `_spendingbyday_1` (
  `customerid` BIGINT NOT NULL,
  `timeDay` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `spending` DOUBLE NOT NULL,
  PRIMARY KEY (`customerid`, `timeDay`) NOT ENFORCED
) WITH (
  'password' = '${JDBC_PASSWORD}',
  'connector' = 'jdbc',
  'driver' = 'org.postgresql.Driver',
  'table-name' = '_spendingbyday_1',
  'url' = '${JDBC_URL}',
  'username' = '${JDBC_USERNAME}'
);

CREATE TEMPORARY TABLE `customertransaction_1` (
  `transactionId` BIGINT NOT NULL,
  `cardNo` DOUBLE NOT NULL,
  `time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `amount` DOUBLE NOT NULL,
  `merchantName` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `category` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `customerid` BIGINT NOT NULL,
  PRIMARY KEY (`transactionId`, `time`) NOT ENFORCED
) WITH (
  'password' = '${JDBC_PASSWORD}',
  'connector' = 'jdbc',
  'driver' = 'org.postgresql.Driver',
  'table-name' = 'customertransaction_1',
  'url' = '${JDBC_URL}',
  'username' = '${JDBC_USERNAME}'
);

CREATE TEMPORARY TABLE `spendingbycategory_1` (
  `customerid` BIGINT NOT NULL,
  `timeWeek` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `category` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `spending` DOUBLE NOT NULL,
  PRIMARY KEY (`customerid`, `timeWeek`, `category`) NOT ENFORCED
) WITH (
  'password' = '${JDBC_PASSWORD}',
  'connector' = 'jdbc',
  'driver' = 'org.postgresql.Driver',
  'table-name' = 'spendingbycategory_1',
  'url' = '${JDBC_URL}',
  'username' = '${JDBC_USERNAME}'
);

CREATE VIEW `table$1`
AS
SELECT *
FROM (SELECT `merchantId`, `name`, `category`, `updatedTime`, ROW_NUMBER() OVER (PARTITION BY `merchantId` ORDER BY `updatedTime` DESC) AS `_rownum`
  FROM `merchant_1`) AS `t`
WHERE `_rownum` = 1;

CREATE VIEW `table$2`
AS
SELECT *
FROM (SELECT `customerId`, `cardNo`, `timestamp`, upper(`cardType`) as cardType, ROW_NUMBER() OVER (PARTITION BY `cardNo` ORDER BY `timestamp` DESC) AS `_rownum`
  FROM `cardassignment_1`) AS `t1`
WHERE `_rownum` = 1;

CREATE VIEW `table$3`
AS
SELECT `$cor0`.`customerId` AS `customerid`, CAST(FLOOR(`$cor0`.`time` TO DAY) + INTERVAL '1' DAY AS TIMESTAMP_LTZ(3)) AS `timeDay`, `$cor0`.`amount`, `$cor0`.`transactionId`, `$cor0`.`time`
FROM (SELECT *
  FROM `transaction_1` AS `$cor1`
   INNER JOIN `table$2` FOR SYSTEM_TIME AS OF `$cor1`.`time` AS `t2` ON `$cor1`.`cardNo` = `t2`.`cardNo`) AS `$cor0`
 INNER JOIN `table$1` FOR SYSTEM_TIME AS OF `$cor0`.`time` AS `t0` ON `$cor0`.`merchantId` = `t0`.`merchantId`;

CREATE VIEW `table$4`
AS
SELECT `customerid`, `window_time` AS `timeDay`, SUM(`amount`) AS `spending`
FROM TABLE(TUMBLE(TABLE `table$3`, DESCRIPTOR(`time`), INTERVAL '86400' SECOND(8), INTERVAL '0' SECOND(1))) AS `t6`
GROUP BY `customerid`, `window_start`, `window_end`, `window_time`;

CREATE VIEW `table$5`
AS
SELECT *
FROM (SELECT `merchantId`, `name`, `category`, `updatedTime`, ROW_NUMBER() OVER (PARTITION BY `merchantId` ORDER BY `updatedTime` DESC) AS `_rownum`
  FROM `merchant_1`) AS `t`
WHERE `_rownum` = 1;

CREATE VIEW `table$6`
AS
SELECT *
FROM (SELECT `customerId`, `cardNo`, `timestamp`, upper(`cardType`) as cardType, ROW_NUMBER() OVER (PARTITION BY `cardNo` ORDER BY `timestamp` DESC) AS `_rownum`
  FROM `cardassignment_1`) AS `t1`
WHERE `_rownum` = 1;

CREATE VIEW `table$7`
AS
SELECT `$cor2`.`transactionId`, `$cor2`.`cardNo`, `$cor2`.`time`, `$cor2`.`amount`, `t0`.`name` AS `merchantName`, `t0`.`category`, `$cor2`.`customerId` AS `customerid`
FROM (SELECT *
  FROM `transaction_1` AS `$cor3`
   INNER JOIN `table$6` FOR SYSTEM_TIME AS OF `$cor3`.`time` AS `t2` ON `$cor3`.`cardNo` = `t2`.`cardNo`) AS `$cor2`
 INNER JOIN `table$5` FOR SYSTEM_TIME AS OF `$cor2`.`time` AS `t0` ON `$cor2`.`merchantId` = `t0`.`merchantId`;

CREATE VIEW `table$8`
AS
SELECT *
FROM (SELECT `merchantId`, `name`, `category`, `updatedTime`, ROW_NUMBER() OVER (PARTITION BY `merchantId` ORDER BY `updatedTime` DESC) AS `_rownum`
  FROM `merchant_1`) AS `t`
WHERE `_rownum` = 1;

CREATE VIEW `table$9`
AS
SELECT *
FROM (SELECT `customerId`, `cardNo`, `timestamp`, `cardType`, ROW_NUMBER() OVER (PARTITION BY `cardNo` ORDER BY `timestamp` DESC) AS `_rownum`
  FROM `cardassignment_1`) AS `t1`
WHERE `_rownum` = 1;

CREATE VIEW `table$10`
AS
SELECT 
  `$cor6`.`customerId` AS `customerid`, 
  CAST(FLOOR(`$cor6`.`time` TO DAY) + INTERVAL '1' DAY * (7 - EXTRACT(DOW FROM `$cor6`.`time`)) AS TIMESTAMP_LTZ(3)) AS `timeWeek`, 
  `t0`.`category`, 
  `$cor6`.`amount`, 
  `$cor6`.`transactionId`, 
  `$cor6`.`time`
FROM (
  SELECT *
  FROM `transaction_1` AS `$cor7`
  INNER JOIN `table$9` FOR SYSTEM_TIME AS OF `$cor7`.`time` AS `t2` 
    ON `$cor7`.`cardNo` = `t2`.`cardNo`
) AS `$cor6`
INNER JOIN `table$8` FOR SYSTEM_TIME AS OF `$cor6`.`time` AS `t0` 
  ON `$cor6`.`merchantId` = `t0`.`merchantId`;

CREATE VIEW `table$11`
AS
SELECT `customerid`, `window_time` AS `timeWeek`, `category`, SUM(`amount`) AS `spending`
FROM TABLE(TUMBLE(TABLE `table$10`, DESCRIPTOR(`time`), INTERVAL '604800' SECOND(9), INTERVAL '0' SECOND(1))) AS `t6`
GROUP BY `customerid`, `category`, `window_start`, `window_end`, `window_time`;

EXECUTE STATEMENT SET BEGIN
INSERT INTO `_spendingbyday_1`
(SELECT *
 FROM `table$4`)
;
INSERT INTO `customertransaction_1`
(SELECT *
  FROM `table$7`)
;
INSERT INTO `spendingbycategory_1`
(SELECT *
   FROM `table$11`)
;
END;
