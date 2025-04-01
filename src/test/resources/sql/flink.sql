CREATE TEMPORARY FUNCTION IF NOT EXISTS `timestamptostring` AS 'com.datasqrl.time.TimestampToString' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofmonth` AS 'com.datasqrl.time.EndOfMonth' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `timestamptoepochmilli` AS 'com.datasqrl.time.TimestampToEpochMilli' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofweek` AS 'com.datasqrl.time.EndOfWeek' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `parsetimestamp` AS 'com.datasqrl.time.ParseTimestamp' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `epochmillitotimestamp` AS 'com.datasqrl.time.EpochMilliToTimestamp' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofminute` AS 'com.datasqrl.time.EndOfMinute' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `timestamptoepoch` AS 'com.datasqrl.time.TimestampToEpoch' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofsecond` AS 'com.datasqrl.time.EndOfSecond' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `epochtotimestamp` AS 'com.datasqrl.time.EpochToTimestamp' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `atzone` AS 'com.datasqrl.time.AtZone' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofday` AS 'com.datasqrl.time.EndOfDay' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofhour` AS 'com.datasqrl.time.EndOfHour' LANGUAGE JAVA;

CREATE TEMPORARY FUNCTION IF NOT EXISTS `endofyear` AS 'com.datasqrl.time.EndOfYear' LANGUAGE JAVA;

CREATE TEMPORARY TABLE `transaction_1` (
  `transactionId` BIGINT NOT NULL,
  `cardNo` DOUBLE NOT NULL,
  `time` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `amount` DOUBLE NOT NULL,
  `merchantId` BIGINT NOT NULL,
  PRIMARY KEY (`transactionId`, `time`) NOT ENFORCED,
  WATERMARK FOR `time` AS `time` - INTERVAL '1.0' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:///datasources/transaction.jsonl',
  'source.monitor-interval' = '1 min',
  'connector' = 'filesystem'
);

CREATE TEMPORARY TABLE `cardassignment_1` (
  `customerId` BIGINT NOT NULL,
  `cardNo` DOUBLE NOT NULL,
  `timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `cardType` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  PRIMARY KEY (`customerId`, `cardNo`, `timestamp`) NOT ENFORCED,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '1.0' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:///datasources/cardAssignment.jsonl',
  'source.monitor-interval' = '1 min',
  'connector' = 'filesystem'
);

CREATE TEMPORARY TABLE `merchant_1` (
  `merchantId` BIGINT NOT NULL,
  `name` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `category` VARCHAR(2147483647) CHARACTER SET `UTF-16LE` NOT NULL,
  `updatedTime` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  PRIMARY KEY (`merchantId`, `updatedTime`) NOT ENFORCED,
  WATERMARK FOR `updatedTime` AS `updatedTime` - INTERVAL '1.0' SECOND
) WITH (
  'format' = 'flexible-json',
  'path' = 'file:///datasources/merchant.jsonl',
  'source.monitor-interval' = '1 min',
  'connector' = 'filesystem'
);

CREATE TEMPORARY TABLE `_spendingbyday_1` (
  `customerid` BIGINT NOT NULL,
  `timeDay` TIMESTAMP(3) WITH LOCAL TIME ZONE NOT NULL,
  `spending` DOUBLE NOT NULL,
  PRIMARY KEY (`customerid`, `timeDay`) NOT ENFORCED
) WITH (
  'password' = '${JDBC_PASSWORD}',
  'connector' = 'jdbc-sqrl',
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
  'connector' = 'jdbc-sqrl',
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
  'connector' = 'jdbc-sqrl',
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
FROM (SELECT `customerId`, `cardNo`, `timestamp`, `cardType`, ROW_NUMBER() OVER (PARTITION BY `cardNo` ORDER BY `timestamp` DESC) AS `_rownum`
  FROM `cardassignment_1`) AS `t1`
WHERE `_rownum` = 1;

CREATE VIEW `table$3`
AS
SELECT `$cor0`.`customerId` AS `customerid`, ENDOFDAY(`$cor0`.`time`) AS `timeDay`, `$cor0`.`amount`, `$cor0`.`transactionId`, `$cor0`.`time`
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
FROM (SELECT `customerId`, `cardNo`, `timestamp`, `cardType`, ROW_NUMBER() OVER (PARTITION BY `cardNo` ORDER BY `timestamp` DESC) AS `_rownum`
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
SELECT `$cor6`.`customerId` AS `customerid`, ENDOFWEEK(`$cor6`.`time`) AS `timeWeek`, `t0`.`category`, `$cor6`.`amount`, `$cor6`.`transactionId`, `$cor6`.`time`
FROM (SELECT *
  FROM `transaction_1` AS `$cor7`
   INNER JOIN `table$9` FOR SYSTEM_TIME AS OF `$cor7`.`time` AS `t2` ON `$cor7`.`cardNo` = `t2`.`cardNo`) AS `$cor6`
 INNER JOIN `table$8` FOR SYSTEM_TIME AS OF `$cor6`.`time` AS `t0` ON `$cor6`.`merchantId` = `t0`.`merchantId`;

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

