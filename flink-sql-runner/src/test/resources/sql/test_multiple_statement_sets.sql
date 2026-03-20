CREATE TABLE InA (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10'
);

CREATE TABLE InB (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10'
);

CREATE TABLE OutA (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'print',
    'print-identifier' = 'PrintOutputA'
);

CREATE TABLE OutB (
    id BIGINT,
    name STRING
) WITH (
    'connector' = 'print',
    'print-identifier' = 'PrintOutputB'
);

EXECUTE STATEMENT SET BEGIN
INSERT INTO OutA
SELECT * FROM InA;
END;

EXECUTE STATEMENT SET BEGIN
INSERT INTO OutB
SELECT * FROM InB;
END
