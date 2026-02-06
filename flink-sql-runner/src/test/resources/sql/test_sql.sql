-- Set parallelism and other configuration settings
SET 'execution.parallelism' = '4';

-- Create a source table using the datagen connector to simulate streaming data
CREATE TABLE orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DECIMAL(10, 2),
    order_status STRING,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '100',
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1',
    'fields.order_id.end' = '10000',
    'fields.customer_id.min' = '1',
    'fields.customer_id.max' = '1000',
    'fields.order_amount.min' = '10.00',
    'fields.order_amount.max' = '500.00',
    'fields.order_status.length' = '10'
);

-- Create a temporary view for filtering orders
CREATE TEMPORARY VIEW MyOrders AS
 SELECT order_id, customer_id, order_amount, order_time
 FROM orders
 WHERE order_id < 10;

-- Create a blackhole sink for orders
CREATE TABLE blackhole_orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DECIMAL(10, 2),
    order_status STRING,
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'blackhole'
);

-- Create a blackhole sink for MyOrders (filtered orders)
CREATE TABLE blackhole_myorders (
    order_id BIGINT,
    customer_id BIGINT,
    order_amount DECIMAL(10, 2),
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'print' --todo back to blackhole
);

-- Insert data into blackhole sinks using STATEMENT SET
EXECUTE STATEMENT SET BEGIN
-- Insert all orders into blackhole_orders
INSERT INTO blackhole_orders
SELECT order_id, customer_id, order_amount, order_status, order_time
FROM orders;

-- Insert filtered MyOrders into blackhole_myorders
INSERT INTO blackhole_myorders
SELECT order_id, customer_id, order_amount, order_time
FROM MyOrders;

END;
