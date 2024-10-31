--
-- Copyright © 2024 DataSQRL (contact@datasqrl.com)
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE FUNCTION IF NOT EXISTS MYSCALARFUNCTION AS 'com.myudf.MyScalarFunction' LANGUAGE JAVA;

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
SELECT order_id, MYSCALARFUNCTION(customer_id, customer_id), order_amount, order_status, order_time
FROM orders;

-- Insert filtered MyOrders into blackhole_myorders
INSERT INTO blackhole_myorders
SELECT order_id, MYSCALARFUNCTION(customer_id, customer_id), order_amount, order_time
FROM MyOrders;

END;
