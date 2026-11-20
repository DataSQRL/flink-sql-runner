--
-- Copyright © 2026 DataSQRL (contact@datasqrl.com)
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

CREATE TABLE orders (
  orderId STRING,
  customerId STRING,
  amount DECIMAL(18, 2),
  createdAt TIMESTAMP_LTZ(3),
  ingested_at TIMESTAMP_LTZ(3) METADATA,
  WATERMARK FOR ingested_at AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'camel',
  'source.class' = 'sample.camel.Orders',
  'ack.max-pending' = '1000'
);
