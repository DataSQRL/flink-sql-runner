/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.flinkrunner.connector.kafka;

public enum DeserFailureHandlerType {
  /**
   * No deserialization failure handling is applied. In case of a problematic record, the
   * application will fail. This is the default setting.
   */
  NONE,

  /**
   * In case of a problematic record, helpful information will be logged. The application continues
   * the execution.
   */
  LOG,

  /**
   * In case of a problematic record, helpful information will be logged, and the record will be
   * sent to a configured Kafka topic as well. The application continues the execution.
   */
  KAFKA
}
