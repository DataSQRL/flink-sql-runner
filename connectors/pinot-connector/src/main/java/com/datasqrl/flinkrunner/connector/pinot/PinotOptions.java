/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
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
package com.datasqrl.flinkrunner.connector.pinot;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PinotOptions {

  public static final ConfigOption<String> CONTROLLER_URL =
      ConfigOptions.key("controller.url")
          .stringType()
          .noDefaultValue()
          .withDescription("Pinot controller base URL, e.g. http://localhost:9000");

  public static final ConfigOption<String> TABLE_NAME =
      ConfigOptions.key("table.name")
          .stringType()
          .noDefaultValue()
          .withDescription("Target Pinot OFFLINE table name");

  public static final ConfigOption<Long> SEGMENT_FLUSH_ROWS =
      ConfigOptions.key("segment.flush.rows")
          .longType()
          .defaultValue(500_000L)
          .withDescription("Max rows buffered per segment before it is flushed and uploaded");
}
