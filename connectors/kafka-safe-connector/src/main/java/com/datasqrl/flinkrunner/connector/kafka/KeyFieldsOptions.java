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
package com.datasqrl.flinkrunner.connector.kafka;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class KeyFieldsOptions {

  public static final ConfigOption<Boolean> SCAN_KEY_FIELDS_READ_FROM_VALUE =
      ConfigOptions.key("scan.key-fields.read-from-value")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether to read the key fields from the value instead of decoding the record key, "
                  + "when the value contains every key field ('value.fields-include' = 'ALL'). "
                  + "This keeps records without a key, which are dropped silently when the key is "
                  + "decoded. Set it to false to decode the key and drop such records, like the "
                  + "'kafka' connector does. Only affects reading: writing always sets the key "
                  + "from 'key.fields'.");
}
