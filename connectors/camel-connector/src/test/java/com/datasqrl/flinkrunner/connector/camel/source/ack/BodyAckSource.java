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
package com.datasqrl.flinkrunner.connector.camel.source.ack;

import com.datasqrl.flinkrunner.camel.AckSource;
import java.io.Serializable;
import org.apache.camel.Exchange;

/** Reads each exchange's body as text and maps it with {@code mapping}. */
record BodyAckSource(String endpoint, Mapping mapping) implements AckSource<String> {

  interface Mapping extends Serializable {
    String map(String body) throws Exception;
  }

  @Override
  public String map(Exchange exchange) throws Exception {
    return mapping.map(exchange.getMessage().getBody(String.class));
  }
}
