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
package com.datasqrl.flinkrunner.camel;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class JsonTest {

  @Test
  void parsesText() {
    assertThat(Json.parse("{\"a\":1}").get("a").asInt()).isEqualTo(1);
  }

  @Test
  void bodyIsParsedOncePerExchange() {
    Exchange exchange = new DefaultExchange(new DefaultCamelContext());
    exchange.getMessage().setBody(new ByteArrayInputStream("{\"a\":1}".getBytes(UTF_8)));

    var first = Json.body(exchange);

    assertThat(Json.body(exchange)).isSameAs(first);
    assertThat(first.get("a").asInt()).isEqualTo(1);
  }
}
