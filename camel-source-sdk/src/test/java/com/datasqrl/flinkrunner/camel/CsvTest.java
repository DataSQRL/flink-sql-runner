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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class CsvTest {

  @Test
  void parsesQuotedFields() {
    Exchange file = new DefaultExchange(new DefaultCamelContext());
    file.getMessage().setBody("1,\"a,b\"\n2,c\n");

    assertThat(Csv.parse(file, Csv.NO_HEADER))
        .containsExactly(List.of("1", "a,b"), List.of("2", "c"));
  }
}
