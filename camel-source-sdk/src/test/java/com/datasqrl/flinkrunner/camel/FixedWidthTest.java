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

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

class FixedWidthTest {

  private static final String EBCDIC = "IBM037";

  @Test
  void decodesAndSplitsOnLineFeedAndNextLine() throws Exception {
    Exchange file = new DefaultExchange(new DefaultCamelContext());
    file.getMessage().setBody("HDR\u0085ROW1\nROW2".getBytes(EBCDIC));

    assertThat(FixedWidth.lines(file, EBCDIC)).containsExactly("HDR", "ROW1", "ROW2");
  }
}
