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
package com.datasqrl.flinkrunner;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CamelSourceIT extends AbstractITSupport {

  @Test
  void givenCamelSourceOnUdfPath_whenExecuting_thenRowsArePrinted() throws Exception {
    flinkRun("--sqlfile", "/it/sqlfile/camel_file_source.sql", "--udfpath", "/it/camel-udfs/");

    untilAssert(
        () -> {
          var printed =
              flinkContainer.execInContainer("bash", "-c", "cat /opt/flink/log/*.out || true");
          assertThat(printed.getStdout())
              .contains("+I[1, alice, people.csv]")
              .contains("+I[2, bob, people.csv]");
        });
  }
}
