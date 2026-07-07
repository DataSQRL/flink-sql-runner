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

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SqlUtilsTest {

  static Stream<Arguments> testArgs() {
    return Stream.of(
        Arguments.of("flink.sql", 12),
        Arguments.of("test_sql.sql", 6),
        Arguments.of("test_udf_sql.sql", 6));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("testArgs")
  void givenSource_when_thenSplitCorrectly(String filename, int numberOfStatements)
      throws Exception {

    var scriptUrl = getClass().getResource("/sql/" + filename);
    assertThat(scriptUrl).isNotNull();

    var script = Resources.toString(scriptUrl, Charsets.UTF_8);
    var stmts = SqlUtils.parseStatements(script);
    assertThat(stmts).isNotNull().isNotEmpty().hasSize(numberOfStatements);
  }
}
