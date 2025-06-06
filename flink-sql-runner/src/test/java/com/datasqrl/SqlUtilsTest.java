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
package com.datasqrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import java.io.IOException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SqlUtilsTest {

  @ParameterizedTest(name = "{0}")
  @CsvSource({"flink.sql,18", "test_sql.sql,6", "test_udf_sql.sql,6"})
  void givenSource_when_thenSplitCorrectly(String filename, int numberOfStatements)
      throws IOException, Exception {
    var script = Resources.toString(getClass().getResource("/sql/" + filename), Charsets.UTF_8);
    var stmts = SqlUtils.parseStatements(script);
    assertThat(stmts).isNotNull().isNotEmpty().hasSize(numberOfStatements);
  }
}
