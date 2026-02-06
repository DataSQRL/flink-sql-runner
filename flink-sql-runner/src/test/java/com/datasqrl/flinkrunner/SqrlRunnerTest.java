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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class SqrlRunnerTest {

  @Test
  void testRunDelegatesToBaseRunnerAndUsesSqlExecutor() throws Exception {
    Configuration config = new Configuration();
    String udfPath = "dummy/path";

    SqrlRunner runner =
        spy(new SqrlRunner(RuntimeExecutionMode.STREAMING, config, null, null, null, udfPath));
    TableResult mockedResult = mock(TableResult.class);
    SqlExecutor mockedExecutor = mock(SqlExecutor.class);

    try (MockedStatic<SqlExecutor> mockedStatic = mockStatic(SqlExecutor.class)) {
      mockedStatic
          .when(() -> SqlExecutor.withUdfClassLoader(config, udfPath))
          .thenReturn(mockedExecutor);

      doReturn(mockedResult).when(runner).run(any());

      TableResult result = runner.run();

      assertThat(result).isSameAs(mockedResult);
      mockedStatic.verify(() -> SqlExecutor.withUdfClassLoader(config, udfPath));
      verify(runner).run(any());
    }
  }
}
