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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class FlinkMainTest {

  @TempDir private Path tempDir;

  @Test
  void run_shouldWarn_ifBothSqlAndPlanProvided() throws Exception {
    // Arrange
    Path sqlFile = tempDir.resolve("script.sql");
    Path planFile = tempDir.resolve("plan.json");

    Files.writeString(sqlFile, "SELECT 1;");
    Files.writeString(planFile, "{\"fake\":\"plan\"}");

    FlinkMain flinkMain =
        new FlinkMain(
            RuntimeExecutionMode.STREAMING, sqlFile.toString(), planFile.toString(), null, null);

    // Act & Assert
    assertThat(flinkMain.run(() -> null)).isEqualTo(1);
  }

  @Test
  void run_shouldFail_ifNoSqlOrPlanProvided() throws Exception {
    // Arrange
    var flinkMain = new FlinkMain(RuntimeExecutionMode.STREAMING, null, null, null, null);

    // Assert
    assertThat(flinkMain.run(() -> null)).isEqualTo(2);
  }

  @Test
  void run_shouldExecuteSqlScript_ifSqlFileProvided() throws Exception {
    // Arrange
    Path sqlFile = tempDir.resolve("script.sql");
    String sql = "SELECT * FROM dummy_table";
    Files.writeString(sqlFile, sql);

    SqlExecutor mockExecutor = mock(SqlExecutor.class);
    Supplier<SqlExecutor> supplier = () -> mockExecutor;

    FlinkMain flinkMain =
        new FlinkMain(RuntimeExecutionMode.STREAMING, sqlFile.toString(), null, null, null);

    // Act
    flinkMain.run(supplier);

    // Assert
    verify(mockExecutor).setupSystemFunctions();
    verify(mockExecutor).executeScript(sql);
    verifyNoMoreInteractions(mockExecutor);
  }

  @Test
  void run_shouldExecuteCompiledPlan_ifPlanFileProvided() throws Exception {
    // Arrange
    Path planFile = tempDir.resolve("plan.json");
    String planJson = "{\"pipeline\":\"plan\"}";
    Files.writeString(planFile, planJson);

    SqlExecutor mockExecutor = mock(SqlExecutor.class);
    Supplier<SqlExecutor> supplier = () -> mockExecutor;

    var flinkMain =
        new FlinkMain(RuntimeExecutionMode.STREAMING, null, planFile.toString(), null, null);

    // Act
    flinkMain.run(supplier);

    // Assert
    verify(mockExecutor).setupSystemFunctions();
    verify(mockExecutor).executeCompiledPlan(planJson);
    verifyNoMoreInteractions(mockExecutor);
  }

  @ParameterizedTest
  @EnumSource(RuntimeExecutionMode.class)
  void initConfiguration_shouldNotOverrideRuntimeModeIfAlreadySet(RuntimeExecutionMode mode)
      throws IOException {
    // Arrange
    Path configFile = tempDir.resolve("config.yaml");
    String yamlConf = "execution.runtime-mode: BATCH\ndummy.key: asd";
    Files.writeString(configFile, yamlConf);

    var flinkMain = new FlinkMain(mode, null, null, tempDir.toString(), null);

    // Act
    var finalConf = flinkMain.initConfiguration();

    // Assert
    assertThat(finalConf.keySet()).hasSize(2);
    assertThat(finalConf.get(ExecutionOptions.RUNTIME_MODE)).isEqualTo(RuntimeExecutionMode.BATCH);
  }

  @ParameterizedTest
  @EnumSource(RuntimeExecutionMode.class)
  void initConfiguration_shouldSetRuntimeModeFromCli(RuntimeExecutionMode mode) throws IOException {
    // Arrange
    Path configFile = tempDir.resolve("config.yaml");
    String yamlConf = "dummy.key: asd";
    Files.writeString(configFile, yamlConf);

    var flinkMain = new FlinkMain(mode, null, null, tempDir.toString(), null);

    // Act
    var finalConf = flinkMain.initConfiguration();

    // Assert
    assertThat(finalConf.keySet()).hasSize(2);
    assertThat(finalConf.get(ExecutionOptions.RUNTIME_MODE)).isEqualTo(mode);
  }

  @ParameterizedTest
  @EnumSource(RuntimeExecutionMode.class)
  void initConfiguration_shouldCreateNewConfigIfNoPathGiven(RuntimeExecutionMode mode) {
    // Arrange
    var flinkMain = new FlinkMain(mode, null, null, null, null);

    // Act
    var finalConf = flinkMain.initConfiguration();

    // Assert
    assertThat(finalConf.keySet()).hasSize(1);
    assertThat(finalConf.get(ExecutionOptions.RUNTIME_MODE)).isEqualTo(mode);
  }
}
