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
package com.datasqrl.flinkrunner;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.FileUtils;

@RequiredArgsConstructor
@Slf4j
abstract class BaseRunner {

  final RuntimeExecutionMode mode;
  final EnvVarResolver resolver;
  @Nullable final String sqlFile;
  @Nullable final String planFile;
  @Nullable final String configDir;
  @Nullable final String udfPath;
  @Nullable final Configuration config;

  public TableResult run() throws Exception {
    var execConfig = config != null ? config : initConfiguration();

    return run(() -> new SqlExecutor(execConfig, udfPath));
  }

  @VisibleForTesting
  TableResult run(Supplier<SqlExecutor> sqlExecutorSupplier) throws Exception {
    var sqlExecutor = sqlExecutorSupplier.get();

    if (StringUtils.isNotBlank(sqlFile) && StringUtils.isNotBlank(planFile)) {
      throw new IllegalArgumentException(
          "Provide either a SQL file or a compiled plan - not both.");
    }

    if (StringUtils.isBlank(sqlFile) && StringUtils.isBlank(planFile)) {
      throw new IllegalArgumentException(
          "Invalid input. Please provide one of the following combinations: 1. A single SQL file (--sqlfile) 2. A plan JSON file (--planfile)");
    }

    if (StringUtils.isNotBlank(sqlFile)) {
      // Single SQL file mode
      var script = readTextFile(sqlFile);
      script = resolver.resolve(script);

      sqlExecutor.setupSystemFunctions();
      return sqlExecutor.executeScript(script);
    }

    // Compiled plan JSON file
    var planJson = readTextFile(planFile);
    planJson = resolver.resolveInJson(planJson);

    sqlExecutor.setupSystemFunctions();
    return sqlExecutor.executeCompiledPlan(planJson);
  }

  /**
   * Initializes Flink {@link Configuration}. Loads configuration from YAML file, if given. Sets
   * {@link RuntimeExecutionMode} based on the given CLI option, if it was not part of the YAML
   * config.
   *
   * @return the initialized Flink {@link Configuration} object
   */
  @VisibleForTesting
  Configuration initConfiguration() {
    var conf = new Configuration();
    if (StringUtils.isNotBlank(configDir)) {
      log.info("Loading Flink configuration from '{}'", configDir);
      conf = GlobalConfiguration.loadConfiguration(configDir);
    }

    // Do not overwrite runtime given in YAML
    if (!conf.contains(ExecutionOptions.RUNTIME_MODE)) {
      conf.set(ExecutionOptions.RUNTIME_MODE, mode);
    }

    return conf;
  }

  static String readTextFile(String path) throws IOException {
    var f = new File(path);
    if (!f.exists()) {
      throw new IllegalArgumentException(String.format("Given file '%s' does not exist", path));
    }

    if (!f.isFile()) {
      throw new IllegalArgumentException(
          String.format("Given file '%s' is not a regular file", path));
    }

    return FileUtils.readFileUtf8(f);
  }
}
