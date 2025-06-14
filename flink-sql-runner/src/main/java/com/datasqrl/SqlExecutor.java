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

import com.datasqrl.flinkrunner.functions.AutoRegisterSystemFunction;
import java.io.File;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for executing SQL scripts programmatically. */
class SqlExecutor {

  private static final Logger log = LoggerFactory.getLogger(SqlExecutor.class);

  private static final Pattern SET_STATEMENT_PATTERN =
      Pattern.compile("SET\\s+'(\\S+)'\\s*=\\s*'(.+)';?", Pattern.CASE_INSENSITIVE);

  private final TableEnvironment tableEnv;

  public SqlExecutor(Configuration configuration, String udfPath) {
    var sEnv = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

    var tEnvConfig = EnvironmentSettings.newInstance().withConfiguration(configuration).build();

    this.tableEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    if (StringUtils.isNotBlank(udfPath)) {
      setupUdfPath(udfPath);
    }
  }

  public void setupSystemFunctions() {
    System.out.println("Setting up automatically registered system functions");

    try {
      ServiceLoader<AutoRegisterSystemFunction> standardLibraryFunctions =
          ServiceLoader.load(AutoRegisterSystemFunction.class);

      standardLibraryFunctions.forEach(
          function ->
              getFunctionNameAndClass(function.getClass())
                  .ifPresent(
                      funcParts ->
                          tableEnv.createTemporarySystemFunction(funcParts.f0, funcParts.f1)));
    } catch (Throwable e) {
      e.printStackTrace(System.out);
      throw new RuntimeException(e);
    }

    System.out.println("Completed auto function registered system functions");
  }

  /**
   * Executes a single SQL script.
   *
   * @param script The SQL script content.
   * @return
   */
  public TableResult executeScript(String script) {
    var statements = SqlUtils.parseStatements(script);
    TableResult tableResult = null;
    for (String statement : statements) {
      tableResult = executeStatement(statement);
    }

    return tableResult;
  }

  /**
   * Executes a single SQL statement.
   *
   * @param statement The SQL statement.
   * @return
   */
  private TableResult executeStatement(String statement) {
    TableResult tableResult = null;
    try {
      var setMatcher = SET_STATEMENT_PATTERN.matcher(statement.trim());

      if (setMatcher.matches()) {
        // Handle SET statements
        var key = setMatcher.group(1);
        var value = setMatcher.group(2);
        tableEnv.getConfig().getConfiguration().setString(key, value);
        log.info("Set configuration: {} = {}", key, value);
      } else {
        System.out.println(statement);
        log.info("Executing statement:\n{}", statement);
        tableResult = tableEnv.executeSql(EnvVarUtils.resolveEnvVars(statement));
      }
    } catch (Exception e) {
      e.addSuppressed(new RuntimeException("Error while executing stmt: " + statement));
      throw e;
    }
    return tableResult;
  }

  /**
   * Sets up the UDF path in the TableEnvironment.
   *
   * @param udfPath The path to UDFs.
   */
  private void setupUdfPath(String udfPath) {
    // Load and register UDFs from the provided path
    log.info("Setting up UDF path: {}", udfPath);
    // Implementation depends on how UDFs are packaged and should be loaded
    // For example, you might use a URLClassLoader to load JARs from udfPath
    try {
      var udfDir = new File(udfPath);
      if (udfDir.exists() && udfDir.isDirectory()) {
        var jarFiles = udfDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jarFiles != null) {
          for (File jarFile : jarFiles) {
            tableEnv.executeSql("ADD JAR 'file://" + jarFile.getAbsolutePath() + "'");
            log.info("Added UDF JAR: {}", jarFile.getAbsolutePath());
          }
        }
      } else {
        log.warn("UDF path does not exist or is not a directory: {}", udfPath);
      }
    } catch (Exception e) {
      log.error("Failed to set up UDF path", e);
      e.printStackTrace(System.out);
    }
  }

  /**
   * Executes a compiled plan from JSON.
   *
   * @param planJson The JSON content of the compiled plan.
   * @return
   */
  protected TableResult executeCompiledPlan(String planJson) {
    log.info("Executing compiled plan from JSON.");
    try {
      var planReference = PlanReference.fromJsonString(planJson);
      var result = tableEnv.executePlan(planReference);
      log.info("Compiled plan executed.");
      return result;
    } catch (Exception e) {
      log.error("Failed to execute compiled plan", e);
      throw e;
    }
  }

  static Optional<Tuple2<String, Class<? extends UserDefinedFunction>>> getFunctionNameAndClass(
      Class<?> clazz) {
    Tuple2<String, Class<? extends UserDefinedFunction>> res = null;

    if (UserDefinedFunction.class.isAssignableFrom(clazz)) {
      var funcName = clazz.getSimpleName().toLowerCase();
      res = Tuple2.of(funcName, (Class<? extends UserDefinedFunction>) clazz);

      log.info("Registering '{}' ...", funcName);

    } else {
      log.warn(
          "Skip registering '{}' as it does not extend '{}'", clazz, UserDefinedFunction.class);
    }

    return Optional.ofNullable(res);
  }
}
