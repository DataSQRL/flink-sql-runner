/*
 * Copyright © 2024 DataSQRL (contact@datasqrl.com)
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

import com.datasqrl.function.StandardLibraryFunction;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class for executing SQL scripts programmatically. */
class SqlExecutor {

  private static final Logger log = LoggerFactory.getLogger(SqlExecutor.class);

  private static final Pattern SET_STATEMENT_PATTERN =
      Pattern.compile("SET\\s+'(\\S+)'\\s*=\\s*'(.+)';?", Pattern.CASE_INSENSITIVE);

  private final TableEnvironment tableEnv;

  public SqlExecutor(Configuration configuration, String udfPath) {
    StreamExecutionEnvironment sEnv =
        StreamExecutionEnvironment.getExecutionEnvironment(configuration);

    EnvironmentSettings tEnvConfig =
        EnvironmentSettings.newInstance().withConfiguration(configuration).build();

    this.tableEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    if (udfPath != null) {
      setupUdfPath(udfPath);
    }
  }

  public void setupSystemFunctions() {

    ServiceLoader<StandardLibraryFunction> standardLibraryFunctions =
        ServiceLoader.load(StandardLibraryFunction.class);

    standardLibraryFunctions.forEach(
        function -> {
          String sql =
              String.format(
                  "CREATE TEMPORARY FUNCTION IF NOT EXISTS `%s` AS '%s' LANGUAGE JAVA;",
                  getFunctionNameFromClass(function.getClass()), function.getClass().getName());

          System.out.println(sql);
          tableEnv.executeSql(sql);
        });
  }

  static String getFunctionNameFromClass(Class clazz) {
    //	    String fctName = clazz.getSimpleName();
    //	    fctName = Character.toLowerCase(fctName.charAt(0)) + fctName.substring(1);
    //	    return fctName;
    return clazz.getSimpleName().toLowerCase();
  }

  /**
   * Executes a single SQL script.
   *
   * @param script The SQL script content.
   * @return
   * @throws Exception If execution fails.
   */
  public TableResult executeScript(String script) throws Exception {
    List<String> statements = SqlUtils.parseStatements(script);
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
      Matcher setMatcher = SET_STATEMENT_PATTERN.matcher(statement.trim());

      if (setMatcher.matches()) {
        // Handle SET statements
        String key = setMatcher.group(1);
        String value = setMatcher.group(2);
        tableEnv.getConfig().getConfiguration().setString(key, value);
        log.info("Set configuration: {} = {}", key, value);
      } else {
        System.out.println(statement);
        log.info("Executing statement:\n{}", statement);
        tableResult = tableEnv.executeSql(EnvironmentVariablesUtils.replaceWithEnv(statement));
      }
    } catch (Exception e) {
      e.addSuppressed(new RuntimeException("Error while executing stmt: " + statement));
      throw e;
    }
    return tableResult;
  }

  public String replaceWithEnv(String command) {
    Map<String, String> envVariables = System.getenv();
    Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");

    String substitutedStr = command;
    StringBuffer result = new StringBuffer();
    // First pass to replace environment variables
    Matcher matcher = pattern.matcher(substitutedStr);
    while (matcher.find()) {
      String key = matcher.group(1);
      String envValue = envVariables.getOrDefault(key, "");
      matcher.appendReplacement(result, Matcher.quoteReplacement(envValue));
    }
    matcher.appendTail(result);

    return result.toString();
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
      File udfDir = new File(udfPath);
      if (udfDir.exists() && udfDir.isDirectory()) {
        File[] jarFiles = udfDir.listFiles((dir, name) -> name.endsWith(".jar"));
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
    }
  }

  /**
   * Executes a compiled plan from JSON.
   *
   * @param planJson The JSON content of the compiled plan.
   * @return
   * @throws Exception If execution fails.
   */
  protected TableResult executeCompiledPlan(String planJson) throws Exception {
    log.info("Executing compiled plan from JSON.");
    try {
      PlanReference planReference = PlanReference.fromJsonString(planJson);
      TableResult result = tableEnv.executePlan(planReference);
      log.info("Compiled plan executed.");
      return result;
    } catch (Exception e) {
      log.error("Failed to execute compiled plan", e);
      throw e;
    }
  }
}
