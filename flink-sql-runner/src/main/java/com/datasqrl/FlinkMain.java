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

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Callable;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.FileUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@RequiredArgsConstructor
public class FlinkMain {

  @Command(
      name = "SqlRunner",
      mixinStandardHelpOptions = true,
      version = "0.2",
      description = "Runs SQL scripts using Flink TableEnvironment.")
  public static class SqlRunner implements Callable<Void> {

    @Option(
        names = {"-s", "--sqlfile"},
        description = "SQL file to execute.")
    private String sqlFile;

    @Option(
        names = {"--planfile"},
        description = "Compiled plan JSON file.")
    private String planFile;

    @Option(
        names = {"--config-dir"},
        description = "Directory containing configuration YAML file.")
    private String configDir;

    @Option(
        names = {"--udfpath"},
        description = "Path to UDFs.")
    private String udfPath;

    @Override
    public Void call() throws Exception {
      return null;
    }
  }

  private final String sqlFile;
  private final String planFile;
  private final String configDir;
  private final String udfPath;

  public static void main(String[] args) throws Exception {
    System.out.printf("\n\nExecuting flink-sql-runner: %s\n\n", Arrays.toString(args));

    var cl = new CommandLine(new SqlRunner());
    cl.execute(args);
    SqlRunner runner = cl.getCommand();

    // Determine UDF path
    if (runner.udfPath == null) {
      runner.udfPath = System.getenv("UDF_PATH");
    }

    new FlinkMain(runner.sqlFile, runner.planFile, runner.configDir, runner.udfPath).run();
    System.out.println("Finished flink-sql-runner");
  }

  public int run() throws Exception {

    // Load configuration if configDir is provided
    var configuration = new Configuration();
    if (configDir != null) {
      configuration = loadConfigurationFromYaml(configDir);
    }

    // Initialize SqlExecutor
    var sqlExecutor = new SqlExecutor(configuration, udfPath);
    TableResult tableResult;
    // Input validation and execution logic
    if (sqlFile != null) {
      if (planFile != null) {
        System.err.println("Provide either a SQL file or a compiled plan - not both.");
      }
      // Single SQL file mode
      var script = FileUtils.readFileUtf8(new File(sqlFile));

      var missingEnvironmentVariables =
          EnvironmentVariablesUtils.validateEnvironmentVariables(script);
      if (!missingEnvironmentVariables.isEmpty()) {
        throw new IllegalStateException(
            "Could not find the following environment variables: %s"
                .formatted(missingEnvironmentVariables));
      }

      tableResult = sqlExecutor.executeScript(script);
    } else if (planFile != null) {
      // Compiled plan JSON file
      var planJson = FileUtils.readFileUtf8(new File(planFile));

      var missingEnvironmentVariables =
          EnvironmentVariablesUtils.validateEnvironmentVariables(planJson);
      if (!missingEnvironmentVariables.isEmpty()) {
        throw new IllegalStateException(
            "Could not find the following environment variables: %s"
                .formatted(missingEnvironmentVariables));
      }

      planJson = replaceScriptWithEnv(planJson);

      sqlExecutor.setupSystemFunctions();
      tableResult = sqlExecutor.executeCompiledPlan(planJson);
    } else {
      System.err.println("Invalid input. Please provide one of the following combinations:");
      System.err.println("- A single SQL file (--sqlfile)");
      System.err.println("- A plan JSON file (--planfile)");
      return 1;
    }

    return 0;
  }

  @SneakyThrows
  private String replaceScriptWithEnv(String script) {
    ObjectMapper objectMapper = getObjectMapper();
    Map map = objectMapper.readValue(script, Map.class);
    return objectMapper.writeValueAsString(map);
  }

  public static ObjectMapper getObjectMapper() {
    var objectMapper = new ObjectMapper();

    // Register the custom deserializer module
    var module = new SimpleModule();
    module.addDeserializer(String.class, new JsonEnvVarDeserializer());
    objectMapper.registerModule(module);
    return objectMapper;
  }

  /**
   * Loads configuration from a YAML file.
   *
   * @param configDir The YAML configuration file.
   * @return A Configuration object.
   * @throws Exception If an error occurs while reading the file.
   */
  private Configuration loadConfigurationFromYaml(String configDir) throws Exception {
    System.out.printf("Loading configuration from %s\n", configDir);
    return GlobalConfiguration.loadConfiguration(configDir);
  }
}
