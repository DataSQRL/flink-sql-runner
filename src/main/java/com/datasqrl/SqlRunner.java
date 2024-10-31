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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.util.FileUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Main class for executing SQL scripts using picocli. */
@Command(
    name = "SqlRunner",
    mixinStandardHelpOptions = true,
    version = "1.0",
    description = "Runs SQL scripts using Flink TableEnvironment.")
@Slf4j
public class SqlRunner implements Callable<Integer> {

  @Option(
      names = {"-s", "--sqlfile"},
      description = "SQL file to execute.")
  private File sqlFile;

  @Option(
      names = {"--block"},
      description = "Wait for the flink job manager to exit.",
      defaultValue = "false")
  private boolean block;

  @Option(
      names = {"--planfile"},
      description = "Compiled plan JSON file.")
  private File planFile;

  @Option(
      names = {"--configfile"},
      description = "Configuration YAML file.")
  private File configFile;

  @Option(
      names = {"--udfpath"},
      description = "Path to UDFs.")
  private String udfPath;

  public static void main(String[] args) {
    var exitCode = new CommandLine(new SqlRunner()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    // Determine UDF path
    if (udfPath == null) {
      udfPath = System.getenv("UDF_PATH");
    }

    // Load configuration if configFile is provided
    var configuration = new Configuration();
    if (configFile != null) {
      configuration = loadConfigurationFromYaml(configFile);
    }

    log.info("Environment variables");
    TreeMap<String, String> envVariables = new TreeMap<>(System.getenv());
    envVariables.forEach((name, value) -> log.info("{}: {}", name, value));

    // Initialize SqlExecutor
    var sqlExecutor = new SqlExecutor(configuration, udfPath, envVariables);
    TableResult tableResult;
    // Input validation and execution logic
    if (sqlFile != null) {
      // Single SQL file mode
      var script = FileUtils.readFileUtf8(sqlFile);
      EnvironmentVariablesUtils.validateEnvironmentVariables(envVariables, script);
      tableResult = sqlExecutor.executeScript(script);
    } else if (planFile != null) {
      // Compiled plan JSON file
      var planJson = FileUtils.readFileUtf8(planFile);
      planJson = replaceScriptWithEnv(planJson);

      tableResult = sqlExecutor.executeCompiledPlan(planJson);
    } else {
      log.error("Invalid input. Please provide one of the following combinations:");
      log.error("- A single SQL file (--sqlfile)");
      log.error("- A plan JSON file (--planfile)");
      return 1;
    }

    if (block) {
      tableResult.await();
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
   * @param configFile The YAML configuration file.
   * @return A Configuration object.
   * @throws Exception If an error occurs while reading the file.
   */
  private Configuration loadConfigurationFromYaml(File configFile) throws Exception {
    log.info("Loading configuration from {}", configFile.getAbsolutePath());
    var configuration = GlobalConfiguration.loadConfiguration(configFile.getAbsolutePath());
    return configuration;
  }
}
