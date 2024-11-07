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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
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
      version = "1.0",
      description = "Runs SQL scripts using Flink TableEnvironment.")
  public static class SqlRunner implements Callable<Void> {

    @Option(
        names = {"-s", "--sqlfile"},
        description = "SQL file to execute.")
    private String sqlFile;

    @Option(
        names = {"--block"},
        description = "Wait for the flink job manager to exit.",
        defaultValue = "false")
    private boolean block;

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
  private final boolean block;

  public static void main(String[] args) throws Exception {
    System.out.printf("\n\nExecuting flink-jar-runner: %s\n\n", Arrays.toString(args));

    CommandLine cl = new CommandLine(new SqlRunner());
    int exitCode = cl.execute(args);
    SqlRunner runner = cl.getCommand();

    // Determine UDF path
    if (runner.udfPath == null) {
      runner.udfPath = System.getenv("UDF_PATH");
    }

    new FlinkMain(runner.sqlFile, runner.planFile, runner.configDir, runner.udfPath, runner.block)
        .run();
    System.out.println("Finished flink-jar-runner");
  }

  public TableResult run() throws Exception {

    // Load configuration if configDir is provided
    Configuration configuration = new Configuration();
    if (configDir != null) {
      configuration = loadConfigurationFromYaml(configDir);
    }

    // Initialize SqlExecutor
    SqlExecutor sqlExecutor = new SqlExecutor(configuration, udfPath);
    TableResult tableResult;
    // Input validation and execution logic
    if (sqlFile != null) {
      // Single SQL file mode
      String script = FileUtils.readFileUtf8(new File(sqlFile));

      Set<String> missingEnvironmentVariables =
          EnvironmentVariablesUtils.validateEnvironmentVariables(script);
      if (!missingEnvironmentVariables.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "Could not find the following environment variables: %s",
                missingEnvironmentVariables));
      }

      tableResult = sqlExecutor.executeScript(script);
    } else if (planFile != null) {
      // Compiled plan JSON file
      String planJson = FileUtils.readFileUtf8(new File(planFile));

      Set<String> missingEnvironmentVariables =
          EnvironmentVariablesUtils.validateEnvironmentVariables(planJson);
      if (!missingEnvironmentVariables.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "Could not find the following environment variables: %s",
                missingEnvironmentVariables));
      }

      planJson = EnvironmentVariablesUtils.replaceWithEnv(planJson);

      tableResult = sqlExecutor.executeCompiledPlan(planJson);
    } else {
      System.err.println("Invalid input. Please provide one of the following combinations:");
      System.err.println("- A single SQL file (--sqlfile)");
      System.err.println("- A plan JSON file (--planfile)");
      return null;
    }

    if (block) {
      tableResult.await();
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
