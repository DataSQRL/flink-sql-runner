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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
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
        names = {"--configfile"},
        description = "Configuration YAML file.")
    private String configFile;

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

  public static void main(String[] args) throws Exception {
    System.out.printf("\n\nExecuting flink-jar-runner: %s\n\n", Arrays.toString(args));

    CommandLine cl = new CommandLine(new SqlRunner());
    int exitCode = cl.execute(args);
    SqlRunner runner = cl.getCommand();

    new FlinkMain(runner.sqlFile).run();
    System.out.println("Finished flink-jar-runner");
  }

  public TableResult run() throws Exception {
    Map<String, String> flinkConfig = new HashMap<>();
    flinkConfig.put("table.exec.source.idle-timeout", "100 ms");
    Configuration sEnvConfig = Configuration.fromMap(flinkConfig);
    StreamExecutionEnvironment sEnv =
        StreamExecutionEnvironment.getExecutionEnvironment(sEnvConfig);
    EnvironmentSettings tEnvConfig =
        EnvironmentSettings.newInstance()
            .withConfiguration(Configuration.fromMap(flinkConfig))
            .build();
    TableResult tableResult = null;
    String script = FileUtils.readFileUtf8(new File(sqlFile));

    // Initialize SqlExecutor
    SqlExecutor sqlExecutor = new SqlExecutor(sEnvConfig, null);

    Set<String> missingEnvironmentVariables =
        EnvironmentVariablesUtils.validateEnvironmentVariables(script);
    if (!missingEnvironmentVariables.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Could not find the following environment variables: %s",
              missingEnvironmentVariables));
    }

    tableResult = sqlExecutor.executeScript(script);

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
}
