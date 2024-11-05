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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@RequiredArgsConstructor
public class FlinkMain {

  private final String sqlFile;

  public static void main(String[] args) {
    System.out.println("Executing flink-jar-runner ");
    new FlinkMain("/opt/flink/usrlib/flink-files/flink.sql").run();
    System.out.println("Finished flink-jar-runner");
  }

  public TableResult run() {
    Map<String, String> flinkConfig = new HashMap<>();
    flinkConfig.put("table.exec.source.idle-timeout", "100 ms");
    Configuration sEnvConfig = Configuration.fromMap(flinkConfig);
    StreamExecutionEnvironment sEnv =
        StreamExecutionEnvironment.getExecutionEnvironment(sEnvConfig);
    EnvironmentSettings tEnvConfig =
        EnvironmentSettings.newInstance()
            .withConfiguration(Configuration.fromMap(flinkConfig))
            .build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;
    String[] statements = readResourceFile(sqlFile).split("\n\n");
    for (String statement : statements) {
      if (statement.trim().isEmpty()) continue;
      tableResult = tEnv.executeSql(replaceWithEnv(statement));
      System.out.println(statement);
      System.out.println("");
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

  private static String readResourceFile(String fileName) {
    try (InputStream inputStream = Files.newInputStream(Path.of(fileName));
        BufferedReader reader =
            new java.io.BufferedReader(
                new java.io.InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      return reader.lines().collect(Collectors.joining("\n"));
    } catch (IOException | NullPointerException e) {
      System.err.println("Error reading the resource file: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
