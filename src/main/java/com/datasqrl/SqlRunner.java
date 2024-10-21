/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;
import picocli.CommandLine;
import picocli.CommandLine.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Main class for executing SQL scripts using picocli.
 */
@Command(name = "SqlRunner", mixinStandardHelpOptions = true, version = "1.0", description = "Runs SQL scripts using Flink TableEnvironment.")
@Slf4j
public class SqlRunner implements Callable<Integer> {

  @Option(names = {"-s", "--sqlfile"}, description = "SQL file to execute.")
  private File sqlFile;

  @Option(names = {"--block"}, description = "Wait for the flink job manager to exit.",
  defaultValue = "false")
  private boolean block;

  @Option(names = {"--planfile"}, description = "Compiled plan JSON file.")
  private File planFile;

  @Option(names = {"--configfile"}, description = "Configuration YAML file.")
  private File configFile;

  @Option(names = {"--udfpath"}, description = "Path to UDFs.")
  private String udfPath;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new SqlRunner()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    // Determine UDF path
    if (udfPath == null) {
      udfPath = System.getenv("UDF_PATH");
    }

    // Load configuration if configFile is provided
    Configuration configuration = new Configuration();
    if (configFile != null) {
      configuration = loadConfigurationFromYaml(configFile);
    }

    // Initialize SqlExecutor
    SqlExecutor sqlExecutor = new SqlExecutor(configuration, udfPath);
    TableResult tableResult;
    // Input validation and execution logic
    if (sqlFile != null) {
      // Single SQL file mode
      String script = FileUtils.readFileUtf8(sqlFile);
      tableResult = sqlExecutor.executeScript(script);
    } else if (planFile != null) {
      // Compiled plan JSON file
      String planJson = FileUtils.readFileUtf8(planFile);
      tableResult = sqlExecutor.executeCompiledPlan(planJson);
    } else {
      log.error("Invalid input. Please provide one of the following combinations:");
      log.error("- A single SQL file (--sqlfile)");
      log.error("- Schema SQL file (--schemafile) and workload SQL file (--workloadfile)");
      log.error("- A plan JSON file (--planfile)");
      return 1;
    }

    if (block) {
      tableResult.await();
    }

    return 0;
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
    Configuration configuration = GlobalConfiguration.loadConfiguration(configFile.getAbsolutePath());
    return configuration;
  }
}

/**
 * Class for executing SQL scripts programmatically.
 */
class SqlExecutor {

  private static final Logger log = LoggerFactory.getLogger(SqlExecutor.class);

  private static final Pattern SET_STATEMENT_PATTERN = Pattern.compile(
      "SET\\s+'(\\S+)'\\s*=\\s*'(.+)';?", Pattern.CASE_INSENSITIVE);

  private final TableEnvironment tableEnv;

  public SqlExecutor(Configuration configuration, String udfPath) {
    StreamExecutionEnvironment sEnv;
    try {
      sEnv = new StreamExecutionEnvironment(configuration);
    } catch (Exception e) {
      throw e;
    }

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(configuration)
//        .withClassLoader(udfClassLoader)
        .build();

    this.tableEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    // Apply configuration settings
    tableEnv.getConfig().addConfiguration(configuration);

//    if (udfPath != null) {
//      setupUdfPath(udfPath);
//    }
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
//
//    TableEnvironmentImpl tEnv1 = (TableEnvironmentImpl) tableEnv;
//
//    StatementSetOperation parse = (StatementSetOperation)tEnv1.getParser()
//        .parse(statements.get(statements.size()-1)).get(0);
//
//    CompiledPlan plan = tEnv1.compilePlan(parse.getOperations());
//    plan.writeToFile("/Users/henneberger/flink-jar-runner/src/test/resources/sql/compiled-plan.json");
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
        log.info("Executing statement:\n{}", statement);
        tableResult = tableEnv.executeSql(statement);
      }
    } catch (Exception e) {
      log.error("Failed to execute statement: {}", statement, e);
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

/**
 * Utility class for parsing SQL scripts.
 */
class SqlUtils {

  private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
  private static final String LINE_DELIMITER = "\n";

  private static final String COMMENT_PATTERN = "(--.*)|(((/\\*)+?[\\w\\W]+?(\\*/)+))";

  private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
  private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
  private static final String ESCAPED_BEGIN_CERTIFICATE = "======BEGIN CERTIFICATE=====";
  private static final String ESCAPED_END_CERTIFICATE = "=====END CERTIFICATE=====";

  /**
   * Parses SQL statements from a script.
   *
   * @param script The SQL script content.
   * @return A list of individual SQL statements.
   */
  public static List<String> parseStatements(String script) {
    String formatted = formatSqlFile(script).replaceAll(BEGIN_CERTIFICATE,
            ESCAPED_BEGIN_CERTIFICATE).replaceAll(END_CERTIFICATE, ESCAPED_END_CERTIFICATE)
        .replaceAll(COMMENT_PATTERN, "").replaceAll(ESCAPED_BEGIN_CERTIFICATE, BEGIN_CERTIFICATE)
        .replaceAll(ESCAPED_END_CERTIFICATE, END_CERTIFICATE);

    List<String> statements = new ArrayList<>();

    StringBuilder current = null;
    boolean statementSet = false;
    for (String line : formatted.split("\n")) {
      String trimmed = line.trim();
      if (trimmed.isBlank()) {
        continue;
      }
      if (current == null) {
        current = new StringBuilder();
      }
      if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
        statementSet = true;
      }
      current.append(trimmed);
      current.append("\n");
      if (trimmed.endsWith(STATEMENT_DELIMITER)) {
        if (!statementSet || trimmed.equalsIgnoreCase("END;")) {
          statements.add(current.toString());
          current = null;
          statementSet = false;
        }
      }
    }
    return statements;
  }

  /**
   * Formats the SQL file content to ensure proper statement termination.
   *
   * @param content The SQL file content.
   * @return Formatted SQL content.
   */
  public static String formatSqlFile(String content) {
    String trimmed = content.trim();
    StringBuilder formatted = new StringBuilder();
    formatted.append(trimmed);
    if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
      formatted.append(STATEMENT_DELIMITER);
    }
    formatted.append(LINE_DELIMITER);
    return formatted.toString();
  }
}
