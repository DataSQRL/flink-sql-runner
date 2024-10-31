package com.datasqrl;

import java.io.File;
import java.util.List;
import java.util.Map;
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
    StreamExecutionEnvironment sEnv;
    try {
      sEnv = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    } catch (Exception e) {
      throw e;
    }

    EnvironmentSettings tEnvConfig =
        EnvironmentSettings.newInstance().withConfiguration(configuration).build();

    this.tableEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    // Apply configuration settings
    tableEnv.getConfig().addConfiguration(configuration);

    if (udfPath != null) {
      setupUdfPath(udfPath);
    }
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
    //
    // plan.writeToFile("/Users/henneberger/flink-jar-runner/src/test/resources/sql/compiled-plan-udf.json");

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
        tableResult = tableEnv.executeSql(replaceWithEnv(statement));
      }
    } catch (Exception e) {
      log.error("Failed to execute statement: {}", statement, e);
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
