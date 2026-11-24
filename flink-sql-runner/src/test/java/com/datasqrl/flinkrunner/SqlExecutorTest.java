/*
 * Copyright © 2026 DataSQRL (contact@datasqrl.com)
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import com.datasqrl.flinkrunner.utils.EnvVarResolver;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sample.Upper;

class SqlExecutorTest {

  private static final String DB_PASSWORD = "s3cr3t--value";
  private static final EnvVarResolver RESOLVER =
      EnvVarResolver.builder()
          .envVars(Map.of("DB_PASSWORD", DB_PASSWORD, "JOB_NAME", "nightly-job"))
          .build();

  @Test
  void testExecuteScriptWithSetAndSQL() {
    TableEnvironment mockTableEnv = mock(TableEnvironment.class);
    TableConfig mockTableConfig = mock(TableConfig.class);
    Configuration mockConfig = mock(Configuration.class);
    TableResult mockResult = mock(TableResult.class);

    when(mockTableEnv.getConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.getConfiguration()).thenReturn(mockConfig);
    when(mockTableEnv.executeSql("SELECT 1;\n")).thenReturn(mockResult);

    SqlExecutor executor = new SqlExecutor(mockTableEnv);

    String script = "SET 'execution.runtime-mode' = 'streaming';\nSELECT 1;";
    TableResult result = executor.executeScript(script, UnaryOperator.identity());

    assertThat(result).isSameAs(mockResult);

    verify(mockTableEnv).getConfig();
    verify(mockTableConfig).getConfiguration();
    verify(mockConfig).setString("execution.runtime-mode", "streaming");
    verify(mockTableEnv).executeSql("SELECT 1;\n");
  }

  @Test
  void executeScript_runsResolvedStatement_butLogsItAsWritten() {
    var tEnv = mock(TableEnvironment.class);
    var executor = new SqlExecutor(tEnv);
    var script = "CREATE TABLE t (id INT) WITH ('password' = '${DB_PASSWORD}');";

    try (var logs = LogCapture.of(SqlExecutor.class)) {
      executor.executeScript(script, RESOLVER::resolve);

      // The "--" in the value would have been stripped as a comment if the script had been
      // resolved before it was split into statements.
      verify(tEnv)
          .executeSql("CREATE TABLE t (id INT) WITH ('password' = '" + DB_PASSWORD + "');\n");
      assertThat(logs.messages())
          .anyMatch(message -> message.contains("'password' = '${DB_PASSWORD}'"))
          .noneMatch(message -> message.contains(DB_PASSWORD));
    }
  }

  @Test
  void executeScript_masksSensitiveSetValues_andLogsOthersAsWritten() {
    var tEnv = mock(TableEnvironment.class);
    var tableConfig = mock(TableConfig.class);
    var config = new Configuration();
    when(tEnv.getConfig()).thenReturn(tableConfig);
    when(tableConfig.getConfiguration()).thenReturn(config);
    var executor = new SqlExecutor(tEnv);
    var script =
        """
        SET 'properties.sasl.jaas.config' = 'ScramLoginModule required password="${DB_PASSWORD}";';
        SET 's3.secret-key' = 'literal-secret-key';
        SET 'pipeline.name' = '${JOB_NAME}';
        """;

    try (var logs = LogCapture.of(SqlExecutor.class)) {
      executor.executeScript(script, RESOLVER::resolve);

      assertThat(config.toMap())
          .containsEntry(
              "properties.sasl.jaas.config",
              "ScramLoginModule required password=\"" + DB_PASSWORD + "\";")
          .containsEntry("s3.secret-key", "literal-secret-key")
          .containsEntry("pipeline.name", "nightly-job");
      assertThat(logs.messages())
          .contains(
              "Set configuration: properties.sasl.jaas.config = ******",
              "Set configuration: s3.secret-key = ******",
              "Set configuration: pipeline.name = ${JOB_NAME}")
          .noneMatch(message -> message.contains(DB_PASSWORD))
          .noneMatch(message -> message.contains("literal-secret-key"));
    }
  }

  @Test
  void executeScript_reportsFailingStatementAsWritten() {
    var tEnv = mock(TableEnvironment.class);
    when(tEnv.executeSql(anyString())).thenThrow(new ValidationException("invalid table"));
    var executor = new SqlExecutor(tEnv);
    var script = "CREATE TABLE t (id INT) WITH ('password' = '${DB_PASSWORD}');";

    assertThatThrownBy(() -> executor.executeScript(script, RESOLVER::resolve))
        .hasMessageContaining("'password' = '${DB_PASSWORD}'")
        .hasMessageNotContaining(DB_PASSWORD);
  }

  @Test
  void executeScript_failsBeforeAnyStatementRuns_ifEnvVarIsMissing() {
    var tEnv = mock(TableEnvironment.class);
    var executor = new SqlExecutor(tEnv);
    var script =
        """
        CREATE TABLE a (id INT);
        CREATE TABLE b (id INT) WITH ('password' = '${MISSING_PASSWORD}');
        """;

    assertThatThrownBy(() -> executor.executeScript(script, RESOLVER::resolve))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("MISSING_PASSWORD");
    verifyNoInteractions(tEnv);
  }

  @Test
  void testExecuteCompiledPlanDelegatesToTableEnv() {
    TableEnvironment tEnv = mock(TableEnvironment.class);
    SqlExecutor executor = new SqlExecutor(tEnv);

    String planJson = "{\"flinkVersion\":\"1.19\"}"; // Dummy
    TableResult mockResult = mock(TableResult.class);

    when(tEnv.executePlan(any())).thenReturn(mockResult);

    TableResult result = executor.executeCompiledPlan(planJson);

    assertThat(result).isSameAs(mockResult);
    verify(tEnv).executePlan(any());
  }

  @Test
  void testGetFunctionNameAndClass_FunctionClass() {
    var res = SqlExecutor.getFunctionNameAndClass(Upper.class);

    assertThat(res)
        .isPresent()
        .get()
        .satisfies(
            tuple -> {
              assertThat(tuple.f0).isEqualTo("upper");
              assertThat(tuple.f1).isEqualTo(Upper.class);
            });
  }

  @Test
  void testGetFunctionNameAndClass_NonFunctionClass() {
    var result = SqlExecutor.getFunctionNameAndClass(Object.class);

    assertThat(result).isEmpty();
  }

  @Test
  void testBuildUdfClassLoader_returnsNullIfNoJars(@TempDir File tempDir) throws Exception {
    File nonJarFile = new File(tempDir, "not-a-jar.txt");
    nonJarFile.createNewFile();

    URLClassLoader loader = SqlExecutor.buildUdfClassLoader(tempDir.getAbsolutePath());
    assertThat(loader).isNull();
  }

  @Test
  void testBuildUdfClassLoader_returnsLoaderIfJarsPresent(@TempDir File tempDir) throws Exception {
    File dummyJar = new File(tempDir, "udf.jar");
    dummyJar.createNewFile();

    URLClassLoader loader = SqlExecutor.buildUdfClassLoader(tempDir.getAbsolutePath());
    assertThat(loader).isNotNull();
    assertThat(loader.getURLs()).anyMatch(url -> url.toString().endsWith("udf.jar"));
  }

  @Test
  void testSetConfigClassPaths_setsClasspathOption() throws Exception {
    Configuration config = new Configuration();

    URL jarUrl = new URL("file:/test.jar");
    URLClassLoader loader = new URLClassLoader(new URL[] {jarUrl});

    SqlExecutor.setConfigClassPaths(config, loader);

    List<String> paths = config.get(PipelineOptions.CLASSPATHS);
    assertThat(paths).contains("file:/test.jar");
  }

  /** Collects the messages one logger writes while the capture is open. */
  private static class LogCapture extends AbstractAppender implements AutoCloseable {

    private final Logger logger;
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private LogCapture(Logger logger) {
      super("LogCapture", null, null, true, Property.EMPTY_ARRAY);
      this.logger = logger;
    }

    static LogCapture of(Class<?> clazz) {
      var capture = new LogCapture((Logger) LogManager.getLogger(clazz));
      capture.start();
      capture.logger.addAppender(capture);
      return capture;
    }

    @Override
    public void append(LogEvent event) {
      messages.add(event.getMessage().getFormattedMessage());
    }

    List<String> messages() {
      return messages;
    }

    @Override
    public void close() {
      logger.removeAppender(this);
      stop();
    }
  }
}
