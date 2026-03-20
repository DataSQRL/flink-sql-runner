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

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sample.Upper;

class SqlExecutorTest {

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
    TableResult result = executor.executeScript(script);

    assertThat(result).isSameAs(mockResult);

    verify(mockTableEnv).getConfig();
    verify(mockTableConfig).getConfiguration();
    verify(mockConfig).setString("execution.runtime-mode", "streaming");
    verify(mockTableEnv).executeSql("SELECT 1;\n");
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

  @Test
  void given_awaitWorks_when_awaitCompletion_then_usesAwaitDirectly() throws Exception {
    TableResult tableResult = mock(TableResult.class);

    SqlExecutor.awaitCompletion(tableResult);

    verify(tableResult).await();
    verify(tableResult, never()).getJobClient();
  }

  @Test
  void given_awaitThrows_when_awaitCompletion_then_fallsBackToPolling() throws Exception {
    TableResult tableResult = mock(TableResult.class);
    JobClient jobClient = mock(JobClient.class);

    doThrow(new RuntimeException("Web Submission does not support await"))
        .when(tableResult)
        .await();
    when(tableResult.getJobClient()).thenReturn(Optional.of(jobClient));
    when(jobClient.getJobID()).thenReturn(new JobID());
    when(jobClient.getJobStatus())
        .thenReturn(CompletableFuture.completedFuture(JobStatus.RUNNING))
        .thenReturn(CompletableFuture.completedFuture(JobStatus.FINISHED));

    SqlExecutor.awaitCompletion(tableResult);

    verify(tableResult).getJobClient();
    verify(jobClient, atLeast(2)).getJobStatus();
  }

  @Test
  void given_jobFails_when_awaitCompletion_then_throwsRuntimeException() throws Exception {
    TableResult tableResult = mock(TableResult.class);
    JobClient jobClient = mock(JobClient.class);
    var jobId = new JobID();

    doThrow(new RuntimeException("Web Submission does not support await"))
        .when(tableResult)
        .await();
    when(tableResult.getJobClient()).thenReturn(Optional.of(jobClient));
    when(jobClient.getJobID()).thenReturn(jobId);
    when(jobClient.getJobStatus()).thenReturn(CompletableFuture.completedFuture(JobStatus.FAILED));

    assertThatThrownBy(() -> SqlExecutor.awaitCompletion(tableResult))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("failed during execution");
  }

  @Test
  void given_multipleStatementSets_when_executeScript_then_bothExecuted() throws Exception {
    TableEnvironment mockTableEnv = mock(TableEnvironment.class);
    TableResult firstResult = mock(TableResult.class);
    TableResult secondResult = mock(TableResult.class);
    JobClient jobClient = mock(JobClient.class);

    when(mockTableEnv.executeSql(contains("OutA"))).thenReturn(firstResult);
    when(mockTableEnv.executeSql(contains("OutB"))).thenReturn(secondResult);
    when(firstResult.getJobClient()).thenReturn(Optional.of(jobClient));
    when(jobClient.getJobID()).thenReturn(new JobID());
    when(jobClient.getJobStatus())
        .thenReturn(CompletableFuture.completedFuture(JobStatus.FINISHED));
    doThrow(new RuntimeException("Web Submission does not support await"))
        .when(firstResult)
        .await();

    SqlExecutor executor = new SqlExecutor(mockTableEnv);

    String script =
        "EXECUTE STATEMENT SET BEGIN\n"
            + "INSERT INTO OutA SELECT * FROM InA;\n"
            + "END;\n"
            + "EXECUTE STATEMENT SET BEGIN\n"
            + "INSERT INTO OutB SELECT * FROM InB;\n"
            + "END";

    TableResult result = executor.executeScript(script);

    assertThat(result).isSameAs(secondResult);
    verify(mockTableEnv).executeSql(contains("OutA"));
    verify(mockTableEnv).executeSql(contains("OutB"));
  }
}
