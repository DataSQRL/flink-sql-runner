/*
 * Copyright © 2025 DataSQRL (contact@datasqrl.com)
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

import com.datasqrl.flinkrunner.stdlib.utils.AutoRegisterSystemFunction;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.UserDefinedFunction;

/** Class for executing SQL scripts programmatically. */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class SqlExecutor {

  private static final Pattern SET_STATEMENT_PATTERN =
      Pattern.compile("SET\\s+'(\\S+)'\\s*=\\s*'(.+)';?", Pattern.CASE_INSENSITIVE);

  private final TableEnvironment tEnv;

  SqlExecutor(Configuration config, @Nullable String udfPath) {
    var sEnv = StreamExecutionEnvironment.getExecutionEnvironment(config);
    var tEnvConfig = EnvironmentSettings.newInstance().withConfiguration(config).build();

    tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    if (StringUtils.isNotBlank(udfPath)) {
      setupUdfPath(udfPath);
    }
  }

  /**
   * Dedicated to be used with {@link SqrlRunner}, which is used in the SQRL test env, where it is
   * necessary to add UDFs to the classpath due to the test env setup.
   *
   * @param config Flink configuration
   * @param udfPath path to a directory with UDF JAR(s)
   * @return a new {@link SqlExecutor} instance configured specifically to the SQRL test env
   * @throws IOException when UDF class loader creation fails
   */
  static SqlExecutor withUdfClassLoader(Configuration config, @Nullable String udfPath)
      throws IOException {
    var udfClassLoader = buildUdfClassLoader(udfPath);
    setConfigClassPaths(config, udfClassLoader);

    var sEnv = new StreamExecutionEnvironment(config, udfClassLoader);
    var tEnvConfig =
        EnvironmentSettings.newInstance()
            .withConfiguration(config)
            .withClassLoader(udfClassLoader)
            .build();

    return new SqlExecutor(StreamTableEnvironment.create(sEnv, tEnvConfig));
  }

  void setupSystemFunctions() {
    log.debug("Setting up automatically registered system functions");

    try {
      ServiceLoader<AutoRegisterSystemFunction> standardLibraryFunctions =
          ServiceLoader.load(AutoRegisterSystemFunction.class);

      standardLibraryFunctions.forEach(
          function ->
              getFunctionNameAndClass(function.getClass())
                  .ifPresent(
                      funcParts -> tEnv.createTemporarySystemFunction(funcParts.f0, funcParts.f1)));
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    log.debug("Completed auto function registered system functions");
  }

  /**
   * Executes a single SQL script.
   *
   * @param script The SQL script content.
   * @return
   */
  TableResult executeScript(String script) {
    var statements = SqlUtils.parseStatements(script);
    TableResult tableResult = null;
    for (String statement : statements) {
      tableResult = executeStatement(statement);
    }

    return tableResult;
  }

  /**
   * Executes a compiled plan from JSON.
   *
   * @param planJson The JSON content of the compiled plan.
   * @return
   */
  TableResult executeCompiledPlan(String planJson) {
    log.info("Executing compiled plan from JSON.");
    try {
      var planReference = PlanReference.fromJsonString(planJson);
      var result = tEnv.executePlan(planReference);
      log.info("Compiled plan executed.");
      return result;
    } catch (Exception e) {
      log.error("Failed to execute compiled plan", e);
      throw e;
    }
  }

  private TableResult executeStatement(String statement) {
    TableResult tableResult = null;
    try {
      var setMatcher = SET_STATEMENT_PATTERN.matcher(statement.trim());

      if (setMatcher.matches()) {
        // Handle SET statements
        var key = setMatcher.group(1);
        var value = setMatcher.group(2);
        tEnv.getConfig().getConfiguration().setString(key, value);
        log.info("Set configuration: {} = {}", key, value);
      } else {
        log.info("Executing statement:\n{}", statement);
        tableResult = tEnv.executeSql(statement);
      }
    } catch (Exception e) {
      e.addSuppressed(new RuntimeException("Error while executing stmt: " + statement));
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
      var udfDir = new File(udfPath);
      if (udfDir.exists() && udfDir.isDirectory()) {
        var jarFiles = udfDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jarFiles != null) {
          for (File jarFile : jarFiles) {
            tEnv.executeSql("ADD JAR 'file://" + jarFile.getAbsolutePath() + "'");
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

  @Nullable
  static URLClassLoader buildUdfClassLoader(String udfPath) throws IOException {
    if (StringUtils.isBlank(udfPath)) {
      return null;
    }

    var udfDir = new File(udfPath);
    if (!udfDir.exists() || !udfDir.isDirectory()) {
      return null;
    }

    var jarFiles = udfDir.listFiles((dir, name) -> name.endsWith(".jar"));
    var jarUrls = new URL[0];
    if (jarFiles != null) {
      jarUrls = new URL[jarFiles.length];

      for (int i = 0; i < jarFiles.length; i++) {
        jarUrls[i] = jarFiles[i].toURI().toURL();
      }
    }

    return jarUrls.length < 1
        ? null
        : new URLClassLoader(jarUrls, Thread.currentThread().getContextClassLoader());
  }

  static void setConfigClassPaths(Configuration config, @Nullable URLClassLoader udfClassLoader) {
    if (udfClassLoader == null || udfClassLoader.getURLs().length < 1) {
      return;
    }

    var joinedUrls =
        Arrays.stream(udfClassLoader.getURLs()).map(URL::toString).collect(Collectors.toList());
    config.set(PipelineOptions.CLASSPATHS, joinedUrls);
  }

  @VisibleForTesting
  static Optional<Tuple2<String, Class<? extends UserDefinedFunction>>> getFunctionNameAndClass(
      Class<?> clazz) {
    Tuple2<String, Class<? extends UserDefinedFunction>> res = null;

    if (UserDefinedFunction.class.isAssignableFrom(clazz)) {
      var funcName = clazz.getSimpleName().toLowerCase();
      res = Tuple2.of(funcName, (Class<? extends UserDefinedFunction>) clazz);

      log.info("Registering '{}' ...", funcName);

    } else {
      log.warn(
          "Skip registering '{}' as it does not extend '{}'", clazz, UserDefinedFunction.class);
    }

    return Optional.ofNullable(res);
  }
}
