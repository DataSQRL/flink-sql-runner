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
package com.datasqrl;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.util.FileUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@RequiredArgsConstructor
public class FlinkMain {

  @SuppressWarnings("unused")
  @Command(
      name = "SqlRunner",
      mixinStandardHelpOptions = true,
      version = "0.6",
      description = "Runs SQL scripts using Flink TableEnvironment.")
  public static class SqlRunner implements Callable<Void> {

    @Option(
        names = {"-m", "--mode"},
        defaultValue = "STREAMING",
        description =
            "Flink runtime execution mode to apply to the given SQL program. Valid values: ${COMPLETION-CANDIDATES}.")
    private RuntimeExecutionMode mode;

    @Option(
        names = {"-s", "--sqlfile"},
        description = "SQL file to execute.")
    private String sqlFile;

    @Option(
        names = {"-p", "--planfile"},
        description = "Compiled plan JSON file.")
    private String planFile;

    @Option(
        names = {"-c", "--config-dir"},
        description = "Directory containing configuration YAML file.")
    private String configDir;

    @Option(
        names = {"-u", "--udfpath"},
        description = "Path to UDFs.")
    private String udfPath;

    @Override
    public Void call() {
      return null;
    }
  }

  private final RuntimeExecutionMode mode;
  @Nullable private final String sqlFile;
  @Nullable private final String planFile;
  @Nullable private final String configDir;
  @Nullable private final String udfPath;

  public static void main(String[] args) throws Exception {
    System.out.printf("\n\nExecuting flink-sql-runner: %s\n\n", Arrays.toString(args));

    var cl = new CommandLine(new SqlRunner());
    var resCode = cl.execute(args);
    if (resCode != 0) {
      System.exit(resCode);
    }

    if (cl.isUsageHelpRequested()) {
      return;
    }

    SqlRunner runner = cl.getCommand();

    // Determine UDF path
    if (runner.udfPath == null) {
      runner.udfPath = System.getenv("UDF_PATH");
    }

    new FlinkMain(runner.mode, runner.sqlFile, runner.planFile, runner.configDir, runner.udfPath)
        .run();

    System.out.println("Finished flink-sql-runner");
  }

  public void run() throws Exception {
    var conf = initConfiguration();
    var exitCode = run(() -> new SqlExecutor(conf, udfPath));
    if (exitCode != 0) {
      System.exit(exitCode);
    }
  }

  int run(Supplier<SqlExecutor> sqlExecutorSupplier) throws Exception {
    var sqlExecutor = sqlExecutorSupplier.get();

    if (StringUtils.isNoneBlank(sqlFile, planFile)) {
      System.err.println("Provide either a SQL file or a compiled plan - not both.");
      return 1;
    }

    if (StringUtils.isAllBlank(sqlFile, planFile)) {
      System.err.println("Invalid input. Please provide one of the following combinations:");
      System.err.println("- A single SQL file (--sqlfile)");
      System.err.println("- A plan JSON file (--planfile)");
      return 2;
    }

    if (StringUtils.isNotBlank(sqlFile)) {
      // Single SQL file mode
      var script = FileUtils.readFileUtf8(new File(sqlFile));
      script = EnvVarUtils.resolveEnvVars(script);

      sqlExecutor.setupSystemFunctions();
      sqlExecutor.executeScript(script);

    } else {
      // Compiled plan JSON file
      var planJson = FileUtils.readFileUtf8(new File(planFile));
      planJson = EnvVarUtils.resolveEnvVarsInJson(planJson);

      sqlExecutor.setupSystemFunctions();
      sqlExecutor.executeCompiledPlan(planJson);
    }

    return 0;
  }

  /**
   * Initializes Flink {@link Configuration}. Loads configuration from YAML file, if given. Sets
   * {@link RuntimeExecutionMode} based on the given CLI option, if it was not part of the YAML
   * config.
   *
   * @return the initialized Flink {@link Configuration} object
   */
  Configuration initConfiguration() {
    var conf = new Configuration();
    if (StringUtils.isNotBlank(configDir)) {
      System.out.printf("Loading configuration from %s\n", configDir);
      conf = GlobalConfiguration.loadConfiguration(configDir);
    }

    // Do not overwrite runtime given in YAML
    if (!conf.contains(ExecutionOptions.RUNTIME_MODE)) {
      conf.set(ExecutionOptions.RUNTIME_MODE, mode);
    }

    return conf;
  }
}
