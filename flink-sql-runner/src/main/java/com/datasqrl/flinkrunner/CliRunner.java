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

import java.util.Arrays;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.RuntimeExecutionMode;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
public class CliRunner extends BaseRunner {

  @SuppressWarnings("unused")
  @Command(
      name = "sql-runner",
      version = "0.9",
      description = "Runs SQL scripts using Flink TableEnvironment.",
      mixinStandardHelpOptions = true)
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

  public CliRunner(
      RuntimeExecutionMode mode,
      @Nullable String sqlFile,
      @Nullable String planFile,
      @Nullable String configDir,
      @Nullable String udfPath) {
    this(mode, new EnvVarResolver(), sqlFile, planFile, configDir, udfPath);
  }

  @VisibleForTesting
  CliRunner(
      RuntimeExecutionMode mode,
      EnvVarResolver resolver,
      @Nullable String sqlFile,
      @Nullable String planFile,
      @Nullable String configDir,
      @Nullable String udfPath) {
    super(mode, resolver, sqlFile, planFile, configDir, udfPath, null);
  }

  public static void main(String[] args) throws Exception {
    log.info("Executing flink-sql-runner: {}", Arrays.toString(args));

    var cmd = new CommandLine(new SqlRunner());
    cmd.setUnmatchedArgumentsAllowed(true);

    var resCode = cmd.execute(args);
    if (resCode != 0) {
      System.exit(resCode);
    }

    if (cmd.isUsageHelpRequested()) {
      return;
    }

    SqlRunner runner = cmd.getCommand();

    // Determine UDF path
    if (runner.udfPath == null) {
      runner.udfPath = System.getenv("UDF_PATH");
    }

    new CliRunner(runner.mode, runner.sqlFile, runner.planFile, runner.configDir, runner.udfPath)
        .run();

    log.info("Finished flink-sql-runner execution");
  }
}
