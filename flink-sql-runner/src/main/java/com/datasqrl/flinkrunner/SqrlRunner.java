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

import javax.annotation.Nullable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.TableResult;

/** Runner class specifically for SQRL test environments. */
public class SqrlRunner extends BaseRunner {

  public SqrlRunner(
      RuntimeExecutionMode mode,
      Configuration config,
      @Nullable EnvVarResolver resolver,
      @Nullable String sqlFile,
      @Nullable String planFile,
      @Nullable String udfPath) {
    super(mode, resolver, sqlFile, planFile, null, udfPath, config);
  }

  public TableResult run() throws Exception {
    FileSystem.initialize(config, null);
    var sqlExecutor = SqlExecutor.withUdfClassLoader(config, udfPath);

    return run(() -> sqlExecutor);
  }
}
