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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;

@Slf4j
public class CommandLineUtil {

  public static String execute(String command) throws ExecuteException {
    return execute(Path.of("."), command);
  }

  public static String execute(Path workDir, String command) throws ExecuteException {
    command = command.replaceAll("\\s+", " ").trim();

    log.info("Executing command: {}", command);

    var cmdLine = CommandLine.parse(command);

    var output = new ByteArrayOutputStream();
    var streamHandler = new PumpStreamHandler(output);

    var executor =
        DefaultExecutor.builder()
            .setWorkingDirectory(workDir.toFile())
            .setExecuteStreamHandler(streamHandler)
            .get();
    executor.setExitValue(0);

    var watchdog = ExecuteWatchdog.builder().setTimeout(Duration.ofMinutes(5)).get();
    executor.setWatchdog(watchdog);

    try {
      var exitValue =
          // This call is synchronous and will block until the command completes
          executor.execute(cmdLine);
      log.info("Installation completed successfully with exit code: {}", exitValue);

      return output.toString();
    } catch (IOException e) {
      var result = output.toString();
      log.error("Error while executing command:\n{}\noutput:\n{}", command, result);
      if (e instanceof ExecuteException) {
        ExecuteException ee = (ExecuteException) e;
        throw new ExecuteException(result, ee.getExitValue(), ee);
      }
      throw new RuntimeException(e);
    }
  }

  private CommandLineUtil() {
    throw new UnsupportedOperationException();
  }
}
