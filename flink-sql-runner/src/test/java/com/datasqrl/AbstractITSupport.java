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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;

@Slf4j
public class AbstractITSupport {

  protected static FlinkApi client;

  @BeforeAll
  static void waitServiceStart() throws ApiException {
    client = new FlinkApi();
    client.getApiClient().setBasePath(serverUrl());

    var timeout = (int) TimeUnit.MINUTES.toMillis(2);
    client
        .getApiClient()
        .setHttpClient(
            client
                .getApiClient()
                .getHttpClient()
                .newBuilder()
                .connectTimeout(Duration.ofMinutes(2))
                .writeTimeout(Duration.ofMinutes(2))
                .readTimeout(Duration.ofMinutes(2))
                .build());

    await()
        .atMost(100, SECONDS)
        .pollInterval(500, MILLISECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              log.info("Awaiting for custody-api");
              return client.getJobsOverview() != null;
            });

    final var statusOverview = client.getJobIdsWithStatusesOverview();
    statusOverview
        .getJobs()
        .forEach(
            jobIdWithStatus -> {
              try {
                client.cancelJob(jobIdWithStatus.getId(), TerminationMode.CANCEL);
              } catch (ApiException ignored) {
              }
            });
  }

  protected static String serverUrl() {
    var serverPort = Optional.ofNullable(System.getProperty("server.port")).orElse("8081");
    return String.format("http://localhost:%s", serverPort);
  }
}
