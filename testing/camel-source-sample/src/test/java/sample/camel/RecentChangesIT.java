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
package sample.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datasqrl.flinkrunner.connector.camel.source.cursor.CamelCursorSource;
import java.time.Duration;
import java.util.Map;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.Test;

class RecentChangesIT {

  private static final int TOO_MANY_REQUESTS = 429;
  private static final String RETRY_AFTER_SECONDS = "30";

  /**
   * The fetcher thread must not sleep through a Retry-After inside one request: Flink gives up
   * waiting for it on cancel after 30 seconds. The source's own retry backs off instead.
   */
  @Test
  void tooManyRequestsFailsWithoutWaitingForRetryAfter() throws Exception {
    try (TestHttpServer api =
        new TestHttpServer(
            "/w/api.php",
            request ->
                new TestHttpServer.Response(
                    TOO_MANY_REQUESTS, "", Map.of("Retry-After", RETRY_AFTER_SECONDS)))) {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(1);
      env.fromSource(
              CamelCursorSource.builder(new RecentChanges(api.url("/w/api.php")))
                  .fetchTimeout(Duration.ofMillis(100))
                  .bounded(true)
                  .build(),
              WatermarkStrategy.noWatermarks(),
              "recent-changes")
          .print();

      long start = System.nanoTime();
      assertThatThrownBy(env::execute)
          .satisfies(
              e ->
                  assertThat(
                          ExceptionUtils.findThrowable(e, HttpOperationFailedException.class)
                              .map(HttpOperationFailedException::getStatusCode))
                      .contains(TOO_MANY_REQUESTS));
      Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

      assertThat(elapsed).isLessThan(Duration.ofSeconds(Long.parseLong(RETRY_AFTER_SECONDS)));
      assertThat(api.requests()).hasSize(1);
    }
  }
}
