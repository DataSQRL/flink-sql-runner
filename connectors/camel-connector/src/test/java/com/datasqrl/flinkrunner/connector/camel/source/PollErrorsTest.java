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
package com.datasqrl.flinkrunner.connector.camel.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import org.apache.camel.RuntimeCamelException;
import org.junit.jupiter.api.Test;

class PollErrorsTest {

  @Test
  void failsAfterMaxAttemptsInARowAndResetsOnSuccess() throws Exception {
    PollErrors errors =
        new PollErrors("sftp://u@h/in?password=secret", 3, Duration.ZERO, Duration.ZERO);
    IllegalStateException cause = new IllegalStateException("login failed");

    // Camel must not retry within the poll; the backoff decides when the next attempt is.
    assertFalse(errors.rollback(null, null, 0, cause));
    errors.failed(cause);
    errors.checkAttempts();

    // A successful poll, even an empty one, starts the count again.
    errors.commit(null, null, 0);
    assertEquals(0, errors.consecutiveFailures());

    errors.failed(cause);
    errors.failed(cause);
    errors.checkAttempts();
    errors.failed(cause);
    IOException giveUp = assertThrows(IOException.class, errors::checkAttempts);
    assertSame(cause, giveUp.getCause());
    assertTrue(giveUp.getMessage().contains("after 3 failed attempts"), giveUp.getMessage());
    assertFalse(giveUp.getMessage().contains("secret"), "password leaked: " + giveUp.getMessage());
  }

  @Test
  void backsOffExponentiallyUpToTheMax() {
    PollErrors errors =
        new PollErrors("file:/in", 100, Duration.ofSeconds(10), Duration.ofSeconds(25));
    assertTrue(errors.begin(null, null));

    errors.failed(new RuntimeException());
    assertFalse(errors.begin(null, null), "polls are skipped while backing off");
    assertFalse(errors.attemptAllowed());

    errors.succeeded();
    assertTrue(errors.begin(null, null));
  }

  @Test
  void doesNotCountARethrownPollFailureTwice() {
    PollErrors errors = new PollErrors("file:/in", 10, Duration.ZERO, Duration.ZERO);
    Exception cause = new Exception("directory does not exist");
    errors.rollback(null, null, 0, cause);
    // File endpoints poll on the receiving thread and rethrow the failure, wrapped.
    errors.receiveFailed(RuntimeCamelException.wrapRuntimeCamelException(cause));
    assertEquals(1, errors.consecutiveFailures());

    errors.receiveFailed(new IllegalStateException("broker down"));
    assertEquals(2, errors.consecutiveFailures());
  }
}
