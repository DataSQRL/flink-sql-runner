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
package com.datasqrl.openai;

import com.datasqrl.openai.util.FunctionMetricTracker;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.functions.FunctionContext;

public class FunctionExecutor {

  private static final String POOL_SIZE = "ASYNC_FUNCTION_THREAD_POOL_SIZE";

  private final FunctionMetricTracker metricTracker;
  private final ExecutorService executorService;

  public FunctionExecutor(FunctionContext context, String functionName) {
    this.metricTracker = new FunctionMetricTracker(context, functionName);
    this.executorService = Executors.newFixedThreadPool(getPoolSize());
  }

  public <T> CompletableFuture<T> executeAsync(Callable<T> task) {
    final CompletableFuture<T> future = new CompletableFuture<>();
    executorService.submit(
        () -> {
          try {
            metricTracker.increaseCallCount();
            final long start = System.nanoTime();

            final T result = task.call();

            final long elapsedTime = System.nanoTime() - start;
            metricTracker.recordLatency(TimeUnit.NANOSECONDS.toMillis(elapsedTime));

            future.complete(result);
          } catch (Exception e) {
            metricTracker.increaseErrorCount();
            future.completeExceptionally(e);
          }
        });
    return future;
  }

  private int getPoolSize() {
    return Integer.parseInt(System.getenv().getOrDefault(POOL_SIZE, "10"));
  }
}
