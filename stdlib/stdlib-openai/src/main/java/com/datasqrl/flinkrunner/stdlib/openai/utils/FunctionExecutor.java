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
package com.datasqrl.flinkrunner.stdlib.openai.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.flink.table.functions.FunctionContext;

/** Makes UDF execution simpler, and adjusts {@link FunctionMetricTracker} counters. */
public class FunctionExecutor {

  private static final String POOL_SIZE = "ASYNC_FUNCTION_THREAD_POOL_SIZE";

  private final String functionName;
  private final FunctionMetricTracker metricTracker;

  private ExecutorService executorService;

  public FunctionExecutor(FunctionContext context, String functionName) {
    this.functionName = functionName;
    metricTracker = new FunctionMetricTracker(context, functionName);
  }

  /** See {@link FunctionExecutor#execute(Callable, CompletableFuture)}. */
  public <T> T execute(Callable<T> func) {
    return execute(func, null);
  }

  /**
   * Executes the given {@link Callable}, which is expected to represent a UDF body. If {@code
   * resultFuture} is provided, the execution will be asynchronous; otherwise, it will be
   * synchronous.
   *
   * @param func the UDF body to execute
   * @param resultFuture a future to handle the output of asynchronous execution
   * @param <T> the return type of the UDF body
   * @return the result of {@code func} in case of synchronous execution, or {@code null} otherwise
   */
  public <T> T execute(Callable<T> func, @Nullable CompletableFuture<T> resultFuture) {
    initExecutorService();

    if (resultFuture == null) {
      return executeInternal(func);
    }

    executorService.submit(
        () -> {
          try {
            T result = executeInternal(func);
            resultFuture.complete(result);
          } catch (RuntimeException ex) {
            resultFuture.completeExceptionally(ex);
          }
        });

    return null;
  }

  private <T> T executeInternal(Callable<T> func) {
    try {
      metricTracker.increaseCallCount();
      final long start = System.nanoTime();

      final T result = func.call();

      final long elapsedTime = System.nanoTime() - start;
      metricTracker.recordLatency(TimeUnit.NANOSECONDS.toMillis(elapsedTime));

      return result;

    } catch (Exception ex) {
      metricTracker.increaseErrorCount();
      throw new RuntimeException("Failure occurred executing function: " + functionName, ex);
    }
  }

  private void initExecutorService() {
    if (executorService == null) {
      int poolSize = Integer.parseInt(System.getenv().getOrDefault(POOL_SIZE, "10"));

      executorService = Executors.newFixedThreadPool(poolSize);
    }
  }
}
