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
package com.datasqrl.flinkrunner.stdlib.openai;

import com.datasqrl.flinkrunner.stdlib.openai.util.FunctionMetricTracker;
import com.google.auto.service.AutoService;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

@AutoService(AsyncScalarFunction.class)
public class vector_embed extends AsyncScalarFunction {

  private transient OpenAIEmbeddings openAIEmbeddings;
  private transient FunctionExecutor executor;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.openAIEmbeddings = createOpenAIEmbeddings();
    this.executor = new FunctionExecutor(context, vector_embed.class.getSimpleName());
  }

  protected OpenAIEmbeddings createOpenAIEmbeddings() {
    return new OpenAIEmbeddings();
  }

  protected FunctionMetricTracker createMetricTracker(
      FunctionContext context, String functionName) {
    return new FunctionMetricTracker(context, functionName);
  }

  public void eval(CompletableFuture<double[]> result, String text, String modelName) {
    executor
        .executeAsync(() -> openAIEmbeddings.vectorEmbed(text, modelName))
        .thenAccept(result::complete)
        .exceptionally(
            ex -> {
              result.completeExceptionally(ex);
              return null;
            });
  }
}
