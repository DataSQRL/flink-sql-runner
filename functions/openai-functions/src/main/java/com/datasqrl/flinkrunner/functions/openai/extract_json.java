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
package com.datasqrl.flinkrunner.functions.openai;

import com.google.auto.service.AutoService;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

@AutoService(AsyncScalarFunction.class)
public class extract_json extends AsyncScalarFunction {

  private transient OpenAICompletions openAICompletions;
  private transient FunctionExecutor executor;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.openAICompletions = createOpenAICompletions();
    this.executor = new FunctionExecutor(context, extract_json.class.getSimpleName());
  }

  protected OpenAICompletions createOpenAICompletions() {
    return new OpenAICompletions();
  }

  public void eval(CompletableFuture<String> result, String prompt, String modelName) {
    eval(result, prompt, modelName, null);
  }

  public void eval(
      CompletableFuture<String> result, String prompt, String modelName, Double temperature) {
    eval(result, prompt, modelName, temperature, null);
  }

  public void eval(
      CompletableFuture<String> result,
      String prompt,
      String modelName,
      Double temperature,
      Double topP) {
    eval(result, prompt, modelName, temperature, topP, null);
  }

  public void eval(
      CompletableFuture<String> result,
      String prompt,
      String modelName,
      Double temperature,
      Double topP,
      String jsonSchema) {
    final CompletionsRequest request =
        new CompletionsRequest.CompletionsRequestBuilder()
            .prompt(prompt)
            .modelName(modelName)
            .requireJsonOutput(true)
            .jsonSchema(jsonSchema)
            .temperature(temperature)
            .topP(topP)
            .build();

    executeRequest(result, request);
  }

  private void executeRequest(CompletableFuture<String> result, CompletionsRequest request) {
    executor
        .executeAsync(() -> openAICompletions.callCompletions(request))
        .thenAccept(result::complete)
        .exceptionally(
            ex -> {
              result.completeExceptionally(ex);
              return null;
            });
  }
}
