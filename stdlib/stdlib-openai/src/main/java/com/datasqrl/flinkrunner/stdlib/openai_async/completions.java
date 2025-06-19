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
package com.datasqrl.flinkrunner.stdlib.openai_async;

import com.datasqrl.flinkrunner.stdlib.openai.OpenAiCompletions;
import com.datasqrl.flinkrunner.stdlib.openai.utils.FunctionExecutor;
import com.google.auto.service.AutoService;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;

@AutoService(AsyncScalarFunction.class)
public class completions extends AsyncScalarFunction {

  private transient OpenAiCompletions openAiCompletions;
  private transient FunctionExecutor executor;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.openAiCompletions = createOpenAICompletions();
    this.executor = new FunctionExecutor(context, completions.class.getSimpleName());
  }

  protected OpenAiCompletions createOpenAICompletions() {
    return new OpenAiCompletions();
  }

  public void eval(CompletableFuture<String> result, String prompt, String modelName) {
    eval(result, prompt, modelName, null, null, null);
  }

  public void eval(
      CompletableFuture<String> result, String prompt, String modelName, Integer maxOutputTokens) {
    eval(result, prompt, modelName, maxOutputTokens, null, null);
  }

  public void eval(
      CompletableFuture<String> result,
      String prompt,
      String modelName,
      Integer maxOutputTokens,
      Double temperature) {
    eval(result, prompt, modelName, maxOutputTokens, temperature, null);
  }

  public void eval(
      CompletableFuture<String> result,
      String prompt,
      String modelName,
      Integer maxOutputTokens,
      Double temperature,
      Double topP) {

    final OpenAiCompletions.CompletionsRequest request =
        OpenAiCompletions.CompletionsRequest.builder()
            .prompt(prompt)
            .modelName(modelName)
            .maxOutputTokens(maxOutputTokens)
            .temperature(temperature)
            .topP(topP)
            .build();

    executor.execute(() -> openAiCompletions.callCompletions(request), result);
  }
}
