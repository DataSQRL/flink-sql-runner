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
package com.datasqrl.flinkrunner.stdlib.openai;

import com.datasqrl.flinkrunner.stdlib.openai.utils.FunctionExecutor;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class completions extends ScalarFunction {

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

  public String eval(String prompt, String modelName) {
    return eval(prompt, modelName, null, null, null);
  }

  public String eval(String prompt, String modelName, Integer maxCompletionTokens) {
    return eval(prompt, modelName, maxCompletionTokens, null, null);
  }

  public String eval(
      String prompt, String modelName, Integer maxCompletionTokens, Double temperature) {
    return eval(prompt, modelName, maxCompletionTokens, temperature, null);
  }

  public String eval(
      String prompt,
      String modelName,
      Integer maxCompletionTokens,
      Double temperature,
      Double topP) {
    return eval(prompt, modelName, maxCompletionTokens, temperature, topP, null);
  }

  public String eval(
      String prompt,
      String modelName,
      Integer maxCompletionTokens,
      Double temperature,
      Double topP,
      String reasoningEffort) {

    final OpenAiCompletions.CompletionsRequest request =
        OpenAiCompletions.CompletionsRequest.builder()
            .prompt(prompt)
            .modelName(modelName)
            .maxCompletionTokens(maxCompletionTokens)
            .temperature(temperature)
            .topP(topP)
            .reasoningEffort(reasoningEffort)
            .build();

    return executor.execute(() -> openAiCompletions.callCompletions(request));
  }
}
