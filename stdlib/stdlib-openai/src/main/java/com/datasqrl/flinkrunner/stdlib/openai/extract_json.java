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

import com.datasqrl.flinkrunner.stdlib.openai.utils.FunctionExecutor;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class extract_json extends ScalarFunction {

  private transient OpenAiCompletions openAiCompletions;
  private transient FunctionExecutor executor;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.openAiCompletions = createOpenAiCompletions();
    this.executor = new FunctionExecutor(context, extract_json.class.getSimpleName());
  }

  protected OpenAiCompletions createOpenAiCompletions() {
    return new OpenAiCompletions();
  }

  public String eval(String prompt, String modelName) {
    return eval(prompt, modelName, null);
  }

  public String eval(String prompt, String modelName, Double temperature) {
    return eval(prompt, modelName, temperature, null);
  }

  public String eval(String prompt, String modelName, Double temperature, Double topP) {
    return eval(prompt, modelName, temperature, topP, null);
  }

  public String eval(
      String prompt, String modelName, Double temperature, Double topP, String jsonSchema) {
    final OpenAiCompletions.CompletionsRequest request =
        OpenAiCompletions.CompletionsRequest.builder()
            .prompt(prompt)
            .modelName(modelName)
            .requireJsonOutput(true)
            .jsonSchema(jsonSchema)
            .temperature(temperature)
            .topP(topP)
            .build();

    return executor.execute(() -> openAiCompletions.callCompletions(request));
  }
}
