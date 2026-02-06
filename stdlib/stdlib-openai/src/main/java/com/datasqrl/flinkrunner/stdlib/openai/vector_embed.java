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
import com.datasqrl.flinkrunner.stdlib.vector.FlinkVectorType;
import com.google.auto.service.AutoService;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

@AutoService(ScalarFunction.class)
public class vector_embed extends ScalarFunction {

  private transient OpenAiEmbeddings openAiEmbeddings;
  private transient FunctionExecutor executor;

  @Override
  public void open(FunctionContext context) throws Exception {
    this.openAiEmbeddings = createOpenAiEmbeddings();
    this.executor = new FunctionExecutor(context, vector_embed.class.getSimpleName());
  }

  protected OpenAiEmbeddings createOpenAiEmbeddings() {
    return new OpenAiEmbeddings();
  }

  public FlinkVectorType eval(String text, String modelName) {
    return executor.execute(() -> openAiEmbeddings.vectorEmbed(text, modelName));
  }
}
