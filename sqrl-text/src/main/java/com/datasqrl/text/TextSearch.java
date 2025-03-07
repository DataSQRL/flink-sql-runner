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
package com.datasqrl.text;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.FlinkTypeUtil.VariableArguments;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Returns a numeric score for how well the given query string matches the provided string text.
 * Returns 0 if there is no match. Use this function for full-text search.
 */
public class TextSearch extends ScalarFunction {

  public static void tokenizeTo(String text, Collection<String> collection) {
    StringTokenizer tokenizer = new StringTokenizer(text);
    while (tokenizer.hasMoreTokens()) {
      collection.add(tokenizer.nextToken().trim().toLowerCase());
    }
  }

  public Double eval(String query, String... texts) {
    if (query == null) {
      return null;
    }
    List<String> queryWords = new ArrayList<>();
    tokenizeTo(query, queryWords);
    if (queryWords.isEmpty()) {
      return 1.0;
    }

    Set<String> searchWords = new HashSet<>();
    Arrays.stream(texts).forEach(text -> tokenizeTo(text, searchWords));

    double score = 0;
    for (String queryWord : queryWords) {
      if (searchWords.contains(queryWord)) {
        score += 1.0;
      }
    }
    return score / queryWords.size();
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .inputTypeStrategy(
            VariableArguments.builder()
                .staticType(DataTypes.STRING())
                .variableType(DataTypes.STRING())
                .minVariableArguments(1)
                .maxVariableArguments(256)
                .build())
        .outputTypeStrategy(FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.DOUBLE()))
        .build();
  }
}
