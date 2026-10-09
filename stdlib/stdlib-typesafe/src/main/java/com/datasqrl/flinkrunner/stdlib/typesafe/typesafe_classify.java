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
package com.datasqrl.flinkrunner.stdlib.typesafe;

import static org.apache.flink.table.types.inference.StaticArgumentTrait.PASS_COLUMNS_THROUGH;
import static org.apache.flink.table.types.inference.StaticArgumentTrait.ROW_SEMANTIC_TABLE;

import java.util.EnumSet;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;

/**
 * Classifies each row of a table with TypeSafe AI's Jev model and appends the typed answers.
 *
 * <pre>{@code
 * SELECT * FROM typesafe_classify(
 *   input    => TABLE t,
 *   state    => DESCRIPTOR(case_file),
 *   noul     => DESCRIPTOR(q_yes_no, ...),
 *   choice   => DESCRIPTOR(q_category, ...),
 *   score    => DESCRIPTOR(q_scale, ...),
 *   model    => 'jev-1.13.0',
 *   on_error => 'NULL',
 *   on_time  => DESCRIPTOR(event_time))
 * }</pre>
 *
 * <p>The answer schema is derived during planning from the column types of the referenced question
 * columns, so a mismatch between a question's kind and its type is a planning error. In particular,
 * the field names of a Choice question's criteria ROW are its options, which makes every option a
 * typed probability field in the output.
 *
 * <p>The function is stateless and time-transparent: it emits exactly one row per input row from
 * within {@code eval()}, never registers timers and never touches time, so event time passes
 * through the framework's {@code on_time} / {@code rowtime} mechanism unchanged.
 *
 * <p>The API key is read from the job parameter {@value TypeSafeConfig#API_KEY} or the environment
 * variable {@value TypeSafeConfig#API_KEY_ENV}; it is never a SQL argument.
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class typesafe_classify extends ProcessTableFunction<Row> implements SpecializedFunction {

  private static final long serialVersionUID = 1L;

  /** The validated call; null until the planner specializes the function for a concrete call. */
  private final ClassifyPlan plan;

  private transient JevCodec codec;
  private transient JevClient client;
  private transient ClassifyMetrics metrics;

  public typesafe_classify() {
    this(null);
  }

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
        .staticArguments(
            StaticArgument.table(
                "input", Row.class, false, EnumSet.of(ROW_SEMANTIC_TABLE, PASS_COLUMNS_THROUGH)),
            StaticArgument.scalar("state", DataTypes.DESCRIPTOR(), false),
            StaticArgument.scalar(QuestionKind.NOUL.getArgName(), DataTypes.DESCRIPTOR(), true),
            StaticArgument.scalar(QuestionKind.CHOICE.getArgName(), DataTypes.DESCRIPTOR(), true),
            StaticArgument.scalar(QuestionKind.SCORE.getArgName(), DataTypes.DESCRIPTOR(), true),
            StaticArgument.scalar("model", DataTypes.STRING(), true),
            StaticArgument.scalar("on_error", DataTypes.STRING(), true))
        .outputTypeStrategy(ctx -> Optional.of(ClassifyPlan.fromCallContext(ctx).outputDataType()))
        .build();
  }

  @Override
  public UserDefinedFunction specialize(SpecializedContext context) {
    return new typesafe_classify(ClassifyPlan.fromCallContext(context.getCallContext()));
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }

  @Override
  public void open(FunctionContext context) {
    open(TypeSafeConfig.from(context), new ClassifyMetrics(context.getMetricGroup()));
  }

  void open(TypeSafeConfig config, ClassifyMetrics metrics) {
    if (plan == null) {
      throw new IllegalStateException("typesafe_classify has not been specialized by the planner.");
    }
    this.metrics = metrics;
    this.codec = new JevCodec(plan);
    this.client = new JevClient(config, metrics);
    log.info("Opened typesafe_classify with model '{}' and {}", plan.getModel(), config);

    if (ClassifyPlan.isMovingAlias(plan.getModel())) {
      log.warn(
          "typesafe_classify uses the moving model alias '{}'. Answers may change when the alias "
              + "moves; pin a model version such as 'jev-1.13.0' for production jobs.",
          plan.getModel());
    }
  }

  @Override
  public void close() {
    // The HttpClient releases its pooled connections once it is unreachable.
    client = null;
  }

  /**
   * Classifies one row. The scalar arguments are constants that were already captured in the plan
   * during specialization.
   */
  public void eval(
      Row input,
      ColumnList state,
      ColumnList noul,
      ColumnList choice,
      ColumnList score,
      String model,
      String onError) {
    collect(classify(input));
  }

  Row classify(Row input) {
    if (input.getField(plan.getStateIndex()) == null) {
      return emptyResult(null);
    }

    try {
      var request = codec.buildRequest(input);
      if (request == null) {
        return emptyResult(null);
      }

      var answers = codec.mapResponse(request, client.classify(request.body()));
      metrics.inputTokens.inc(answers.inputTokens());
      metrics.outputTokens.inc(answers.outputTokens());

      return result(answers.values(), answers.model(), null);

    } catch (ClassifyException e) {
      metrics.errors(e.getErrorClass()).inc();
      if (e.isFatal() || plan.getErrorPolicy() == ErrorPolicy.FAIL) {
        throw e;
      }
      metrics.nullRows.inc();
      return emptyResult(e.getMessage());
    }
  }

  private Row emptyResult(String error) {
    return result(new Object[plan.getQuestions().size()], plan.getModel(), error);
  }

  private Row result(Object[] answers, String model, String error) {
    var withError = plan.getErrorPolicy() == ErrorPolicy.NULL;
    var fields = new Object[answers.length + (withError ? 2 : 1)];
    System.arraycopy(answers, 0, fields, 0, answers.length);
    fields[answers.length] = model;
    if (withError) {
      fields[answers.length + 1] = error;
    }

    return Row.of(fields);
  }
}
