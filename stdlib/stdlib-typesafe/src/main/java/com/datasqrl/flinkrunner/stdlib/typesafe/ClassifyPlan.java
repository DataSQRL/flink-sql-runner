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

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.RowKind;

/**
 * The validated, planning-time description of a {@link typesafe_classify} call.
 *
 * <p>A plan is derived from the call's {@link CallContext} during type inference, where it also
 * performs all planning-time validation, and again when the function is specialized for runtime,
 * where it drives request construction and response mapping.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ClassifyPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  static final int ARG_INPUT = 0;
  static final int ARG_STATE = 1;
  static final int ARG_NOUL = 2;
  static final int ARG_CHOICE = 3;
  static final int ARG_SCORE = 4;
  static final int ARG_MODEL = 5;
  static final int ARG_ON_ERROR = 6;

  /** Number of arguments declared by the function, excluding the framework's system arguments. */
  static final int DECLARED_ARG_COUNT = 7;

  /** Offset of the framework's {@code on_time} argument from the end of the argument list. */
  private static final int SYSTEM_ARG_ON_TIME_OFFSET = 2;

  static final String DEFAULT_MODEL = "jev-latest";
  static final String ANSWER_SUFFIX = "_answer";
  static final String MODEL_COLUMN = "_model";
  static final String ERROR_COLUMN = "_error";
  static final String ROWTIME_COLUMN = "rowtime";
  static final String INSTRUCTIONS_FIELD = "instructions";
  static final String CRITERIA_FIELD = "criteria";

  static final int MIN_CHOICE_OPTIONS = 2;
  static final int MAX_CHOICE_OPTIONS = 255;

  private final RowType inputType;
  private final int stateIndex;
  private final List<Question> questions;
  private final String model;
  private final ErrorPolicy errorPolicy;

  /**
   * A single question column together with the kind it was declared as.
   *
   * @param options the Choice option keys in declaration order; empty for other kinds
   */
  public record Question(
      String column,
      int columnIndex,
      QuestionKind kind,
      int instructionsField,
      LogicalType instructionsType,
      int criteriaField,
      LogicalType criteriaType,
      List<String> options)
      implements Serializable {

    String answerColumn() {
      return column + ANSWER_SUFFIX;
    }
  }

  LogicalType getStateType() {
    return inputType.getTypeAt(stateIndex);
  }

  /** The columns this function appends to the pass-through input columns. */
  public DataType outputDataType() {
    var fields = new ArrayList<DataTypes.Field>();
    for (var q : questions) {
      fields.add(FIELD(q.answerColumn(), q.kind.answerType(q.options)));
    }
    fields.add(FIELD(MODEL_COLUMN, STRING().notNull()));
    if (errorPolicy == ErrorPolicy.NULL) {
      fields.add(FIELD(ERROR_COLUMN, STRING()));
    }

    return ROW(fields);
  }

  /**
   * Derives and validates a plan from a call context.
   *
   * @throws ValidationException if the call violates the function's typing contract
   */
  public static ClassifyPlan fromCallContext(CallContext ctx) {
    var args = ctx.getArgumentDataTypes();
    var inputLogicalType = args.get(ARG_INPUT).getLogicalType();
    if (!(inputLogicalType instanceof RowType inputType)) {
      throw new ValidationException("typesafe_classify: 'input' must be a table argument.");
    }

    var inputNames = inputType.getFieldNames();
    checkInsertOnly(ctx);
    if (inputNames.contains(ROWTIME_COLUMN)) {
      throw new ValidationException(
          "`input => TABLE ...`: column '%s' is reserved, because the framework appends it for `on_time`. Please rename the input column."
              .formatted(ROWTIME_COLUMN));
    }

    // state
    var stateColumns = descriptor(ctx, ARG_STATE);
    var stateArg = describeArg("state", stateColumns);
    if (stateColumns.size() != 1) {
      throw new ValidationException(
          "%s: exactly one state column is required, but %d were given."
              .formatted(stateArg, stateColumns.size()));
    }
    var stateColumn = stateColumns.get(0);
    var stateIndex = columnIndex(inputType, stateColumn, stateArg);

    // questions
    Map<QuestionKind, List<String>> declared = new EnumMap<>(QuestionKind.class);
    for (QuestionKind kind : QuestionKind.values()) {
      declared.put(kind, descriptor(ctx, kind.getArgPos()));
    }
    checkDisjoint(stateColumn, declared);
    if (declared.values().stream().allMatch(List::isEmpty)) {
      throw new ValidationException(
          "typesafe_classify: at least one question is required. Please pass question columns via `noul`, `choice` or `score`.");
    }

    checkText(stateArg, stateColumn, "state", inputType.getTypeAt(stateIndex));

    var questions = new ArrayList<Question>();
    for (var kind : QuestionKind.values()) {
      var columns = declared.get(kind);
      var arg = describeArg(kind.getArgName(), columns);
      for (var column : columns) {
        var idx = columnIndex(inputType, column, arg);
        questions.add(question(arg, column, idx, kind, inputType.getTypeAt(idx)));
      }
    }

    var model = stringLiteral(ctx, ARG_MODEL, "model").orElse(DEFAULT_MODEL);
    if (model.isBlank()) {
      throw new ValidationException("`model => ''`: the model id must not be empty.");
    }

    var errorPolicy =
        stringLiteral(ctx, ARG_ON_ERROR, "on_error")
            .map(
                v ->
                    ErrorPolicy.parse(v)
                        .orElseThrow(
                            () ->
                                new ValidationException(
                                    "`on_error => '%s'`: must be 'FAIL' or 'NULL'.".formatted(v))))
            .orElse(ErrorPolicy.FAIL);

    var plan = new ClassifyPlan(inputType, stateIndex, List.copyOf(questions), model, errorPolicy);
    plan.checkOutputNames();
    checkOnTime(ctx, inputType);

    return plan;
  }

  // ------------------------------------------------------------------------------------------
  // Validation helpers
  // ------------------------------------------------------------------------------------------

  private static void checkInsertOnly(CallContext ctx) {
    var mode = ctx.getTableSemantics(ARG_INPUT).flatMap(TableSemantics::changelogMode);
    if (mode.isPresent() && !mode.get().containsOnly(RowKind.INSERT)) {
      throw new ValidationException(
          "`input => TABLE ...`: the input table must be insert-only, but it produces "
              + mode.get()
              + ".");
    }
  }

  private static List<String> descriptor(CallContext ctx, int pos) {
    return ctx.getArgumentValue(pos, ColumnList.class).map(ColumnList::getNames).orElse(List.of());
  }

  private static String describeArg(String argName, List<String> columns) {
    return "`%s => DESCRIPTOR(%s)`".formatted(argName, String.join(", ", columns));
  }

  private static int columnIndex(RowType inputType, String column, String arg) {
    var idx = inputType.getFieldIndex(column);
    if (idx >= 0) {
      return idx;
    }

    throw new ValidationException(
        "%s: column '%s' does not exist in the input table. Available columns: %s"
            .formatted(arg, column, inputType.getFieldNames()));
  }

  private static void checkDisjoint(String stateColumn, Map<QuestionKind, List<String>> declared) {
    var owner = new HashMap<String, String>();
    owner.put(stateColumn, "state");

    for (var e : declared.entrySet()) {
      var arg = e.getKey().getArgName();

      for (var column : e.getValue()) {
        var previous = owner.putIfAbsent(column, arg);
        if (previous != null) {
          throw new ValidationException(
              "%s: column '%s' is already used by `%s`. Each column can be used only once across `state`, `noul`, `choice` and `score`."
                  .formatted(describeArg(arg, e.getValue()), column, previous));
        }
      }
    }
  }

  private static Question question(
      String arg, String column, int columnIndex, QuestionKind kind, LogicalType type) {

    if (!(type instanceof RowType row)
        || row.getFieldCount() != 2
        || row.getFieldIndex(INSTRUCTIONS_FIELD) < 0
        || row.getFieldIndex(CRITERIA_FIELD) < 0) {

      throw new ValidationException(
          "%s: column '%s' must be of type ROW<instructions T, criteria %s>, but found %s."
              .formatted(arg, column, kind.expectedCriteria(), type.asSummaryString()));
    }

    var instructionsField = row.getFieldIndex(INSTRUCTIONS_FIELD);
    var criteriaField = row.getFieldIndex(CRITERIA_FIELD);
    var instructions = row.getTypeAt(instructionsField);
    var criteria = row.getTypeAt(criteriaField);

    checkText(arg, column, INSTRUCTIONS_FIELD, instructions);
    findUnsupported(criteria, CRITERIA_FIELD)
        .ifPresent(u -> unsupported(arg, column, u.path(), u.type()));

    var shapeError = criteriaShapeError(kind, criteria);
    if (shapeError.isPresent()) {
      var hint =
          Arrays.stream(QuestionKind.values())
              .filter(k -> k != kind && criteriaShapeError(k, criteria).isEmpty())
              .findFirst()
              .map(k -> " Did you mean `%s`?".formatted(k.getArgName()))
              .orElse("");

      throw new ValidationException(
          "%s: column '%s' must have criteria of type %s, but found %s.%s%s"
              .formatted(
                  arg,
                  column,
                  kind.expectedCriteria(),
                  criteria.asSummaryString(),
                  shapeError.get().isEmpty() ? "" : " " + shapeError.get(),
                  hint));
    }

    List<String> options =
        kind == QuestionKind.CHOICE ? List.copyOf(((RowType) criteria).getFieldNames()) : List.of();

    return new Question(
        column,
        columnIndex,
        kind,
        instructionsField,
        instructions,
        criteriaField,
        criteria,
        options);
  }

  /**
   * Returns why {@code criteria} doesn't fit {@code kind}, or empty if it does. An empty message
   * means the top-level type is wrong and no further detail is needed.
   */
  private static Optional<String> criteriaShapeError(QuestionKind kind, LogicalType criteria) {
    switch (kind) {
      case NOUL:
        {
          if (!(criteria instanceof RowType row)) {
            return Optional.of("");
          }
          if (row.getFieldCount() != 2
              || row.getFieldIndex("true") < 0
              || row.getFieldIndex("false") < 0) {
            return Optional.of(
                "Noul criteria need exactly the fields `true` and `false`, but found "
                    + row.getFieldNames()
                    + ".");
          }
          return nonTextField(row);
        }
      case CHOICE:
        {
          if (!(criteria instanceof RowType row)) {
            return Optional.of("");
          }

          if (row.getFieldCount() < MIN_CHOICE_OPTIONS
              || row.getFieldCount() > MAX_CHOICE_OPTIONS) {
            return Optional.of(
                "Choice criteria need between %d and %d options, but found %d."
                    .formatted(MIN_CHOICE_OPTIONS, MAX_CHOICE_OPTIONS, row.getFieldCount()));
          }
          return nonTextField(row);
        }
      case SCORE:
        {
          if (!(criteria instanceof ArrayType)) {
            return Optional.of("");
          }
          var element = ((ArrayType) criteria).getElementType();
          if (!isTextRoot(element)) {
            return Optional.of(
                "Score levels must be text-like (T), but found " + element.asSummaryString() + ".");
          }
          return Optional.empty();
        }
      default:
        throw new IllegalStateException("Unknown kind " + kind);
    }
  }

  private static Optional<String> nonTextField(RowType row) {
    return row.getFields().stream()
        .filter(f -> !isTextRoot(f.getType()))
        .findFirst()
        .map(
            f ->
                "Field '%s' must be text-like (T), but found %s."
                    .formatted(f.getName(), f.getType().asSummaryString()));
  }

  /** Checks that a value is {@code T}: text-like at the top level and serializable throughout. */
  private static void checkText(String arg, String column, String path, LogicalType type) {
    findUnsupported(type, path).ifPresent(u -> unsupported(arg, column, u.path(), u.type()));
    if (!isTextRoot(type)) {
      throw new ValidationException(
          "%s: column '%s' must have a text-like type (STRING, ROW, ARRAY or MAP<STRING, ...>) at '%s', but found %s."
              .formatted(arg, column, path, type.asSummaryString()));
    }
  }

  private static void unsupported(String arg, String column, String path, LogicalType type) {
    throw new ValidationException(
        "%s: column '%s' has unsupported type %s at '%s'. Jev accepts text only, so BINARY, VARBINARY, RAW and MAP with non-string keys are not supported."
            .formatted(arg, column, type.asSummaryString(), path));
  }

  /** Whether the type is {@code T}: a string, ROW, ARRAY or MAP with string keys. */
  static boolean isTextRoot(LogicalType type) {
    return switch (type.getTypeRoot()) {
      case CHAR, VARCHAR, ROW, ARRAY -> true;
      case MAP -> ((MapType) type).getKeyType().is(LogicalTypeFamily.CHARACTER_STRING);
      default -> false;
    };
  }

  private record Unsupported(String path, LogicalType type) {}

  /** Finds the first nested type the JSON serializer cannot handle. */
  private static Optional<Unsupported> findUnsupported(LogicalType type, String path) {
    return switch (type.getTypeRoot()) {
      case CHAR,
          VARCHAR,
          BOOLEAN,
          TINYINT,
          SMALLINT,
          INTEGER,
          BIGINT,
          FLOAT,
          DOUBLE,
          DECIMAL,
          DATE,
          TIME_WITHOUT_TIME_ZONE,
          TIMESTAMP_WITHOUT_TIME_ZONE,
          TIMESTAMP_WITH_LOCAL_TIME_ZONE,
          INTERVAL_YEAR_MONTH,
          INTERVAL_DAY_TIME,
          NULL ->
          Optional.empty();
      case ROW -> {
        for (RowType.RowField f : ((RowType) type).getFields()) {
          var u = findUnsupported(f.getType(), path + "." + f.getName());
          if (u.isPresent()) {
            yield u;
          }
        }
        yield Optional.empty();
      }
      case ARRAY -> findUnsupported(((ArrayType) type).getElementType(), path + "[]");
      case MULTISET -> findUnsupported(((MultisetType) type).getElementType(), path + "[]");
      case MAP -> {
        if (!isTextRoot(type)) {
          yield Optional.of(new Unsupported(path, type));
        }
        yield findUnsupported(((MapType) type).getValueType(), path + "{}");
      }
      default -> Optional.of(new Unsupported(path, type));
    };
  }

  private static Optional<String> stringLiteral(CallContext ctx, int pos, String argName) {
    if (ctx.isArgumentNull(pos)) {
      return Optional.empty();
    }

    if (!ctx.isArgumentLiteral(pos)) {
      throw new ValidationException(
          "`%s`: the argument must be a string literal.".formatted(argName));
    }

    return ctx.getArgumentValue(pos, String.class);
  }

  private void checkOutputNames() {
    var inputNames = new HashSet<>(inputType.getFieldNames());
    var outputNames = new ArrayList<String>();
    questions.forEach(q -> outputNames.add(q.answerColumn()));
    outputNames.add(MODEL_COLUMN);
    if (errorPolicy == ErrorPolicy.NULL) {
      outputNames.add(ERROR_COLUMN);
    }

    var collisions = outputNames.stream().filter(inputNames::contains).toList();
    if (!collisions.isEmpty()) {
      throw new ValidationException(
          "`input => TABLE ...`: the input already contains the column(s) %s that typesafe_classify appends. Please rename or project them away."
              .formatted(collisions));
    }
  }

  /**
   * Ensures event time is not silently dropped. The framework's {@code on_time} argument is only
   * visible during planning, when the call still carries the system arguments.
   */
  private static void checkOnTime(CallContext ctx, RowType inputType) {
    var argCount = ctx.getArgumentDataTypes().size();
    if (argCount <= DECLARED_ARG_COUNT) {
      return;
    }
    var rowtimeColumns =
        inputType.getFields().stream()
            .filter(f -> LogicalTypeChecks.isRowtimeAttribute(f.getType()))
            .map(RowType.RowField::getName)
            .toList();
    if (rowtimeColumns.isEmpty()) {
      return;
    }
    var onTime = descriptor(ctx, argCount - SYSTEM_ARG_ON_TIME_OFFSET);
    if (onTime.stream().noneMatch(rowtimeColumns::contains)) {
      throw new ValidationException(
          "`on_time`: the input has the event-time attribute '%s', so `on_time => DESCRIPTOR(%s)` must be supplied. Otherwise event time would be dropped."
              .formatted(rowtimeColumns.get(0), rowtimeColumns.get(0)));
    }
  }

  static boolean isMovingAlias(String model) {
    return model.equals("jev-latest") || model.equals("jev-preview");
  }
}
