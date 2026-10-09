# TypeSafe Classify

`TypeSafeClassify` is a Process Table Function (PTF). It sends each row of an insert-only table to TypeSafe AI's Jev model (`POST /v1/systemone`) and appends the typed answers to that row.

The answer schema is derived during planning from the column types:

- A mismatch between a question's kind and its column type fails planning, not the running job.
- The field names of a Choice question's `criteria` ROW are its options, so each option becomes a typed probability field in the output.

```sql
CREATE TEMPORARY FUNCTION typesafe_classify
  AS 'com.datasqrl.flinkrunner.stdlib.typesafe.typesafe_classify';

SELECT * FROM typesafe_classify(
  input    => TABLE underwriting_input,
  state    => DESCRIPTOR(case_file),          -- exactly one text-like column
  noul     => DESCRIPTOR(q_income_verified),  -- ROW<instructions T, criteria ROW<`true` T, `false` T>>
  choice   => DESCRIPTOR(q_purpose),          -- ROW<instructions T, criteria ROW<opt_1 T, ..., opt_n T>>
  score    => DESCRIPTOR(q_scrutiny),         -- ROW<instructions T, criteria ARRAY<T>>
  model    => 'jev-1.13.0',                   -- default 'jev-latest'
  on_error => 'NULL',                         -- 'FAIL' (default) or 'NULL'
  on_time  => DESCRIPTOR(submitted_at)        -- required if the input has an event-time attribute
);
```

`T` is a text-like value: `STRING`, `ROW`, `ARRAY` or `MAP<STRING, ...>`, nested arbitrarily. Nested values may also be numbers, booleans, temporal types or intervals. `BINARY`, `VARBINARY`, `RAW` and maps with non-string keys are rejected.

## Output

The output contains all input columns, followed by these appended columns, followed by the framework's `rowtime` column when `on_time` is given:

| Column | Type |
|---|---|
| `<noul column>_answer` | `DOUBLE` (probability of yes) |
| `<choice column>_answer` | `ROW<choice STRING, confidence DOUBLE, probabilities ROW<opt_1 DOUBLE, ...>>` |
| `<score column>_answer` | `ROW<score DOUBLE, confidence DOUBLE, probabilities ARRAY<DOUBLE>>` |
| `_model` | `STRING NOT NULL`, the versioned model from the response |
| `_error` | `STRING`, present only with `on_error => 'NULL'` |

Answers appear in this order: Noul, then Choice, then Score, each in descriptor order.

Use `rowtime` for downstream time-based operations. The original time column is passed through with its value unchanged.

NULL handling:

- **NULL state:** no API call is made. All answers are NULL, and `_model` is the requested model.
- **NULL question:** the question is left out of the request, and its answer is NULL.

## Configuration

Configuration is read from job parameters (`pipeline.global-job-parameters`). The API key is never a SQL argument.

| Job parameter | Default |
|---|---|
| `typesafe.api-key` | Falls back to the `TYPESAFE_API_KEY` environment variable. Required. |
| `typesafe.endpoint` | `https://api.typesafe.ai/v1/systemone` |
| `typesafe.connect-timeout` | `10 s` |
| `typesafe.request-timeout` | `120 s` |
| `typesafe.max-retries` | `5` |
| `typesafe.max-backoff` | `30 s` |

## Errors

| Condition | Behavior |
|---|---|
| 429, 529, 5xx, I/O errors | Retried with exponential backoff and full jitter. Once retries are exhausted, `on_error` applies. |
| Other 4xx, such as 422 | Not retried. `on_error` applies. |
| 401 / 403 | Always fails the job. |
| Contract violations (NULL instructions, 2–10 score levels, malformed answers) | `on_error` applies. |

`_error` holds a code plus a fixed message, such as `HTTP_422: request rejected by the API (token_limit_exceeded)`. Free-text API messages, request bodies and response bodies are never logged or copied into errors, because the state may contain PII.

## Metrics

These metrics are reported per subtask:

- `requests`
- `requestLatency` (histogram, in milliseconds)
- `retries`
- `errors`, tagged with `error_class`: `retryable_exhausted`, `http_4xx`, `contract`, `fatal`
- `nullRows`
- `inputTokens`
- `outputTokens`

## API assumptions

The requirements don't pin down the following parts of the wire format. Confirm them against the live API:

- **Authentication:** `Authorization: Bearer <key>`.
- **Response:** `{"model": ..., "answers": {"<question>": {"type": ..., ...}}, "usage": {"input_tokens": ..., "output_tokens": ...}}`.
- **Answer fields by kind:**
  - Noul: `noul`.
  - Choice: `choice`, `confidence` and `probabilities`, keyed by option.
  - Score: `score`, `confidence` and `probabilities`, keyed `"0".."n-1"`.
- **Error responses:** a machine-readable code is taken from `error.code` or `error.type`, if one is present.
