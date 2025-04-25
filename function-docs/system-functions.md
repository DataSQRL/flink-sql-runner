# System Functions

The following system functions are added to [Flink's built-in system functions](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/systemfunctions/) to support additional types and functionality.

## JSONB Functions

The binary JSON type (jsonb) uses a more efficient representation of JSON data and allows for native support of JSON in formats and connectors, like mapping to Postgre's JSONB column type.

The following system functions create and manipulate binary JSON data.

| Function Name         | Description                                                                                          | Example Usage                                                                                     |
|-----------------------|------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `to_jsonb`            | Parses a JSON string or Flink object (e.g., `Row`, `Row[]`) into a JSON object.                      | `to_jsonb('{"name":"Alice"}')` → JSON object                                                      |
| `jsonb_to_string`     | Serializes a JSON object into a JSON string.                                                         | `jsonb_to_string(to_jsonb('{"a":1}'))` → `'{"a":1}'`                                               |
| `jsonb_object`        | Constructs a JSON object from key-value pairs. Keys must be strings.                                 | `jsonb_object('a', 1, 'b', 2)` → `{"a":1,"b":2}`                                                   |
| `jsonb_array`         | Constructs a JSON array from multiple values or JSON objects.                                        | `jsonb_array(1, 'a', to_jsonb('{"b":2}'))` → `[1,"a",{"b":2}]`                                     |
| `jsonb_extract`       | Extracts a value from a JSON object using a JSONPath expression. Optionally specify default value.   | `jsonb_extract(to_jsonb('{"a":1}'), '$.a')` → `1`                                                  |
| `jsonb_query`         | Executes a JSONPath query on a JSON object and returns the result as a JSON string.                  | `jsonb_query(to_jsonb('{"a":[1,2]}'), '$.a')` → `'[1,2]'`                                          |
| `jsonb_exists`        | Returns `TRUE` if a JSONPath exists within a JSON object.                                            | `jsonb_exists(to_jsonb('{"a":1}'), '$.a')` → `TRUE`                                                |
| `jsonb_concat`        | Merges two JSON objects. If keys overlap, the second object's values are used.                       | `jsonb_concat(to_jsonb('{"a":1}'), to_jsonb('{"b":2}'))` → `{"a":1,"b":2}`                         |
| `jsonb_array_agg`     | Aggregate function: accumulates values into a JSON array.                                            | `SELECT jsonb_array_agg(col) FROM tbl`                                                             |
| `jsonb_object_agg`    | Aggregate function: accumulates key-value pairs into a JSON object.                                  | `SELECT jsonb_object_agg(key_col, val_col) FROM tbl`                                               |

## Vector Functions

The vector type supports efficient representation of large numeric vector, e.g. for embeddings. 

Use the following system functions to manipulate, convert, and aggregate vectors.

| Function Name           | Description                                                                 | Example Usage                        |
|------------------------|-----------------------------------------------------------------------------|-------------------------------------|
| `cosine_similarity`    | Computes cosine similarity between two vectors.                             | `cosine_similarity(vec1, vec2)`     |
| `cosine_distance`      | Computes cosine distance between two vectors (1 - cosine similarity).       | `cosine_distance(vec1, vec2)`       |
| `euclidean_distance`   | Computes the Euclidean distance between two vectors.                        | `euclidean_distance(vec1, vec2)`    |
| `double_to_vector`     | Converts a `DOUBLE[]` array into a `VECTOR`.                                | `double_to_vector([1.0, 2.0, 3.0])` |
| `vector_to_double`     | Converts a `VECTOR` into a `DOUBLE[]` array.                                | `vector_to_double(vec)`             |
| `center`               | Computes the centroid (average) of a collection of vectors. Aggregate function. | `SELECT center(vecCol) FROM vectors`|

## Text Functions

Functions for text manipulations and search.

| Function Name                    | Description                                                                                                                                                                                                                                                                                              |
|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `format(String, String...)`      | A function that formats text based on a format string and variable number of arguments. It uses Java's `String.format` method internally. If the input text is `null`, it returns `null`. For example, `Format("Hello %s!", "World")` returns "Hello World!".                                            |
| `text_search(String, String...)` | Evaluates a query against multiple text fields and returns a score based on the frequency of query words in the texts. It tokenizes both the query and the texts, and scores based on the proportion of query words found in the text. For example, `text_search("hello", "hello world")` returns `1.0`. |