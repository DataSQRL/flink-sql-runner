# Flink SQL Runner

Tools and extensions for running Apache Flink SQL applications, including Docker images, data types, connectors, and formats.

This repository contains core components for running Flink SQL applications in production using the Flink Kubernetes Operator, without manual JAR assembly or custom infrastructure.

![Flink SQL Runner Logo](docs/runner_logo.png)

## Features

- 📝 **SQL Script Execution**: Run SQL scripts directly with Flink.
- 🧾 **Compiled Plan Execution**: Run pre-compiled Flink SQL plans to manage production deployments and versioning.
- 🔄 **Environment Variable Substitution**: Inject environment variables `${VAR}` into SQL scripts and configs at runtime.
- 📦 **JAR Dependency Management**: Reference local directories with required JARs (e.g. UDFs).
- 🌍 **Kubernetes-Friendly**: Built to run with the Flink Kubernetes Operator.
- 🔧 **Function Infrastructure**: Utilities for writing and loading UDFs as system functions.
- 🪄 **Flink Extensions**:
    - Dead-letter queue support in Kafka for poison message handling.
    - Native JSON and Vector types with JSON format and PostgreSQL connector support.
    - Additional configuration options for CSV format.


---

## Flink SQL Runner Usage

You can use the docker image to run FlinkSQL scripts or compiled plans locally or in Kubernetes.
The docker image contains the executable flink-sql-runner.jar file which supports the following command line arguments:

| Argument	| Description                                                                                           |
 ----------- |-------------------------------------------------------------------------------------------------------|
|--sqlfile| 	FlinkSQL script to execute                                                                           |
|--planfile| 	Compiled plan (i.e. JSON file) to execute                                                            |
|--config-dir| 	Directory containing Flink configuration YAML files                                                  |
|--udfpath| 	Path to jar files that implement user defined functions (UDFs) or other runtime extensions for Flink |

Note that the runner expects either a FlinkSQL script or a compiled plan - not both.

We strongly recommend to run compiled plans for production FlinkSQL applications since they support
lifecycle management of applications, are stable across Flink versions, and provide more control over
the executed jobgraph.
You can use the [SQRL compiler](https://github.com/DataSQRL/sqrl/) to compile FlinkSQL applications to compiled plans.

The configuration directory contains the [Flink configuration files](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/).

The UDF path points to a directory containing JAR files. Most commonly, the jars implement user defined functions that are invoked by the FlinkSQL application. They can also contain other runtime extension for Flink.

### Running Locally

Pull the latest Docker image from Docker Hub:

```bash
docker pull datasqrl/flink-sql-runner:latest
```

@Marvin: please add an example for how to run this locally
Can we use the docker-compose.yml in the flink-sql-runner module?

### Running in Kubernetes with Flink Operator
Here's how to use the Flink Jar Runner with the Flink Operator on Kubernetes:

1. Prepare Your Files: Ensure that your SQL scripts (`statements.sql`) or compiled plans (`compiledplan.json`), and JAR files are accessible within your container.

1. Example Helm Chart Configuration:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: sql-example
spec:
  image: datasqrl/flink-sql-runner:latest
  flinkVersion: v1_19
  flinkConfiguration:
    taskmanager.numberOfTaskSlots: "1"
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "2048m"
      cpu: 1
  job:
    jarURI: http://raw.github.com/datasqrl/releases/1.0.0/flink-sql-runner.jar
    args: ["--sqlfile", "/opt/flink/usrlib/sql-scripts/statements.sql", "--planfile", "/opt/flink/usrlib/sql-scripts/compiledplan.json", "--udfpath", "/opt/flink/usrlib/jars"]
    parallelism: 1
    upgradeMode: stateless
```

Note: configure either the sql script OR the compiled plan - not both.

1. Deploy with Helm:
```bash
helm install sql-example -f your-helm-values.yaml your-helm-chart
```

### Environment Variable Substitution
Flink SQL Runner automatically substitutes environment variables in your configuration files, SQL scripts, and compiled plans for secrets and environment specific configuration. Environment variables must be of the form `${ENV_VARIABLE}` and inside of strings.

For example, `${DATA_PATH}` is an environment variable inside the connector configuration of a table that is substituted at runtime:
```sql
CREATE TEMPORARY TABLE `MyTable` (
  ...
) WITH (
  'connector' = 'filesystem',
  'format' = 'json',
  'path' = '${DATA_PATH}/applications.jsonl',
  'source.monitor-interval' = '1'
);
```

### Building Your Own Flink SQL Runner

The Flink SQL runner is published to Maven Central and you can add it as a dependency in your project to extend
the runner to suit your needs

- Maven:

```xml
<dependency>
<groupId>com.datasqrl.flinkrunner</groupId>
<artifactId>flink-sql-runner</artifactId>
<version>1.0.0</version>
</dependency>
```
- Gradle:

```groovy
implementation 'com.datasqrl.flinkrunner:flink-sql-runner:1.0.0'
```
---

## Flink Extensions

The Flink SQL Runner contains a few extensions to the Flink runtime.

### Dead-Letter-Queue Support for Kafka Sources

If a FlinkSQL fails to deserialize a message from a Kafka topic, the entire job can fail. 

This project implements the `kafka-safe` and `upsert-kafka-safe` [connectors](connectors/kafka-safe) which extend the respective kafka connectors with dead-letter-queue support, so that messages which fail to deserialize are sent elsewhere, not processed any further, and do not fail the job.

In addition to the configuration options exposed by the original kafka connectors, the `-safe` versions support the following optional configuration options:

| Options | Default | Type   | Description                                                                                                                       |
|---------|---------|--------|-----------------------------------------------------------------------------------------------------------------------------------|
| scan.deser-failure.handler        | none    | String | Use `log` to output failed messages to the logger, `kafka` to output failed messages to a kafka topic, or `none` to fail the job. |
| scan.deser-failure.topic        | -       | String | The topic for the dead-letter-queue that failed messages are written to. Required when handler is configured to `kafka`            |

### JSON Type

This project adds a native [JSON type](types/json-type) and associated functions for more efficient JSON handling that does not serialize from and to string repeatedly.

Native JSON type support is also extended to the [JSON format](formats/flexible-json-format) called `flexible-json` for writing JSON data as nested documents (instead of strings) as well as the [JDBC connector for PostgreSQL](connectors/postgresql-connector) to write JSON data to JSONB columns.

The native JSON type is supported by the following functions:

| Function Name       | Description                                                                                          | Example Usage                                                                                     |
|---------------------|------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|
| `toJson`            | Parses a JSON string or Flink object (e.g., `Row`, `Row[]`) into a JSON object.                      | `toJson('{"name":"Alice"}')` → JSON object                                                        |
| `jsonToString`      | Serializes a JSON object into a JSON string.                                                         | `jsonToString(toJson('{"a":1}'))` → `'{"a":1}'`                                                    |
| `jsonObject`        | Constructs a JSON object from key-value pairs. Keys must be strings.                                 | `jsonObject('a', 1, 'b', 2)` → `{"a":1,"b":2}`                                                     |
| `jsonArray`         | Constructs a JSON array from multiple values or JSON objects.                                        | `jsonArray(1, 'a', toJson('{"b":2}'))` → `[1,"a",{"b":2}]`                                         |
| `jsonExtract`       | Extracts a value from a JSON object using a JSONPath expression. Optionally specify default value.   | `jsonExtract(toJson('{"a":1}'), '$.a')` → `1`                                                      |
| `jsonQuery`         | Executes a JSONPath query on a JSON object and returns the result as a JSON string.                  | `jsonQuery(toJson('{"a":[1,2]}'), '$.a')` → `'[1,2]'`                                              |
| `jsonExists`        | Returns `TRUE` if a JSONPath exists within a JSON object.                                            | `jsonExists(toJson('{"a":1}'), '$.a')` → `TRUE`                                                    |
| `jsonConcat`        | Merges two JSON objects. If keys overlap, the second object's values are used.                       | `jsonConcat(toJson('{"a":1}'), toJson('{"b":2}'))` → `{"a":1,"b":2}`                               |
| `jsonArrayAgg`      | Aggregate function: accumulates values into a JSON array.                                            | `SELECT jsonArrayAgg(col) FROM tbl`                                                               |
| `jsonObjectAgg`     | Aggregate function: accumulates key-value pairs into a JSON object.                                  | `SELECT jsonObjectAgg(key_col, val_col) FROM tbl`                                                 |

### Vector Type

This project adds a native [Vector type](types/vector-type) and associated functions for more efficient handling of vectors (e.g. for content embeddings).

Native Vector type support is also extended to the [JDBC connector for PostgreSQL](connectors/postgresql-connector) to write vector data to vector columns for the pgvector extension.

The native vector type is supported by the following functions:

## Vector Function Reference

## Vector Function Reference

| Function Name         | Description                                                                 | Example Usage |
|----------------------|-----------------------------------------------------------------------------|----------------|
| `asciiTextTestEmbed` | Returns a test vector where each character in a string is counted (mod 256). Used for testing only. | `asciiTextTestEmbed('hello')` |
| `cosineSimilarity`   | Computes cosine similarity between two vectors.                             | `cosineSimilarity(vec1, vec2)` |
| `cosineDistance`     | Computes cosine distance between two vectors (1 - cosine similarity).       | `cosineDistance(vec1, vec2)` |
| `euclideanDistance`  | Computes the Euclidean distance between two vectors.                        | `euclideanDistance(vec1, vec2)` |
| `doubleToVector`     | Converts a `DOUBLE[]` array into a `VECTOR`.                                | `doubleToVector([1.0, 2.0, 3.0])` |
| `vectorToDouble`     | Converts a `VECTOR` into a `DOUBLE[]` array.                                | `vectorToDouble(vec)` |
| `center`             | Computes the centroid (average) of a collection of vectors. Aggregate function. | `SELECT center(vecCol) FROM vectors` |

### CSV Format

The `flexible-csv` format extends the standard csv format with a configuration option `skip-header` to skip the first row in a CSV file (i.e. the header).

---

## Community Contributions

Contributions are welcome! Feel free to open an issue or submit a [pull request](https://github.com/DataSQRL/flink-sql-runner/pulls) on GitHub.

### Releasing
Release process is fully automated and driven by github release.  Just [create a new release](https://github.com/DataSQRL/flink-sql-runner/releases/new) and github action will take care of the rest.  The new release version will match the `tag`, so must use [semver](https://semver.org/) when selecting tag name.

### License
This project is licensed under the Apache 2 License. See the [LICENSE](https://github.com/DataSQRL/flink-sql-runner/blob/main/LICENSE) file for details.

### Contact & Support
For any questions or support, please open an [issue](https://github.com/DataSQRL/flink-sql-runner/issues) in the GitHub repository.
