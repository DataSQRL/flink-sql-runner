# Flink SQL Runner

<img src="docs/runner_logo.png" alt="Flink SQL Runner Logo" width="270" align="right" />

Tools and extensions for running Apache Flink SQL applications, including Docker images, data types, connectors, and formats.

This repository contains core components for running Flink SQL applications in production using the Flink Kubernetes Operator, without manual JAR assembly or custom infrastructure.



## Features

- 📝 **SQL Script Execution**: Run SQL scripts directly with Flink.
- 🧾 **Compiled Plan Execution**: Run pre-compiled Flink SQL plans to manage production deployments and versioning.
- 🔄 **Environment Variable Substitution**: Inject environment variables `${VAR}` into SQL scripts and configs at runtime.
- 📦 **JAR Dependency Management**: Reference local directories with required JARs (e.g. UDFs).
- 🌍 **Kubernetes-Friendly**: Built to run with the Flink Kubernetes Operator.
- 🔧 **Function Infrastructure**: Utilities for writing and loading UDFs as system functions.
- 🪄 **Flink Extensions**:
    - 💀 Dead-letter queue support in Kafka for poison message handling.
    - 🚀 Native JSON and Vector types with JSON format and PostgreSQL connector support.
    - 📚 Function libraries for additional functionality in FlinkSQL (advanced math, OpenAI, etc)
    - ⚙️ Additional configuration options for CSV format.

---

## Flink SQL Runner Usage

You can use the docker image to run FlinkSQL scripts or compiled plans locally or in Kubernetes.
The docker image contains the executable flink-sql-runner.jar file which supports the following command line arguments:

| Argument	      | Description                                                                                           |
 ----------------|-------------------------------------------------------------------------------------------------------|
| **--planfile** | 	Compiled plan (i.e. JSON file) to execute                                                            |
| --sqlfile      | 	FlinkSQL script to execute                                                                           |
| --config-dir   | 	Directory containing Flink configuration YAML files                                                  |
| --udfpath      | 	Path to jar files that implement user defined functions (UDFs) or other runtime extensions for Flink |

Note that the runner expects either a FlinkSQL script or a compiled plan - not both.

We strongly recommend to run compiled plans for production FlinkSQL applications since they support
lifecycle management of applications, are stable across Flink versions, and provide more control over
the executed jobgraph.
You can use the [SQRL compiler](https://github.com/DataSQRL/sqrl/) to compile FlinkSQL applications to compiled plans.

The configuration directory contains the [Flink configuration files](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/config/).

The UDF path points to a directory containing JAR files. Most commonly, the jars implement user defined functions that are invoked by the FlinkSQL application. They can also contain other runtime extension for Flink.

### Running Locally

To run Flink SQL Runner locally using Docker in a self-contained cluster (JobManager and TaskManager in a single container):

1. Create your SQL script
Put your Flink SQL (e.g., flink.sql) in a local directory, such as:

```bash
./sql-scripts/flink.sql
```

1. Run the Docker image
This starts a full standalone Flink session cluster in one container:

```bash
docker run --rm -it \
  -p 8081:8081 \
  -v "$PWD/sql-scripts":/flink/sql \
  --name flink \
  datasqrl/flink-sql-runner:0.1-flink-1.19 \
  bash -c "bin/start-cluster.sh && tail -f /dev/null"
```

1. Submit your SQL job
In a separate terminal, run:

```bash
docker exec -it flink flink run ./plugins/flink-sql-runner/flink-sql-runner.uber.jar --sqlfile /flink/sql/flink.sql
```

The job will be submitted to the embedded JobManager and executed using the local TaskManager.

1. Inspect output
If your SQL uses the print connector as a sink, you can check logs via:

```bash
docker exec -it flink bash -c "cat /opt/flink/log/$(ls /opt/flink/log | grep 'flink--taskexecutor' | grep '.out')"
```

Or use the Flink UI at http://localhost:8081 to monitor jobs.

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
    jarURI: http://raw.github.com/datasqrl/releases/0.6.0/flink-sql-runner.jar
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
<version>0.6.0</version>
</dependency>
```
- Gradle:

```groovy
implementation 'com.datasqrl.flinkrunner:flink-sql-runner:0.6.0'
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

### JSONB Type

This project adds a [binary JSON type](types/json-type) and associated functions for more efficient JSON handling that does not serialize from and to string repeatedly.

Native JSON type support is also extended to the [JSON format](formats/flexible-json-format) called `flexible-json` for writing JSON data as nested documents (instead of strings) as well as the [JDBC connector for PostgreSQL](connectors/postgresql-connector) to write JSON data to JSONB columns.

The binary JSON type is supported by [these system functions](docs/system-functions.md#jsonb-functions).

### Vector Type

This project adds a native [Vector type](types/vector-type) and associated functions for more efficient handling of vectors (e.g. for content embeddings).

Native Vector type support is also extended to the [JDBC connector for PostgreSQL](connectors/postgresql-connector) to write vector data to vector columns for the pgvector extension.

The native vector type is supported by [these system functions](docs/system-functions.md#vector-functions).

### Function Libraries

<img src="docs/sqrl_functions_logo.png" alt="Flink SQL Runner Logo" width="270" align="right" />

Implementation of FlinkSQL and SQRL functions that can be added as user-defined functions (UDFs) to support additional functionality.

* [Math](docs/library-functions.md#advanced-math): Advanced math functions
* [OpenAI](docs/library-functions.md#openai): Function for calling completions, structured data extraction, and vector embeddings.

## Usage

### Within DataSQRL

If you are using the [DataSQRL framework](https://github.com/DataSQRL/sqrl) to compile your SQRL project, you can import the function library as follows:

`IMPORT functions.[library-name].*`

where `[library-name]` is replaced with the name of the library.
To import a single function:

`IMPORT functions.[library-name].[function-name]`

### Flink SQL Runner

To use a function library with the Flink SQL Runner:

1. Copy the JAR file for the function library to the UDF directory that is passed as an argument.
2. Declare the function in your FlinkSQL script:
```sql
CREATE FUNCTION TheFunctionToAdd AS 'com.datasqrl.flinkrunner.[library-name].[function-name]';
```

where you replace `[library-name]` with the name of the function library and `[function-name]` with the name of the function.

### Custom Flink Implementation

If you are building your own FlinkSQL runner, you can depend on the function modules and load the functions into your project.


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
