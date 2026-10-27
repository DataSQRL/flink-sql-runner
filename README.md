[![GitHub release](https://img.shields.io/github/v/release/DataSQRL/flink-sql-runner?sort=semver)](https://github.com/DataSQRL/flink-sql-runner/releases)
[![Docker Image Version](https://img.shields.io/docker/v/datasqrl/flink-sql-runner?sort=semver)](https://hub.docker.com/r/datasqrl/flink-sql-runner/tags)
[![Maven Central](https://img.shields.io/maven-central/v/com.datasqrl.flinkrunner/flink-sql-runner)](https://repo1.maven.org/maven2/com/datasqrl/flinkrunner/flink-sql-runner/)

# Flink SQL Runner

<img src="stdlib-docs/img/runner_logo.png" alt="Flink SQL Runner Logo" width="300" align="right" />

Tools and extensions for running Apache Flink SQL applications, including Docker images, data types, connectors, function libraries, and formats.

This repository contains core components for running Flink SQL applications in production using the Flink Kubernetes Operator, without manual JAR assembly or custom infrastructure.

The individual components are modular and the project is composable to make it easy to create your own custom Flink SQL runner.

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
    - 📚 Function libraries for additional functionality in Flink SQL (advanced math, OpenAI, etc)

---

## Flink SQL Runner Usage

You can use the docker image to run Flink SQL scripts or compiled plans locally or in Kubernetes.
The docker image contains the executable flink-sql-runner.jar file which supports the following command line arguments:

| Argument           | Description                                                                                                                                                                                         |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `-p, --planfile`   | Compiled plan (i.e. JSON file) to execute                                                                                                                                                           |
| `-s, --sqlfile`    | Flink SQL script to execute                                                                                                                                                                         |
| `-c, --config-dir` | Directory containing the [Flink configuration YAML file](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/)                                                         |
| `-u, --udfpath`    | Path to folder that contains JAR files that implement user defined functions (UDFs) or other runtime extensions for Flink                                                                           |
| `-m, --mode`       | Optional argument to specify [Flink execution mode](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/dev/datastream/execution_mode/) (`STREAMING` (default), `BATCH`, or `AUTOMATIC`) |

> [!WARNING]
> The runner expects either a Flink SQL script or a compiled plan - not both.
> 
> The `--mode` argument - even if it is explicitly set - will be ignored if the Flink YAML configuration set via `--config-dir` contains `execution.runtime-mode`.

We strongly recommend to run compiled plans for production Flink SQL applications since they support
lifecycle management of applications, are stable across Flink versions, and provide more control over
the executed JobGraph.
You can use the [SQRL compiler](https://github.com/DataSQRL/sqrl/) to compile Flink SQL applications to compiled plans.

### Running Locally

To run Flink SQL Runner locally using Docker in a self-contained cluster (JobManager and TaskManager in a single container):

1\. Create your SQL script
Put your Flink SQL (e.g., `flink.sql`) in a local directory, such as:

```bash
./sql-scripts/flink.sql
```

2\. Run the Docker image
This starts a full standalone Flink session cluster in one container:

```bash
docker run -d --rm -it \
  -p 8081:8081 \
  -v "$PWD/sql-scripts":/flink/sql \
  --name runner \
  datasqrl/flink-sql-runner:0.11.0-flink-2.3 \
  cluster
```

3\. Submit your SQL job
In a separate terminal, run:

```bash
docker exec -it runner sql-runner --sqlfile /flink/sql/flink.sql
```

The job will be submitted to the embedded JobManager and executed using the local TaskManager.

> [!NOTE]  
> The [`sql-runner`](https://github.com/DataSQRL/flink-sql-runner/blob/main/flink-sql-runner/src/main/docker/sql-runner) wrapper submits the runner through `flink run` so you do not need to specify the main class or placeholder JAR manually.
> The placeholder JAR is necessary because `sql-runner.jar` is shipped under Flink's `lib/` fodler, while the Flink CLI still requires a job JAR argument for `flink run`.
> By default, all arguments are passed to the SQL runner, for example `sql-runner --sqlfile /flink/sql/flink.sql`.
> To pass options to `flink run`, put them before `--`; arguments after `--` are passed to the SQL runner, for example `sql-runner -s /path/to/savepoint -- --planfile /flink/plan.json`.
> You can also omit using `sql-runner` and directly use `flink run`, just make sure to pass the main class and use the `noop.jar`.

4\. Inspect output
If your SQL uses the print connector as a sink, you can check logs via:

```bash
docker exec -it runner bash -c "cat /opt/flink/log/$(ls /opt/flink/log | grep 'flink--taskexecutor' | grep '.out')"
```

Or use the Flink UI at http://localhost:8081 to monitor jobs.

### Running in Kubernetes with Flink Operator

Here's how to use the Flink Jar Runner with the Flink Operator on Kubernetes:

1\. Prepare Your Files: Ensure that your SQL scripts (`statements.sql`) or compiled plans (`compiled_plan.json`), and JAR files are accessible within your container.

Example Helm chart configuration:

```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: sql-example
spec:
  image: datasqrl/flink-sql-runner:latest
  flinkVersion: v2_3
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
    jarURI: http://raw.github.com/datasqrl/releases/0.11.0/flink-sql-runner.jar
    args: ["--sqlfile", "/opt/flink/usrlib/sql-scripts/statements.sql", "--planfile", "/opt/flink/usrlib/sql-scripts/compiled_plan.json", "--udfpath", "/opt/flink/usrlib/jars"]
    parallelism: 1
    upgradeMode: stateless
```

> [!WARNING]
> Configure either the SQL script OR the compiled plan - not both.

1. Deploy with Helm:
```bash
helm install sql-example -f <your-helm-values>.yaml <your-helm-chart>
```

### Environment Variable Substitution

Flink SQL Runner automatically substitutes environment variables in your configuration files, SQL scripts, and compiled plans for secrets and environment specific configuration.
Environment variables must be of the form `${ENV_VARIABLE}` and inside of strings.
They also support bash-style defaults using `${ENV_VARIABLE:-default}` or `${ENV_VARIABLE:=default}`.

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

### Default Environment Variables

The Flink SQL Runner automatically provides default values for deployment-specific environment variables if they are not already set in your environment.
The following environment variables are automatically set with default values:

| Variable               | Default Value                | Description                                                                           |
|------------------------|------------------------------|---------------------------------------------------------------------------------------|
| `DEPLOYMENT_ID`        | Random UUID                  | A unique identifier for the deployment (e.g., `550e8400-e29b-41d4-a716-446655440000`) |
| `DEPLOYMENT_TIMESTAMP` | Current time in milliseconds | The deployment timestamp in milliseconds since epoch (e.g., `1704067200000`)          |

These defaults are applied at runtime before environment variable substitution occurs, making them available for use in SQL scripts and compiled plans even when not explicitly set in your environment.

Example usage in a SQL script:
```sql
SELECT '${DEPLOYMENT_ID}' AS deployment_id, CAST('${DEPLOYMENT_TIMESTAMP}' AS BIGINT) AS deployment_timestamp;
```

### Building Your Own Flink SQL Runner

The Flink SQL runner is published to Maven Central and you can add it as a dependency in your project to extend
the runner to suit your needs.

- Maven:

```xml
<dependency>
  <groupId>com.datasqrl.flinkrunner</groupId>
  <artifactId>flink-sql-runner</artifactId>
  <version>0.11.0</version>
</dependency>
```
- Gradle:

```groovy
implementation 'com.datasqrl.flinkrunner:flink-sql-runner:0.11.0'
```

### Profiling with async-profiler

The image ships [async-profiler](https://github.com/async-profiler/async-profiler) under `/opt/async-profiler`,
with the `asprof` launcher on the `PATH`. No sidecar, extra image, or JVM agent argument is needed — it attaches
to a running JVM.

Find the JVM to profile, then attach to it:

```bash
kubectl exec -it <taskmanager-pod> -- jps -l
# 1426 org.apache.flink.runtime.taskexecutor.TaskManagerRunner

kubectl exec -it <taskmanager-pod> -- asprof -e cpu -d 30 -f /tmp/flame.html 1426
kubectl cp <taskmanager-pod>:/tmp/flame.html ./flame.html
```

Profile the TaskManager, not the JobManager — the record processing happens there.

Useful modes:

| Command | What it shows |
|---------|---------------|
| `asprof -e cpu -d 30 -f /tmp/cpu.html <pid>` | Where CPU time goes (default choice) |
| `asprof -e wall -t -d 30 -f /tmp/wall.html <pid>` | Wall-clock per thread — blocked/idle time, useful for backpressure |
| `asprof -e alloc -d 30 -f /tmp/alloc.html <pid>` | Allocation pressure driving GC |
| `asprof -e lock -d 30 -f /tmp/lock.html <pid>` | Lock contention |
| `asprof -d 30 -o collapsed -f /tmp/out.txt <pid>` | Folded stacks, greppable and diffable |

Notes:

- Write output to a path inside the container (e.g. `/tmp`). The profiled JVM writes the file itself as the
  `flink` user, so a mounted host volume usually fails with `Could not open output file`.
- Only one profiling session per JVM at a time. `[ERROR] Profiler already started` means a previous run is still
  active — collect it with `asprof stop -f /tmp/flame.html <pid>`.
- For precise inlined-frame attribution, add `-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints` to
  `env.java.opts.all`. It is not on by default; stacks are already usable without it.
- CPU profiling falls back to `ctimer` when `perf_events` are restricted (the common case for an unprivileged
  container), so no extra Kubernetes capabilities are required. To use hardware `perf_events` instead, the pod
  needs `SYS_ADMIN` and a host `kernel.perf_event_paranoid` of `1` or lower.

### Checking connectivity with netcat

The image ships [netcat](https://linuxize.com/post/netcat-nc-command-with-examples/) (`nc`, the OpenBSD
flavor) so connectivity to a Kafka broker, a Postgres instance, or any other sink can be checked from inside
the pod, without an ephemeral debug container:

```bash
kubectl exec -it <taskmanager-pod> -- nc -zv kafka.default.svc.cluster.local 9092
kubectl exec -it <taskmanager-pod> -- nc -zv postgres 5432
```

Add `-w 5` to bound the wait when a host silently drops packets, and `-u` to probe a UDP port.

---

## Flink Extensions

The Flink SQL Runner contains a few extensions to the Flink runtime.

### Dead-Letter-Queue Support for Kafka Sources

If a Flink SQL application fails to deserialize a message from a Kafka topic, the entire job can fail. 

This project implements the `kafka-safe` and `upsert-kafka-safe` [connectors](connectors/kafka-safe) which extend the respective kafka connectors with dead-letter-queue support, so that messages which fail to deserialize can be logged, or sent to a dead-letter-queue, instead of failing the job.

In addition to the configuration options exposed by the original kafka connectors, the `-safe` versions support the following optional configuration options:

| Options                    | Default | Type   | Description                                                                                                                       |
|----------------------------|---------|--------|-----------------------------------------------------------------------------------------------------------------------------------|
| scan.deser-failure.handler | none    | String | Use `log` to output failed messages to the logger, `kafka` to output failed messages to a kafka topic, or `none` to fail the job. |
| scan.deser-failure.topic   | -       | String | The topic for the dead-letter-queue that failed messages are written to. Required when the handler is configured to `kafka`.      |

> [!NOTE]  
> The dead-letter-queue producer will use the same Kafka configuration that is provided for the Flink SQL table that reads the data.

### Kafka Records Without a Key

When a table sets `key.fields`, the `kafka` connector decodes the record key on read and silently drops records that have no key.
With `'value.fields-include' = 'ALL'` the value already contains every key field, so the `kafka-safe` connector reads the key fields from the value instead and keeps such records.

| Options                         | Default | Type    | Description                                                                                                                                                       |
|---------------------------------|---------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| scan.key-fields.read-from-value | true    | Boolean | Read the key fields from the value when it contains every key field. Set to `false` to decode the key and drop records without a key, like the `kafka` connector. |

The option only affects reading, writing always sets the record key from `key.fields`. The `upsert-kafka-safe` connector always decodes the key, since upserts and deletes need it.

### Adaptive Kafka Source Watermarks

The `kafka-safe` and `upsert-kafka-safe` connectors can generate source watermarks from Kafka record timestamps.
Enable them with `SOURCE_WATERMARK()` on a Kafka timestamp metadata column:

```sql
CREATE TABLE events (
  id BIGINT,
  payload STRING,
  kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR kafka_timestamp AS SOURCE_WATERMARK()
) WITH (
  'connector' = 'kafka-safe',
  'topic' = 'events',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'events-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);
```

The generator waits until it has observed `scan.source-watermark.min-records` records, then subtracts an adaptive out-of-orderness delay from the greatest Kafka record timestamp seen.
The delay is the configured quantile of recent record lateness samples, constrained by the configured minimum and maximum.
This absorbs typical out-of-order records without allowing a rare late record to indefinitely delay event-time progress.
Watermarks are monotonic and use the Kafka record timestamp, not an event-time field in the payload.

| Option                                            | Default       | Description                                                                                                                               |
|---------------------------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `scan.source-watermark.min-records`               | `250`         | Records to observe before emitting adaptive source watermarks. Must be greater than zero.                                                 |
| `scan.source-watermark.min-out-of-orderness`      | `50 ms`       | Lower bound for the adaptive out-of-orderness delay.                                                                                      |
| `scan.source-watermark.max-out-of-orderness`      | `3 d`         | Upper bound for the adaptive out-of-orderness delay. Must be at least the minimum.                                                        |
| `scan.source-watermark.out-of-orderness-quantile` | `0.95`        | Quantile of observed record lateness used as the delay. Must be greater than zero and no greater than one.                                |
| `scan.watermark.emit.strategy`                    | Flink default | Flink source-watermark emission strategy: `on-event` emits as records arrive; `on-periodic` emits on Flink's periodic watermark interval. |
| `scan.watermark.idle-timeout`                     | Flink default | How long a source split must be idle before Flink marks it idle. Applies only when idle advancement is disabled.                          |

#### Idle Watermark Advancement

By default, a source watermark cannot advance after Kafka traffic stops, so a final event-time window may remain open.
Set `scan.source-watermark.idle-advance-timeout` to advance the watermark conservatively using wall-clock time once a partition is idle, that is, once it has read nothing for that duration:

```sql
'scan.source-watermark.idle-advance-timeout' = '1 min',
'scan.source-watermark.idle-advance-safety-margin' = '10 s'
```

Idle advancement supports only the `topic` table option, not `topic-pattern`. Configuring `topic-pattern` with idle advancement will result in a failure.
The connector checks this through Kafka's `AdminClient` and requires permission to describe the topic configuration and its offsets.
If Kafka cannot be reached or the topic is not configured for `LogAppendTime`, idle advancement pauses rather than advancing unsafely.
Do not combine idle advancement with `scan.watermark.idle-timeout`. The connector ignores that option when idle advancement is enabled, because once Flink marks a partition idle, it stops letting the connector advance that partition's watermark.

The time a partition spends paused for watermark alignment or under backpressure does not count towards the idle-advance timeout.
A partition can be idle with records still unread, when the source is busy with other partitions or rate-limited, so being idle alone never advances it.
For an idle partition, the connector asks the broker for the end of its log, and the watermark advances only if the partition has been read to that end.
It then moves to the wall-clock time at which the broker was asked, minus the adaptive out-of-orderness delay and the safety margin, also when the partition's last record was read from a backlog, such as when a new job starts from the earliest offset.
An idle partition with unread records, or whose log end cannot be fetched, keeps the watermark its records give it.
An idle partition that has not produced a row yet is marked idle instead, which leaves it out of the source watermark until it does.
A partition's first row moves its watermark to just below that row at once, without waiting for `scan.source-watermark.min-records` records, so downstream operators see the source subtask as active, and hold their watermark below the row, before any watermark that would pass it.

The log ends of all of a source subtask's partitions are fetched together, at most once per broker-check TTL, which therefore also bounds how far an idle partition's watermark trails the wall clock: keep it close to the idle-advance timeout.
The safety margin must cover clock skew between the Kafka brokers and the TaskManagers, and the time a record takes to become readable after the broker timestamps it.

For transactional topics, commit markers take offsets that the consumer never returns, and so do the records of aborted transactions for a `read_committed` source.
The consumer moves its position past them, and a consumer interceptor that the connector adds to `interceptor.classes` reports that position.
A partition counts as read to the end once its split has read every record the consumer returned and the consumer's position has reached the end of the log, however many markers the log ends in.
A `read_committed` source does not advance a partition while a transaction is open on it.

| Option                                                    | Default        | Description                                                                                                                       |
|-----------------------------------------------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------|
| `scan.source-watermark.idle-advance-timeout`              | `0` (disabled) | How long a partition must read nothing to be idle, after which idle advancement may begin. Must not be negative.                  |
| `scan.source-watermark.idle-advance-safety-margin`        | `10 s`         | Extra delay subtracted from wall-clock-derived watermarks to avoid closing windows too early.                                     |
| `scan.source-watermark.idle-advance-broker-check-timeout` | `1 s`          | Timeout for each Kafka broker readiness, timestamp-type and log-end check. Must be greater than zero.                             |
| `scan.source-watermark.idle-advance-broker-check-ttl`     | `10 s`         | How long broker check results, including partition log ends, are cached before they are fetched again. Must be greater than zero. |

### Conflict Handling for PostgreSQL Sinks

The [JDBC connector for PostgreSQL](connectors/postgresql-connector) (`jdbc-sqrl`) supports configurable behavior when an inserted row conflicts with an existing row on the primary key. The action is selected via the following options:

| Options                           | Default | Type   | Description                                                                                                                                          |
|-----------------------------------|---------|--------|------------------------------------------------------------------------------------------------------------------------------------------------------|
| sink.on-conflict.action           | update  | Enum   | One of `update`, `timestamp`, or `ignore`. Selects how primary-key conflicts are resolved on the sink.                                               |
| sink.on-conflict.timestamp-column | -       | String | The column used to compare incoming and existing rows when the action is `timestamp`. Required when `sink.on-conflict.action` is set to `timestamp`. |

The `sink.on-conflict.action` values map to the following PostgreSQL `ON CONFLICT` behaviors:

- `update`: The existing row is overwritten with the incoming row (`ON CONFLICT (...) DO UPDATE SET ...`). This is the default.
- `timestamp`: The existing row is overwritten only if the incoming row's `sink.on-conflict.timestamp-column` value is greater than or equal to the existing row's. 
  Otherwise, the conflicting insert is silently skipped, so out-of-order events do not roll back newer state.
- `ignore`: The existing row is kept, and the conflicting insert is silently skipped (`ON CONFLICT (...) DO NOTHING`).


### JSONB Type

This project adds a [binary JSON type](types/json-type) and associated functions for more efficient JSON handling that does not serialize from and to string repeatedly.

Native JSON type support is also extended to the [JSON format](formats/flexible-json-format) called `flexible-json` for writing JSON data as nested documents (instead of strings) as well as the [JDBC connector for PostgreSQL](connectors/postgresql-connector) to write JSON data to JSONB columns.

The binary JSON type is supported by [these system functions](stdlib-docs/system-functions.yml).

### Vector Type

This project adds a native [Vector type](types/vector-type) and associated functions for more efficient handling of vectors (e.g. for content embeddings).

Native Vector type support is also extended to the [JDBC connector for PostgreSQL](connectors/postgresql-connector) to write vector data to vector columns for the `pgvector` extension.

The native vector type is supported by [these system functions](stdlib-docs/system-functions.yml).

### Function Libraries

<img src="stdlib-docs/img/sqrl_functions_logo.png" alt="Flink SQL Runner Logo" width="300" align="right" />

Implementation of Flink SQL and SQRL functions that can be added as user-defined functions (UDFs) to support additional functionality.

* [Math](stdlib-docs/library-functions.yml): Advanced math functions
* [Iceberg](stdlib-docs/library-functions.yml): Helper functions for advanced Iceberg functionality.
* [OpenAI](stdlib-docs/library-functions.yml): Function for calling completions, structured data extraction, and vector embeddings.

## Usage

### Within DataSQRL

If you are using the [DataSQRL framework](https://github.com/DataSQRL/sqrl) to compile your SQRL project, you can import the function library as follows:

`IMPORT stdlib.[library-name].*`

where `[library-name]` is replaced with the name of the library, e.g. `stdlib.math.*`.

To import a single function:

`IMPORT stdlib.[library-name].[function-name]`

e.g. `stdlib.text.split`.

### Flink SQL Runner

To use a function library with the Flink SQL Runner:

1. Copy the JAR file for the function library to the UDF directory that is passed as an argument.
2. Declare the function in your Flink SQL script:
```sql
CREATE FUNCTION TheFunctionToAdd AS 'com.datasqrl.flinkrunner.[library-name].[function-name]';
```

where you replace `[library-name]` with the name of the function library and `[function-name]` with the name of the function.

### Custom Flink Implementation

If you are building your own Flink SQL runner, you can depend on the function modules and load the functions into your project.
