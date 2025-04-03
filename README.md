# Flink-sql-runner

**Flink-sql-runner** is a utility designed to simplify the deployment of Apache Flink applications by managing environment variables, SQL script execution, compiled plan management, and JAR dependency resolution. This tool makes it easy to deploy Flink applications without needing to manually build JAR files.

---

## Features

- **SQL Script Execution**: Run SQL scripts directly within Flink.
- **Compiled Plan Support**: Execute pre-compiled Flink plans without hassle.
- **Environment Variable Substitution**: Automatically resolve and substitute environment variables in your configurations and SQL scripts.
- **JAR Dependency Management**: Simplifies pointing to directories with JAR files required by Flink jobs.
- **Kubernetes-Friendly**: Works seamlessly with the Flink Operator for Kubernetes deployments.
- **Maven and Docker Support**: Available on Maven Central and Docker Hub for easy setup.

---

## Installation

### Maven
Add the following to your pom.xml:

```xml
<dependency>
<groupId>com.datasqrl</groupId>
<artifactId>flink-sql-runner</artifactId>
<version>0.1</version>
</dependency>
```

### Docker
Pull the latest Docker image from Docker Hub:

```bash
docker pull datasqrl/flink-sql-runner:latest
```
---

## Usage
### Command-Line Interface
The tool can be executed via the command line with the following arguments:

| Argument	| Description                                |
 ----------- |--------------------------------------------|
|--sqlfile| 	SQL file to execute               |
|--planfile| 	Compiled plan JSON file       |
|--config-dir| 	Directory containing configuration YAML file       |
|--udfpath| 	Path to UDFs |

Example usage:
```bash
java -jar flink-sql-runner.jar \
--sqlfile /path/to/schema.sql \
--planfile /path/to/compiledplan.json \
--udfpath /path/to/jars
```

### Environment Variable Substitution
The tool automatically substitutes environment variables in your configuration files and SQL scripts, ensuring smooth deployment across different environments.

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

### Kubernetes Deployment with Flink Operator
Here's how to use **Flink Jar Runner** with the Flink Operator on Kubernetes:

1. Prepare Your Files: Ensure that your SQL scripts (`schema.sql`), compiled plans (`compiledplan.json`), and JAR files are accessible within your container.

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
    args: ["--sqlfile", "/opt/flink/usrlib/sql-scripts/schema.sql", "--planfile", "/opt/flink/usrlib/sql-scripts/compiledplan.json", "--udfpath", "/opt/flink/usrlib/jars"]
    parallelism: 1
    upgradeMode: stateless
```

1. Deploy with Helm:
```bash
helm install sql-example -f your-helm-values.yaml your-helm-chart
```
---

### Example
Running Flink jar-runner locally:

```bash
java -jar flink-sql-runner.jar \
--sqlfile /path/to/schema.sql \
--planfile /path/to/compiledplan.json \
--udfpath /path/to/jars
```

---
## Getting the JAR
You can download the JAR directly from Maven Central or Docker Hub:

- Maven:

```xml
<dependency>
<groupId>com.datasqrl</groupId>
<artifactId>flink-sql-runner</artifactId>
<version>0.1</version>
</dependency>
```
- Docker:

```bash
docker pull datasqrl/flink-sql-runner:0.1-sqrlv0.5.10
```

## Contributing
Contributions are welcome! Feel free to open an issue or submit a [pull request](https://github.com/DataSQRL/flink-sql-runner/pulls) on GitHub.

### Releasing
Release process is fully automated and driven by github release.  Just [create a new release](https://github.com/DataSQRL/flink-sql-runner/releases/new) and github action will take care of the rest.  The new release version will match the `tag`, so must use [semver](https://semver.org/) when selecting tag name.

## License
This project is licensed under the Apache 2 License. See the [LICENSE](https://github.com/DataSQRL/flink-sql-runner/blob/main/LICENSE) file for details.

## Contact
For any questions or support, please open an [issue](https://github.com/DataSQRL/flink-sql-runner/issues) in the GitHub repository.
