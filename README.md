# Flink jar-runner

**Flink jar-runner** is a utility designed to simplify the deployment of Apache Flink applications by managing environment variables, SQL script execution, compiled plan management, and JAR dependency resolution. This tool makes it easy to deploy Flink applications without needing to manually build JAR files.

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
<artifactId>flink-jar-runner</artifactId>
<version>1.0.0</version>
</dependency>
```

### Docker
Pull the latest Docker image from Docker Hub:

```bash
docker pull datasqrl/flink-jar-runner:latest
```
---

## Usage
### Command-Line Interface
The tool can be executed via the command line with the following arguments:

| Argument	| Description                                |
 ----------- |--------------------------------------------|
|--sql-schema| 	Path to the SQL schema file               |
|--compiled-plan| 	Path to the compiled plan JSON file       |
|--system-jars| 	Directory containing additional JAR files |
|--local-mode| 	tbd                                       |

Example usage:
```bash
java -jar flink-jar-runner.jar \
--sql-schema /path/to/schema.sql \
--compiled-plan /path/to/compiledplan.json \
--system-jars /path/to/jars
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
  image: datasqrl/flink-jar-runner:latest
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
    jarURI: http://raw.github.com/datasqrl/releases/1.0.0/flink-jar-runner.jar
    args: ["--sql-schema", "/opt/flink/usrlib/sql-scripts/schema.sql", "--compiled-plan", "/opt/flink/usrlib/sql-scripts/compiledplan.json", "--system-jars", "/opt/flink/usrlib/jars"]
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
java -jar flink-jar-runner.jar \
--sql-schema /path/to/schema.sql \
--compiled-plan /path/to/compiledplan.json \
--system-jars /path/to/jars \
--local-mode
```

---
## Getting the JAR
You can download the JAR directly from Maven Central or Docker Hub:

- Maven:

```xml
<dependency>
<groupId>com.datasqrl</groupId>
<artifactId>flink-jar-runner</artifactId>
<version>1.0.0</version>
</dependency>
```
- Docker:

```bash
docker pull datasqrl/flink-jar-runner:latest
```

## Contributing
Contributions are welcome! Feel free to open an issue or submit a [pull request](https://github.com/DataSQRL/flink-jar-runner/pulls) on GitHub.

## License
This project is licensed under the MIT License. See the [LICENSE](https://github.com/DataSQRL/flink-jar-runner/blob/main/LICENSE) file for details.

## Contact
For any questions or support, please open an [issue](https://github.com/DataSQRL/flink-jar-runner/issues) in the GitHub repository.