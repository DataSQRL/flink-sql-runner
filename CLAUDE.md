# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a multi-module Maven project for the Flink SQL Runner - a tool and library ecosystem for running Apache Flink SQL applications. The project is structured as a parent POM with multiple modules providing different functionality.

## Module Structure

- `flink-sql-runner/` - Main CLI application and Docker image
- `connectors/` - Custom Flink connectors (kafka-safe, postgresql)
- `formats/` - Custom data formats (flexible-csv, flexible-json)
- `types/` - Custom data types (json-type, vector-type)
- `stdlib/` - Standard library modules with system functions (json, math, text, vector, openai)
- `testing/` - Testing utilities and sample UDF projects

## Common Commands

### Building and Testing
```bash
# Full build with all checks
mvn clean install -Pci,flink-1.19

# Fast build (skips tests, formatting, license checks)
mvn clean install -Pfast

# Run tests only
mvn test

# Run integration tests
mvn verify

# Code formatting check
mvn spotless:check

# Apply code formatting
mvn spotless:apply

# License header check
mvn license:check

# Apply license headers
mvn license:format
```

### Running the Application
```bash
# Run SQL file
java -jar flink-sql-runner/target/flink-sql-runner.jar --sqlfile script.sql

# Run compiled plan
java -jar flink-sql-runner/target/flink-sql-runner.jar --planfile plan.json

# With custom configuration
java -jar flink-sql-runner/target/flink-sql-runner.jar --sqlfile script.sql --config-dir config/

# With UDF path
java -jar flink-sql-runner/target/flink-sql-runner.jar --sqlfile script.sql --udfpath /path/to/udfs
```

## Technology Stack

- **Java 17** - Base language version
- **Apache Flink 2.2.0** - Stream processing framework
- **Maven** - Build system
- **Lombok** - Code generation
- **PicoCLI** - Command line interface
- **JUnit 6** - Testing framework
- **Testcontainers** - Integration testing
- **Docker** - Containerization

## Development Notes

### Code Style
- Uses Google Java Format for code formatting
- Enforced via Spotless Maven plugin
- License headers are automatically managed
- Separate formatting rules for Flink-specific code (AOSP style)

### Testing
- Unit tests: `mvn test`
- Integration tests: `mvn verify` (includes failsafe plugin)
- Test coverage enforced at 70% minimum via JaCoCo

### Architecture
The main application (`CliRunner`) processes command-line arguments and delegates to `BaseRunner` which:
1. Initializes Flink configuration
2. Resolves environment variables in SQL/JSON files
3. Creates `SqlExecutor` to run SQL scripts or compiled plans
4. Handles UDF loading and system function registration

### Key Classes
- `CliRunner` - Main entry point and CLI argument processing
- `BaseRunner` - Core execution logic and configuration
- `SqlExecutor` - Flink SQL script and plan execution
- `EnvVarResolver` - Environment variable substitution in configs

### Adding New Modules
New modules should:
1. Follow the existing directory structure
2. Include proper Maven coordinates under `com.datasqrl.flinkrunner`
3. Add to parent POM modules section
4. Include common dependencies from parent (Lombok, JUnit, etc.)
