# Security Policy

## Supported Versions

Each minor release is maintained on its own `release-x.y` branch (e.g. `release-0.11`).
Security fixes are applied to `main` and backported to the latest release branch only.

| Version | Supported          |
|---------|--------------------|
| 0.11.x  | :white_check_mark: |
| < 0.11  | :x:                |

We recommend always running the latest release of Flink SQL Runner.

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues, discussions, or pull requests.**

Instead, report them privately via [GitHub Security Advisories](https://github.com/DataSQRL/flink-sql-runner/security/advisories/new).
If you are unable to use GitHub, you can email us at [security@datasqrl.com](mailto:security@datasqrl.com).

Please include as much of the following as you can:

- A description of the vulnerability and its potential impact
- Affected component(s), such as the runner, a connector, a format, or a function library
- Affected version(s) and, if applicable, the Docker image tag
- Steps to reproduce, or a proof of concept
- Any suggested mitigation or fix

Vulnerabilities in Apache Flink itself should be reported to the Apache Flink project, following
its [security policy](https://flink.apache.org/what-is-flink/security/).

## What to Expect

- We will acknowledge your report within 5 business days.
- We will investigate, keep you informed of our progress, and may ask for additional details.
- Once a fix is available, we will publish a GitHub Security Advisory and credit you, unless you prefer to remain anonymous.

Please give us a reasonable amount of time to address the issue before any public disclosure.
