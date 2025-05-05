# Release Process for flink-sql-runner

Releasing a new version of `flink-sql-runner` is simple and fully automated via GitHub Actions.

## Steps to Create a New Release

1. Go to the [GitHub Releases page](https://github.com/DataSQRL/flink-sql-runner/releases/new).
2. Set the **tag name** using [Semantic Versioning (SemVer 2.0.0)](https://semver.org/), e.g.:
   - `1.0.0`
   - `1.2.3-alpha.1`
   - `2.0.0+build.456`
3. Fill in the release title and description (e.g., changelog), and click **Publish release**.

⚠️ The tag name must be a valid SemVer string — it becomes the version number used in the Docker image and Maven artifact.

## What Happens Next

Once the release is created:
- GitHub Actions will automatically trigger a build:
  - Example: [Build Job](https://github.com/DataSQRL/flink-sql-runner/actions/runs/14844543209/job/41675174215)
- In a few minutes, the release will be published to:
  - **Docker Hub**: [`datasqrl/flink-sql-runner`](https://hub.docker.com/layers/datasqrl/flink-sql-runner/)
  - **Maven Central**: [`com.datasqrl.flinkrunner:flink-sql-runner`](https://repo1.maven.org/maven2/com/datasqrl/flinkrunner/flink-sql-runner/)

## Notes

- No manual deployment steps are required.
- Docker images and Maven artifacts are tagged with the exact version string from the GitHub release tag.

