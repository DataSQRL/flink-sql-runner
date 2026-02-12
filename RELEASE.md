# Release Process for Flink SQL Runner

Releasing a new version of `flink-sql-runner` is simple and fully automated via GitHub Actions.
The project follows semantic versioning, and have separate release branches for every minor version (e.g. `release-1.0`, `release-1.1`).
The `main` branch points to the next upcoming minor (or major) version.

## Release Steps

1. If you release a patch version, make sure all the necessary commits are present on the relevant `release-x.y` branch
2. Create a new signed **tag** using [SemVer](https://semver.org/) (for example `1.0.0`, or `1.0.0-alpha.1`)
   ```sh
   git tag -s 1.0.0 -m 'Release version 1.0.0'
   git push origin 1.0.0
   ``` 
3. Go to the [GitHub Releases page](https://github.com/DataSQRL/flink-sql-runner/releases/new)
4. Fill in the release title and changelog (optional but recommended)
5. Click **"Publish release"**
6. In case of a new minor or major release, create a new `release-x.y` branch for the new version from `main` that points to the tagged commit

> [!WARNING]
>️ The tag name must be a valid SemVer string — it becomes the version number used in the Docker image and Maven artifact.

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
