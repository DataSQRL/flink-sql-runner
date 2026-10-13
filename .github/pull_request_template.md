## What does this change?

<!-- One or two sentences. Link the issue it fixes, e.g. "Closes #123". -->

## Area

- [ ] Runner CLI and Docker image (`flink-sql-runner`)
- [ ] Connectors (`connectors`)
- [ ] Formats (`formats`)
- [ ] Function libraries (`stdlib`)
- [ ] Environment utilities (`env-utils`)
- [ ] Tests and samples (`testing`)
- [ ] Build / CI / dependencies
- [ ] Docs (`README.md`, `stdlib-docs`)

## How did you test it?

<!-- For example: added a unit test, added an integration test with Testcontainers, or ran the runner locally with `--sqlfile`. -->

## Checklist

- [ ] The PR title starts with a type such as `feat:`, `fix:` or `docs:`, optionally with a scope (e.g. `fix(flexible-json):`)
- [ ] `mvn clean install` passes locally and the code is formatted with `mvn spotless:apply`
- [ ] New or changed behavior is covered by tests (`given_when_then` naming, AssertJ assertions)
- [ ] New dependency and plugin versions are defined as properties in the root `pom.xml`
- [ ] Commits are signed off and signed (`git commit -s -S`)
- [ ] `README.md` and `stdlib-docs` are updated if user-facing behavior or functions changed
