# Contributing to Flink SQL Runner

Thanks for your interest in Flink SQL Runner! We welcome contributions from anyone: code, documentation,
bug reports, and ideas.

By participating, you agree to follow our [Code of Conduct](CODE_OF_CONDUCT.md).
To report a security vulnerability, follow the [Security Policy](SECURITY.md) and do not open a public issue.

## How to Contribute

### 1. Discuss

- **Questions and ideas**: Open a [feature request](https://github.com/DataSQRL/flink-sql-runner/issues/new?template=feature_request.md).
  For new features or larger changes, please discuss the approach there first, so we can agree on
  the direction before you invest time in an implementation. General questions about DataSQRL can
  go to the [DataSQRL Discussions](https://github.com/DataSQRL/sqrl/discussions).
- **Bugs**: Search the [GitHub Issues](https://github.com/DataSQRL/flink-sql-runner/issues)
  to see if it is already reported. If not, open a new [bug report](https://github.com/DataSQRL/flink-sql-runner/issues/new?template=bug_report.md)
  with a clear description, steps to reproduce, and the Flink SQL Runner version (or Docker image tag) you are using.

Small fixes, such as typos or obvious bugs, can go straight to a pull request.

### 2. Get Assigned

Comment on the issue you want to work on, so others know it is being worked on and we can avoid duplicate effort.

### 3. Implement

1. Fork the repository and create a branch from `main`.
2. Make your change, following the [code style](#code-style) of the project.
3. Add or update tests that cover your change. Use the `given_when_then` naming pattern and AssertJ assertions.
4. Format the code and make sure the build passes locally (see [Development Setup](#development-setup)).
5. [Sign off and sign](#sign-your-work) your commits.

### 4. Open a Pull Request

- Reference the related issue in the description (e.g. `Closes #123`).
- Use a [Conventional Commits](https://www.conventionalcommits.org) title.
  Allowed types: `feat`, `fix`, `chore`, `test`, `docs`, `refactor`, `ci`, `build`, `perf`, `revert`.
  Add the affected module or function library as a scope where it helps (e.g. `feat(openai): Add reasoning effort support`).
- Keep pull requests focused on a single change. Unrelated changes belong in separate pull requests.
- Make sure all CI checks pass.

### 5. Review and Merge

A contributor or committer will review your pull request and may ask for changes or more
information. Once it is approved and CI is green, a committer will merge it. Fixes may be
backported to the supported `release-x.y` branch when applicable.

## Development Setup

### Prerequisites

- Java 17
- Maven 3.9+, or the bundled Maven Wrapper (`./mvnw`)
- Docker (required for tests that use Testcontainers)

### Build

Run a full build first, and let all tests run:

```bash
mvn clean install
```

Besides compiling and running the unit and integration tests, this checks code formatting, license headers,
enforcer rules, and test coverage (at least 70%). The runner uber JAR is written to
`flink-sql-runner/target/flink-sql-runner.uber.jar`, next to the generated `Dockerfile`.

### Useful Commands

```bash
# Quick build: skip tests and checks
mvn clean install -Pfast

# Format code (Google Java Format) and add missing license headers
mvn spotless:apply license:format

# Unit tests of a single module
mvn test -pl stdlib/stdlib-openai

# Integration tests (requires Docker)
mvn verify

# Build the Docker image locally, after `mvn install`
docker build -t flink-sql-runner:local flink-sql-runner/target
```

See [Running Locally](README.md#running-locally) for how to run a SQL script with the Docker image.

### Code Style

- Java code is formatted with Google Java Format and enforced by the Spotless Maven plugin.
  Patched Flink classes under `org.apache.flink` use the AOSP style instead, to keep them close to upstream.
- Every source file needs the Apache 2.0 license header, which `mvn license:format` adds for you.
- To apply formatting automatically before each commit, install the Git hooks:

  ```bash
  ./scripts/install-git-hooks.sh
  ```

### Tips

- Add `-T <threads>` (e.g. `-T 1C`) to build modules in parallel.
- On case-insensitive file systems (macOS, Windows), run `git config core.ignorecase false` so Git
  picks up file renames that only change letter casing.

## Sign Your Work

The _sign-off_ is a simple line at the end of the message for a commit. All commits need to be signed.
Your signature certifies that you wrote the patch or otherwise have the right to contribute the material
(see [Developer Certificate of Origin](https://developercertificate.org)):

```
This is my commit message

Signed-off-by: John Doe <john.doe@example.com>
```

Git has a [`-s`](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---signoff) command line option to
append this automatically to your commit message:

```bash
$ git commit -s -m "This is my commit message"
```

Unfortunately, anyone with write access to a repository can easily impersonate another user.
Consider how each commit is associated with a user via their email address.
There's nothing stopping someone from using someone else's email address to make commits.
The issue extends to the signoff message as well.

That's why it's advisable to sign your commits with a unique key. Git offers support for various types of keys,
and this time, we'll walk you through signing your commits using GPG.

#### Setup Git using GPG

Ensure that gpg is installed on your system.

On MacOS:
```bash
brew install gpg
```

Generate your key:
```bash
gpg --full-generate-key
```

Recommended settings:
- **key kind:** (1) RSA and RSA
- **key size:** 4096
- **key validity:** key does not expire
  (you can revoke keys, so unless you don't lose access to your key it is more convenient)
- **real name:** it is recommended using your real name
- **email address:** it is recommended to use the same email address here that you use to commit your work
- **comment:** it is recommended to use different keys for different use-cases / organizations.
  If you use the same email across organizations, you can distinguish your keys with the help of this field.
  eg.: "CODE SIGNING KEY" or "DATASQRL CODE SIGNING KEY"

You can create the new key by selecting "(O)kay"

To view your key, you issue this command:
```bash
gpg --list-secret-keys --keyid-format=long
```

The output should look like this:
```
sec   rsa4096/D2A162EAE1016F3G 2024-04-05 [SC]
      AFB8C2DEFEA93470D81C84E7D2A162EAE1016F3G
uid                 [ultimate] John Doe (CODE SIGNING KEY) <john.doe@example.com>
ssb   rsa4096/2F7B9EAC4D6F8150 2024-04-05 [E]
```

To use the above key to sign your commits cd into a repository and issue these commands:
```bash
git config user.signingkey D2A162EAE1016F3G
git config commit.gpgsign true
```

You also need to add the public key to your github profile for the signing to be verified.

To do so, go to your github settings page, select the `SSH and GPG keys` tab.

Press `New GPG Key`, then enter a name for the key and the outputs of the following command.

```
gpg --armor --export D2A162EAE1016F3G
```

## License

By contributing to Flink SQL Runner, you agree that your contributions will be licensed under the
[Apache 2.0 License](LICENSE).
