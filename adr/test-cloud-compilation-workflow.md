# Comment- and label-driven cloud-compilation sync workflow

## Summary
Adds a GitHub Actions workflow that lets contributors test a flink-sql-runner PR end-to-end against cloud-compilation. Contributors kick the flow off by commenting `/test-cloud-compilation`, which both syncs the PR to a cloud-compilation branch and tags the flink-sql-runner PR with the `cloud-compilation` label. Once the label is on a PR, every subsequent push re-syncs; when it isn't, the workflow posts a passing skip status so `cloud-compilation/build` can be required without blocking unrelated PRs.

## Key Decisions
- **Label-gated sync.** The `cloud-compilation` label decides whether `cloud-compilation/build` is required. With label → run the cross-repo sync; without label → post a passing `cloud-compilation/build` status so branch protection can require the check globally without blocking PRs that don't need it. Mirrors `skip-compilation-check` / `sync-compilation-pr` from `cloud-backend` but driven by a label rather than file paths.
- **Manual kickoff via comment.** `/test-cloud-compilation` triggers the sync **and** adds the label (idempotently). Most flink-sql-runner PRs don't touch cloud-compilation, so opt-in keeps noise and CI cost down.
- **Comment always re-syncs.** Even when the label is already present, re-commenting re-runs the sync. Concurrency group `sync-cc-<pr_number>` (cancel-in-progress=false) serializes overlapping runs so the comment-triggered sync and the `pull_request: labeled` event it produces don't race.
- **PAT for label add.** The label is added with `BOT_PAT` so the resulting `pull_request: labeled` event re-triggers downstream workflows; `GITHUB_TOKEN` would be silently ignored.
- **`GITHUB_TOKEN` for same-repo bot actions.** Reactions on the trigger comment, comment-back on the flink-sql-runner PR, and the collaborator permission check use `GITHUB_TOKEN` so they're attributed to `github-actions[bot]` rather than the PAT owner. Cross-repo ops on `DataSQRL/cloud-compilation` keep `BOT_PAT`.
- **Override `images.flinkSqlRunner`** in `integration-tests/config/application-override.yaml` to `ghcr.io/datasqrl/flink-sql-runner`, because PR images are published to GHCR while the default override points to a Docker Hub ECR pull-through cache that won't have `pr-N` tags.
- **GPG key id resolved dynamically.** Imports the private key in `secrets.GPG_PRIVATE_KEY`, then derives the signing key id from `gpg --list-secret-keys` and writes `default-key` + `pinentry-mode loopback` into `~/.gnupg/gpg.conf`. Avoids depending on a `GPG_KEY_ID` secret being kept in lockstep with the private key.
- **Auto-cleanup on PR close.** Revert `application.yaml` + `application-override.yaml` back to base, drop the tracking file, close the cloud-compilation PR if the branch matches base, otherwise leave a comment for manual review. Closed-without-merge just leaves a comment. Cleanup is gated on the cloud-compilation branch existing, so it's safe even if the label was removed before close.
- **Commenter authorization** is enforced via the collaborators-permission API (admin/write/maintain only) so external comments can't trigger paid CI.

## Notes
- Required secrets: `BOT_PAT`, `GPG_PRIVATE_KEY`. (`GPG_KEY_ID` no longer needed.)
- The `flink-sql-runner` label is applied to the cloud-compilation PR; create it in cloud-compilation if it doesn't already exist. Likewise the `cloud-compilation` label on the flink-sql-runner side.
- Mirrors patterns from `DataSQRL/cloud-backend/.github/workflows/build-and-test.yml` (`sync-compilation-pr`, `sync-compilation-revert`, `sync-compilation-comment-closed`).
