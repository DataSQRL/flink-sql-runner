# Comment-triggered cloud-compilation sync workflow

## Summary
Adds a GitHub Actions workflow that lets contributors test a flink-sql-runner PR end-to-end against cloud-compilation by commenting `/test-cloud-compilation`. Once a PR has been kicked off, subsequent pushes to the same PR auto-resync, and PR close cleans up the cloud-compilation side.

## Key Decisions
- Manual kickoff via PR comment (not automatic on PR open) — most flink-sql-runner PRs do not need cloud-compilation integration runs, so opt-in keeps noise and CI cost down. Mirrors the spirit of cloud-backend's `sync-compilation-pr` flow but inverts the default.
- Auto-sync after first kickoff (on `pull_request: synchronize`/`reopened`) — gated on the cloud-compilation branch already existing, so it only ever fires for PRs the user previously opted in.
- Override `images.flinkSqlRunner` in `integration-tests/config/application-override.yaml` to `ghcr.io/datasqrl/flink-sql-runner` because PR images are published to GHCR, while the default override points to a Docker Hub ECR pull-through cache that won't have `pr-N` tags.
- Auto-cleanup on PR close — revert the `application.yaml` and `application-override.yaml` files to base, drop the tracking file, close the cloud-compilation PR if the branch matches base, otherwise leave a comment for manual review. Closed-without-merge just leaves a comment.
- Commenter authorization is enforced via the GitHub permissions API (admin/write/maintain only) so external comments cannot trigger paid CI.

## Notes
- Requires repo/org secrets `BOT_PAT`, `GPG_PRIVATE_KEY`, `GPG_KEY_ID` (already used by the cloud-backend equivalent).
- The `flink-sql-runner` label is applied to the cloud-compilation PR; create it in cloud-compilation if it doesn't already exist.
- Mirrors patterns from `DataSQRL/cloud-backend/.github/workflows/build-and-test.yml` (`sync-compilation-pr`, `sync-compilation-revert`, `sync-compilation-comment-closed`).
