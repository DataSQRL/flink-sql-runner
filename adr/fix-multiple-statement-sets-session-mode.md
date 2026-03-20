# Fix: Multiple EXECUTE STATEMENT SET blocks in session mode

## Summary

When running SQL scripts with multiple `EXECUTE STATEMENT SET` blocks via FlinkSessionJob (session mode), only the first block executes. The fix replaces `tableResult.await()` with a fallback to job status polling when `await()` is not supported.

## Key Decisions

**Why `await()` fails in session mode**: FlinkSessionJob submits jobs via REST API, which returns a `WebSubmissionJobClient`. This client does not support `getJobExecutionResult()`, which `await()` internally calls. The first statement set runs fine, but the runner throws when trying to wait for it before submitting the second.

**Fix approach — try/catch with polling fallback**: `awaitCompletion()` first tries `await()` (works in application mode). If that throws, it falls back to polling `jobClient.getJobStatus()` until a terminal state (FINISHED, FAILED, CANCELED). This preserves the serialization between statement sets that Ferenc identified as essential, while working in both application and session modes.

**Why not remove `await()` entirely**: The await/polling is necessary to serialize multiple statement sets. Without it, the second set would submit before the first completes, which breaks the intended execution order.

## Related

- Issue: https://github.com/DataSQRL/flink-sql-runner/issues/308
- Test manifest: https://github.com/DataSQRL/cloud-compilation/blob/add-dedup-test/test-case.yaml
