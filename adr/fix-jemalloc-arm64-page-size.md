# Ship a 4 KiB-page jemalloc for arm64

## Summary
The Flink base image installs Debian/Ubuntu's `libjemalloc2`, and the entrypoint preloads it. On arm64 that package is compiled with `--with-lg-page=16` (64 KiB pages) so a single binary also runs on 64K-page kernels. On 4 KiB-page kernels (Bottlerocket, AL2023 and Ubuntu on Graviton), every jemalloc slab and extent is rounded to 64 KiB, and dirty memory is purged in 64 KiB units. RocksDB-heavy TaskManagers pay for it in resident memory that Flink's memory model does not account for.

The image now builds jemalloc 5.3.0 with `--with-lg-page=12` into `/opt/jemalloc-4k`, and `entrypoint.sh` preloads it when `getconf PAGESIZE` is 4096. Any other page size keeps the distribution library.

## Evidence (prod metrics2, 2026-10-14/15)
- The steady metrics TaskManager (8000Mi, limit = request, 4 slots, ~290 RocksDB instances) ran for days on amd64 `m5ad` with no OOMKill.
- It moved to arm64 `r6gd` at 2026-10-14 20:06 and was OOMKilled 69 minutes later. After that, ARM TaskManagers lasted 69–279 minutes each. All kills were cgroup kills (exit 137); peak heap use stayed around 3 GiB of 4.6 GiB.
- `MALLOC_CONF=stats_print:true` reports `Page size: 65536` on arm64 and `Page size: 4096` on amd64, for the same image and jemalloc 5.3.0. The kernel page size is 4096 on both.
- A/B with the same job, image and config, measured from `/proc/1/smaps`: the malloc'd region was ~3.3 GiB on ARM against 2.5–2.8 GiB on x86 during its first 40 minutes, so ARM used roughly 500–800 MiB more. The overhead budget is 799 MiB. Thread stacks were identical (~380 MiB) despite the 2 MiB default stack size on aarch64.

## Key Decisions
- **Build from source, not runtime config.** `MALLOC_CONF` cannot change the page size, which is fixed at compile time (`LG_PAGE`).
- **Guard on the kernel page size.** A 4 KiB-page jemalloc refuses to run on a 64 KiB-page kernel, which some RHEL/Ubuntu arm64 kernels use. The entrypoint falls back to the stock library there, so customer dataplanes with 64K kernels keep working.
- **Use it on amd64 too.** x86 is already 4 KiB, so both architectures now run the same jemalloc build.
- **Pinned version and checksum.** `JEMALLOC_VERSION` and `JEMALLOC_SHA256` are build args, and the tarball is verified with `sha256sum -c`.

## Notes
- `DISABLE_JEMALLOC=true` (glibc malloc) still works and is a stopgap for images built before this change. glibc fragments badly under RocksDB, so pair it with `MALLOC_ARENA_MAX`.
- To verify inside a TaskManager: `tr '\0' '\n' </proc/1/environ | grep LD_PRELOAD` should show `/opt/jemalloc-4k/libjemalloc.so.2`.
