# Ship a 4 KiB-page jemalloc on every architecture

## Summary
The Flink base image installs Debian/Ubuntu's `libjemalloc2`, and the entrypoint preloads it from `/usr/lib/<arch>-linux-gnu/libjemalloc.so`. On arm64 that package is compiled with `--with-lg-page=16` (64 KiB pages) so a single binary also runs on 64K-page kernels. On 4 KiB-page kernels (Bottlerocket, AL2023 and Ubuntu on Graviton), every jemalloc slab and extent is rounded to 64 KiB, and dirty memory is purged in 64 KiB units. RocksDB-heavy TaskManagers pay for it in resident memory that Flink's memory model does not account for.

The image now builds jemalloc 5.3.0 with `--with-lg-page=12` and installs it over the distribution's `libjemalloc.so.2` on both amd64 and arm64.

## Evidence (prod metrics2, 2026-10-14/15)
- The steady metrics TaskManager (8000Mi, limit = request, 4 slots, ~290 RocksDB instances) ran for days on amd64 `m5ad` with no OOMKill.
- It moved to arm64 `r6gd` at 2026-10-14 20:06 and was OOMKilled 69 minutes later. After that, ARM TaskManagers lasted 69–279 minutes each. All kills were cgroup kills (exit 137); peak heap use stayed around 3 GiB of 4.6 GiB.
- `MALLOC_CONF=stats_print:true` reports `Page size: 65536` on arm64 and `Page size: 4096` on amd64, for the same image and jemalloc 5.3.0. The kernel page size is 4096 on both.
- A/B with the same job, image and config, measured from `/proc/1/smaps`: the malloc'd region was ~3.3 GiB on ARM against 2.5–2.8 GiB on x86 during its first 40 minutes, so ARM used roughly 500–800 MiB more. The overhead budget is 799 MiB. Thread stacks were identical (~380 MiB) despite the 2 MiB default stack size on aarch64.

## Key Decisions
- **Build from source, not runtime config.** `MALLOC_CONF` cannot change the page size, which is fixed at compile time (`LG_PAGE`).
- **Replace the library in place, not in an entrypoint.** The Flink Kubernetes operator starts pods with the upstream `/docker-entrypoint.sh`, not the image's `/entrypoint.sh`. Overwriting the file that every entrypoint preloads makes the choice independent of which script starts the JVM.
- **Same library on every architecture.** amd64 gets the custom build too, so both architectures run the same allocator and there are no per-arch surprises.
- **The build fails if the wrong library is installed.** The final stage preloads the installed library and requires `Version: "5.3.0-` and `Page size: 4096` in its stats.
- **Built once, in a separate base image.** `Dockerfile.base` holds jemalloc, the JDK and async-profiler, and is published as `ghcr.io/datasqrl/flink-sql-runner-base:<sha1 of the filtered Dockerfile.base>`. CI builds it only when that tag is missing. `Dockerfile` starts `FROM` it through the `BASE_TAG` build arg, which defaults to `latest`, and `latest` moves only on pushes to main.
- **Pinned version and checksum.** `JEMALLOC_VERSION` and `JEMALLOC_SHA256` are build args, and the tarball is verified with `sha256sum -c`.

## Notes
- **64K-page kernels are not supported** by this image's jemalloc, because a 4 KiB-page build refuses to run there. Some RHEL/Ubuntu arm64 kernels use 64K pages. Set `DISABLE_JEMALLOC=true` on such clusters to fall back to glibc malloc (pair it with `MALLOC_ARENA_MAX`).
- To verify a running TaskManager:
  - `grep -m1 jemalloc /proc/1/maps` shows the mapped library, which must be `/usr/lib/<arch>-linux-gnu/libjemalloc.so.2`.
  - `MALLOC_CONF=stats_print:true LD_PRELOAD=/usr/lib/$(uname -m)-linux-gnu/libjemalloc.so /bin/true 2>&1 | grep 'Page size'` must print `Page size: 4096`.
