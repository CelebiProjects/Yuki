# Yuki package boundaries

Yuki uses four dependency layers:

```text
server -> services -> kernel -> utils
```

- `Yuki.server` contains HTTP and Celery adapters. Routes should translate
  requests and responses, then delegate work.
- `Yuki.services` contains application operations shared by the server, CLI,
  and workers, such as workflow lifecycle and impression transfer.
- `Yuki.kernel` contains job models, workflow implementations, execution
  state, runner support, and storage primitives.
- `Yuki.utils` contains small domain-independent helpers. Canonical utility
  modules must not import from `server`, `services`, or `kernel`.

The kernel is divided by responsibility:

```text
kernel/
  jobs/        job domain models
  workflows/   workflow model, Snakefile builder, and execution backends
  runners/     runner configuration, environment mapping, and SSH pooling
  execution/   status, leases, submissions, monitoring, and local execution
  storage/     staging, impressions, raw data, remote data, and file metadata
```

The former flat kernel modules have been removed. Code and `unittest.mock.patch`
targets must use the canonical package paths; keeping duplicate compatibility
files would make the physical layout misleading and allow the old architecture
to keep spreading.

Server-only REANA booking lives in `Yuki.server.services`; reusable lifecycle,
inventory, and result-transfer operations live in `Yuki.services`. Do not move domain
logic into `utils` merely to avoid choosing an owner.
