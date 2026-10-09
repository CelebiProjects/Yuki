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
  execution/   leases, submissions, and progress snapshots
  storage/     liveness and output-file classification
```

Legacy flat modules such as `Yuki.kernel.vworkflow` remain compatibility
aliases for one deprecation cycle. They resolve to the same module objects as
their canonical paths, so existing imports and `unittest.mock.patch` targets
continue to affect the implementation. The legacy alias modules are the sole
exception to the dependency direction above. New code must use canonical paths.

Server-only REANA booking lives in `Yuki.server.services`; reusable lifecycle,
inventory, and transfer operations live in `Yuki.services`. Do not move domain
logic into `utils` merely to avoid choosing an owner.
