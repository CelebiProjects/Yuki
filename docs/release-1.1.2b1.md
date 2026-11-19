# Yuki 1.1.2b1

This beta combines [EOS usage and cleanup](eos-usage.md) with the native-runner
preview fix prepared as 1.1.1b1. The full changes are in the
[release note](../releaseNote/release_notes_v1.1.2b1).

Upgrade CelebiChrono and Yuki to the matching source version and reload the Yuki
HTTP service. The new `/eos-usage` and `/eos-cleanup` routes launch small REANA
jobs; the server itself needs no EOS mount. Configure a REANA runner with its URL,
token and `/eos/` cache root, and provide EOS/Kerberos access inside the jobs.
Existing reports without metadata fingerprints cannot be used for cleanup;
create a new usage scan. Installation never starts cleanup automatically.

`/remote-export` now serves native workflow stageout files, including repeated
outputs such as `stageout/2/plots/fit.png`, without copying them into Yuki Storage.
MIME types, HEAD and byte ranges are supported. Paths and symlinks escaping the
stageout directory are rejected. SSH and IHEP streaming remain supported.

The VS Code extension needs its separate Remote SSH preview-forwarding build
when used remotely. Updating Yuki does not install that plugin. This source
update does not itself publish a PyPI package or restart a deployed service.
