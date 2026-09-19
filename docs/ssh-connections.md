# SSH connection reuse

Workflow status checks, inventory, cache operations, and file transfers using
`_SshConnection` borrow persistent SSH/SFTP sessions. Sequential operations reuse
one connection. Concurrent operations can open up to four connections for each
host, port, user, and private-key identity in each Yuki process. If all four are
busy, callers wait up to 30 seconds for an available connection.

Each lease owns its SSH client and SFTP session exclusively. Keepalive messages
are sent after 30 seconds without traffic. Idle connections remain available
until process shutdown; disconnected transports and closed SFTP sessions are
replaced when next borrowed. Exceptions escaping an operation discard its
connection. Operations are not automatically replayed, because a remote command
may already have taken effect before the connection failed.

Changing the connection settings or replacing the configured key file selects a
new pool identity. Worker processes have independent pools; inherited SSH
transports are discarded after fork. Restart the server and workers to load this
change or close their existing pooled connections.

Detached job startup deliberately discards its connection after use: some SSH
servers leave the startup channel open even after reporting an exit status.
Runner settings connectivity/capability probes and environment discovery still
use their own short-lived connections.

Registration status endpoints read job state locally and refresh remote byte
progress in background threads. A request waits at most one second for progress;
if SSH is slow, it returns a cached snapshot or `progress: null`. Snapshots are
refreshed on demand after one second, with at most four concurrent reads and
128 cached entries per server process. Repeated polls share the pending read.
Completed and failed jobs bypass progress reads entirely. This keeps SSH delays
from exhausting the CLI's 10-second HTTP read timeout. Restart the Yuki server
after installing this change to load the updated endpoints.

Registration copies now run as detached Python processes on the runner. The
launcher creates a new session and redirects stdin to `/dev/null` and output to
`<remote_workdir>/register-progress/<job_id>.json.copy.log`. The copy no longer
depends on the SSH session or Celery worker remaining alive. Hashing still runs
through the existing SSH command; only the copy phase is detached.

The remote supervisor atomically records its PID and copying/done/failed state
in `<job_id>.json.copy` beside the log. A per-job file lock prevents concurrent
copies on redelivery, and terminal records prevent copying again after success
or failure. Retain these records while the registration exists. Celery performs
short checks, rescheduling itself after two seconds while pending (ten seconds
after connection errors); worker-loss
redelivery is enabled. Each check reads remote state and probes the process lock
before launching anything: active copies are only observed, completed copies
are reconciled, and missing or abandoned copies can be started. A failed probe
does not trigger a launch. Status/progress polling can also reconcile a remote
completion without a worker. Both the registration and impression states are
updated on completion. An SSH outage leaves the copy pending until connectivity
returns; it does not imply failure of the remote copy.

The remote copy samples its byte progress every second and replaces the progress
file atomically. Progress requests fetch process state and byte progress in one
SSH command. A terminal state discovered during that refresh is returned in the
same HTTP response when the refresh finishes within the one-second wait budget.
Slow runners still yield cached progress instead of blocking the request.

Deploy the updated server and restart Celery workers to enable detached copies
for subsequent launches. Already running synchronous copies are not converted.
Remote host shutdown or termination of the remote processes is outside the
worker/SSH-disconnect survival guarantee.

Rawdata file-status requests show live destination files and their current sizes
while copying. These may include incomplete files. Listings are fetched in a
separate bounded background cache, refreshed on demand after one second; HTTP
requests wait at most one second and return the previous snapshot if still busy.
Running and failed copies only use transient snapshots. Once the copy is
archived, completed listings (including empty ones) are saved atomically.
Older listings without a completed-registration marker are refreshed after
archiving, so a partial directory observed during copying cannot remain the
permanent file list. This also keeps the CLI `status` file-table request from
waiting on a slow SSH/SFTP directory scan.

When several registrations reference the same impression, status lookup selects
the most recently created job using its persisted `created_at_ns`, not the
lexicographic order of random UUIDs. This timestamp is retained across updates.
Legacy records fall back to file modification time, preserved as their creation
timestamp on the next write. A superseded copy may finish its own job record,
but cannot change the impression state owned by a newer registration.
