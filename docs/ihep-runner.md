# IHEP runner

The `ihep` backend stages workflows over SSH, then submits the executable
workflow wrapper to the IHEP batch system with `hep_sub`. Snakemake is never
started directly on `lxlogin.ihep.ac.cn`.

Register it through the normal runner API with these fields:

```text
runner=ihep
backend_type=ihep
ssh_host=lxlogin.ihep.ac.cn
ssh_user=<IHEP account>
ssh_key_path=<private key visible to Yuki>
remote_workdir=<shared filesystem path visible on login and worker nodes>
hep_group=<optional IHEP group>
hep_max_jobs=<maximum concurrent Snakemake step jobs; default 100>
hep_step_memory_mb=<fallback memory when a job has no valid limit; default 1024>
```

`remote_workdir` must be on storage shared by the login and batch worker nodes;
do not use login-node-local `/tmp`. The optional `hep_sub_path`, `hep_q_path`,
and `hep_rm_path` settings override the corresponding commands when the HepJob
tools are not in the non-interactive SSH `PATH`.

Install `snakemake-executor-plugin-cluster-generic` in the Snakemake environment
when using Snakemake 8.6 or newer. Older Snakemake releases with the legacy
`--cluster` option are also supported.

Yuki uses two levels of batch submission:

1. The workflow coordinator (`yuki_run.sh`) is submitted with `hep_sub`.
2. The coordinator asks Snakemake to submit every ready rule jobscript through
   `hep_sub`, up to `hep_max_jobs` at once. Yuki converts each Celebi
   `memory_limit` to the numeric Snakemake resource `mem_mb` in Python, then
   passes it to `hep_sub -m`; `hep_step_memory_mb` is only the
   fallback for a missing or invalid value. Submitted scripts plus stdout/stderr
   are kept under the workflow's `hep_jobs/` directory.

The coordinator and all child job ids are recorded. Status uses `hep_q -i`, and
cancellation removes both the coordinator and recorded children with `hep_rm`.
File staging, result collection, runner cache operations, and conda environment
discovery use the same SSH transport as an `ssh` runner.

## Installing the remote environment

IHEP AFS home access normally requires a Kerberos ticket and an AFS token.
Install the runner environment from an interactive login shell:

```bash
# On your local machine, upload the setup script to shared CEFS storage.
scp scripts/setup-ihep-runner.sh \
  mzhao@lxlogin.ihep.ac.cn:/cefs/higgs/zhaomr/setup-ihep-runner.sh

# Log in interactively, then run the script. It invokes kinit when needed,
# runs aklog -d, and installs the runtime on CEFS.
ssh mzhao@lxlogin.ihep.ac.cn
sh /cefs/higgs/zhaomr/setup-ihep-runner.sh \
  --root /cefs/higgs/zhaomr/yuki-runner
```

The script writes `runner-settings.txt` below the selected root. Use those
absolute paths when registering the runner. Its Conda wrapper sets
`register_envs: false`, so coordinator and rule jobs do not need an AFS token
to update `~/.conda/environments.txt`. The wrapper also redirects XDG and
Conda caches away from AFS home, so workflow execution stays entirely on CEFS.

Do not put the installation on `/workfs2/higgs/mzhao` unless its user quota is
large enough for Conda's package and file count.
