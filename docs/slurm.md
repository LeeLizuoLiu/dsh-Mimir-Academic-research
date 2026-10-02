# Slurm jobs in Mimir

Register the SSH entry host in **Servers** with role **Login node**, then select
**Slurm** in the jobs form. The manual login-node check opens an authenticated SSH connection and checks whether `sbatch` and `squeue` are available over batch-mode SSH. It does not
run `nvidia-smi`: GPUs belong to allocated compute nodes, and login nodes may
have no GPU tools installed. Use role **Compute node** only for a host where a
direct `nvidia-smi` readout is meaningful. Existing saved servers default to
Compute node until their role is edited.

Paste a batch script (including any `#SBATCH` directives) and, optionally, a
JSON object of parameters. You can also enter an absolute local source
directory. Mimir hashes and rsyncs its contents before submission into this
  job's unique `source/` directory. The script runs with that snapshot as its
  working directory. The canonical parameters file remains in the job root
  and its path is provided in `$MIMIR_PARAMETERS_FILE`. `.git`,
`node_modules`, `.venv`, `__pycache__`, and `.DS_Store` are excluded. Link an
experiment when you want its status and last-job summary updated after Slurm
confirms completion.

Mimir creates `~/scratch/.mimir/jobs/<mimir-job-id>/` on that server by default
and stages:

- `job.sh`: the submitted script;
- `parameters.json`: the canonical JSON object, available relative to the
  script's working directory;
- `manifest.json`: the Mimir job id, experiment id, script hash, parameter
  values, and configuration hash.
- `source/` (optional): the per-job source snapshot, including its own
  source files when a local source directory was supplied.

The **Remote jobs directory** in the login-node server settings controls
this location. Enter a path relative to the remote Home directory, such as
`scratch/.mimir/jobs`, or an absolute path on a storage area allocated to
you. Use a directory with enough quota for the source snapshots and logs;
the default Burgundy scratch quota may be too small for large or repeated
jobs. Editing this setting affects only new submissions. Existing job records
retain their original remote paths, including jobs created under the older
`~/.mimir/jobs/` location. Mimir does not move or delete old remote files.

The configuration hash is SHA-256 of the script, canonical parameter JSON,
and (when supplied) the filtered source-tree digest. It identifies the
submitted configuration; Mimir still creates a new Slurm job for every
submission. It does not hash external input datasets. Include dataset hashes
in the parameter JSON when they are needed for your provenance policy.

After `sbatch --parsable` acknowledges a submission, Mimir stores the Slurm
job id. Opening the jobs view reads saved records only. Click **Refresh status and logs**
to query `squeue`, then `sacct` when the job leaves the queue, and read the tails
of `stdout.log` and `stderr.log`. There is no automatic Slurm polling. Active Slurm jobs
survive a Mimir restart. A temporary query failure leaves the job active with
a diagnostic. **Cancel job** sends `scancel` and waits for scheduler
confirmation; it does not delete the job record.

If the SSH connection drops during `sbatch`, Mimir reports `unknown` because
the scheduler may have accepted the job without returning its id. Inspect
the remote directory and scheduler before retrying. Output files other than
the two logs remain on the remote host; this integration does not yet pull
them into local Mimir artifacts.

The agent's `server_submit_job` tool accepts `mode: "slurm"` and
`parameters_json`, and `server_list_jobs` reads the same persistent records.

## SSH connection reuse and manual refresh

**Connect / check**, submission, transfers, cancellation and explicit status refreshes
reuse one OpenSSH ControlMaster connection per user, host and port in each Mimir
service. Commands are serialized per target, including scp and rsync, so concurrent
requests do not each establish a fresh connection. Control sockets live in a private
0700 directory. Existing SSH configuration and host-key checking remain in force.

Between actions no remote commands run. SSH sends a keepalive every 60 seconds;
after two unanswered keepalives it closes. A healthy connection is retained for up
to 30 idle minutes; disabling/unloading the plugin closes its owned connections.
Closing the web page stops its activity but does not itself unload the host plugin.
A broken connection does not start a reconnect loop. A transport failure suppresses
further attempts to that target for five minutes, then a new explicit action can retry.

The listJobs API and server_list_jobs tool default to cached records. Pass
`refresh: true` only for an explicit scheduler query. Local UI data-change events
and updates after submission/cancellation do not query Slurm. Status and output
remain last known until a manual refresh succeeds. The initial server check also
requires an explicit click; opening the page does not probe the cluster.

Servers with a username use the authenticated SSH check directly, with no separate
connect-and-disconnect TCP probe. An SSH failure is displayed as offline rather than
Online. Servers without a username retain an explicit TCP-only probe.
