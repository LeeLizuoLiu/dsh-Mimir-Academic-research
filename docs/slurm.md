# Slurm jobs in Mimir

Register an SSH server in **Servers**, then select **Slurm** in the jobs form.
Paste a batch script (including any `#SBATCH` directives) and, optionally, a
JSON object of parameters. Link an experiment when you want its status and
last-job summary updated after Slurm confirms completion.

Mimir creates `~/.mimir/jobs/<mimir-job-id>/` on that server and stages:

- `job.sh`: the submitted script;
- `parameters.json`: the canonical JSON object, available relative to the
  script's working directory;
- `manifest.json`: the Mimir job id, experiment id, script hash, parameter
  values, and configuration hash.

The configuration hash is SHA-256 of the script and canonical parameter JSON.
It identifies the submitted configuration; Mimir still creates a new Slurm
job for every submission. It does not hash external input datasets or source
trees. Include dataset hashes and code revisions in the parameter JSON when
they are needed for your provenance policy.

After `sbatch --parsable` acknowledges a submission, Mimir stores the Slurm
job id. The jobs view queries `squeue`, then `sacct` when the job leaves the
queue, and shows the tail of `stdout.log` and `stderr.log`. Active Slurm jobs
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
