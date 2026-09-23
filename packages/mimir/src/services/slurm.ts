/** Slurm transport and state mapping for Mimir's durable remote jobs. */
import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import type { JobRecord, ServerRecord } from '../types.ts'

const execFileAsync = promisify(execFile)
const SSH_TOKEN = /^[a-zA-Z0-9._:-]+$/
const SSH_USER = /^[a-zA-Z0-9._-]+$/
const JOB_ID = /^job-[a-z0-9]+-\d+$/
const SLURM_ID = /^\d+$/
const REMOTE_TIMEOUT_MS = 20_000
const OUTPUT_LIMIT = 8192

export type Parameters = Record<string, unknown>

export class SlurmSubmissionError extends Error {
  constructor(message: string, readonly maybeSubmitted: boolean) { super(message) }
}

/** JSON representation independent of object key order. */
function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`
  if (value !== null && typeof value === 'object') {
    const object = value as Record<string, unknown>
    return `{${Object.keys(object).sort().map(key => `${JSON.stringify(key)}:${canonical(object[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

export function parseParameters(source: string | undefined): Parameters {
  if (source === undefined || source.trim() === '') return {}
  if (source.length > 32_768) throw new Error('parameters JSON must be at most 32768 characters')
  const parsed: unknown = JSON.parse(source)
  if (parsed === null || Array.isArray(parsed) || typeof parsed !== 'object') {
    throw new Error('parameters must be a JSON object')
  }
  return parsed as Parameters
}

export function configurationHash(script: string, parameters: Parameters): string {
  return createHash('sha256').update(canonical({ script, parameters })).digest('hex')
}

function checkTarget(server: ServerRecord, job: JobRecord): void {
  if (!SSH_TOKEN.test(server.host) || server.host.startsWith('-')
    || !SSH_USER.test(server.username) || server.username.startsWith('-')
    || !Number.isInteger(server.port) || server.port < 1 || server.port > 65535
    || !JOB_ID.test(job.id)) {
    throw new Error('invalid stored SSH target or job id')
  }
}

function sshArgs(server: ServerRecord, command: string): string[] {
  return [
    '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
    '-p', String(server.port), '--', `${server.username}@${server.host}`, command,
  ]
}

async function ssh(server: ServerRecord, command: string): Promise<string> {
  const { stdout } = await execFileAsync('ssh', sshArgs(server, command), {
    timeout: REMOTE_TIMEOUT_MS, maxBuffer: 1024 * 1024,
  })
  return stdout
}

function remotePath(job: JobRecord): string {
  if (!JOB_ID.test(job.id)) throw new Error('invalid job id')
  return `$HOME/.mimir/jobs/${job.id}`
}

/** Stage an immutable script, parameter file and manifest; then submit exactly once. */
export async function submitSlurm(server: ServerRecord, job: JobRecord, parameters: Parameters): Promise<string> {
  checkTarget(server, job)
  const localDir = await mkdtemp(join(tmpdir(), 'mimir-slurm-'))
  const remote = remotePath(job)
  const script = job.command.startsWith('#!') ? job.command : `#!/bin/bash\n${job.command}\n`
  const manifest = {
    mimirJobId: job.id,
    experimentId: job.experimentId ?? null,
    configHash: job.configHash,
    scriptSha256: createHash('sha256').update(script).digest('hex'),
    parameters,
    createdAt: job.createdAt,
  }
  try {
    await Promise.all([
      writeFile(join(localDir, 'job.sh'), script, { mode: 0o600 }),
      writeFile(join(localDir, 'parameters.json'), `${canonical(parameters)}\n`, { mode: 0o600 }),
      writeFile(join(localDir, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 }),
    ])
    await ssh(server, `mkdir -p "${remote}"`)
    await execFileAsync('scp', [
      '-q', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5', '-P', String(server.port),
      join(localDir, 'job.sh'), join(localDir, 'parameters.json'), join(localDir, 'manifest.json'),
      `${server.username}@${server.host.includes(':') ? `[${server.host}]` : server.host}:.mimir/jobs/${job.id}/`,
    ], { timeout: REMOTE_TIMEOUT_MS, maxBuffer: 1024 * 1024 })
    // The batch script runs in its own directory, so parameters.json is available
    // as a relative file. Slurm owns the process after this acknowledgment.
    try {
      const reply = await ssh(server,
        `sbatch --parsable --chdir="${remote}" --output="${remote}/stdout.log" --error="${remote}/stderr.log" "${remote}/job.sh"`)
      const id = reply.trim().split(';')[0] ?? ''
      if (!SLURM_ID.test(id)) throw new Error(`unrecognized sbatch reply: ${reply.trim().slice(0, 200)}`)
      return id
    } catch (error) {
      const code = (error as { code?: unknown }).code
      // A definite remote sbatch rejection is safe to report as failure.
      // Transport loss or an unparseable reply could hide an accepted job.
      throw new SlurmSubmissionError(error instanceof Error ? error.message : String(error), typeof code !== 'number' || code === 255)
    }
  } finally {
    await rm(localDir, { recursive: true, force: true })
  }
}

export type SlurmProbe =
  | { status: 'queued' | 'running'; note?: string }
  | { status: 'succeeded' | 'failed' | 'cancelled'; exitCode: number | null }

function mapState(raw: string, exitCode: number | null): SlurmProbe | null {
  const state = raw.trim().split(/[ +]/)[0]?.toUpperCase()
  if (state === 'PENDING' || state === 'REQUEUED' || state === 'REQUEUE_FED') return { status: 'queued' }
  if (state === 'RUNNING' || state === 'CONFIGURING' || state === 'COMPLETING'
    || state === 'SUSPENDED' || state === 'RESIZING') return { status: 'running' }
  if (state === 'COMPLETED') return { status: exitCode === null || exitCode === 0 ? 'succeeded' : 'failed', exitCode }
  if (state === 'CANCELLED' || state === 'PREEMPTED') return { status: 'cancelled', exitCode }
  if (state === 'FAILED' || state === 'TIMEOUT' || state === 'NODE_FAIL'
    || state === 'OUT_OF_MEMORY' || state === 'BOOT_FAIL' || state === 'DEADLINE'
    || state === 'REVOKED') return { status: 'failed', exitCode }
  return null
}

/** Query the live queue first, accounting second. Missing data remains active. */
export async function probeSlurm(server: ServerRecord, job: JobRecord): Promise<SlurmProbe> {
  checkTarget(server, job)
  const id = job.slurmJobId
  if (id === undefined || !SLURM_ID.test(id)) throw new Error('missing Slurm job id')
  const queued = (await ssh(server, `squeue -h -j ${id} -o '%T'`)).trim().split('\n')[0] ?? ''
  if (queued !== '') {
    const state = mapState(queued, null)
    if (state !== null) return state
    return { status: job.status === 'running' ? 'running' : 'queued', note: `unrecognized squeue state: ${queued}` }
  }
  const accounting = await ssh(server, `sacct -n -P -j ${id} --format=JobIDRaw,State,ExitCode`)
  const row = accounting.split('\n').map(line => line.trim().split('|')).find(parts => parts[0] === id)
  if (row === undefined) return { status: job.status === 'running' ? 'running' : 'queued', note: 'job absent from squeue and sacct; status pending confirmation' }
  const exitText = row[2]?.split(':')[0] ?? ''
  const exitCode = /^\d+$/.test(exitText) ? Number(exitText) : null
  const state = mapState(row[1] ?? '', exitCode)
  return state ?? { status: job.status === 'running' ? 'running' : 'queued', note: `unrecognized sacct state: ${row[1] ?? ''}` }
}

export async function slurmLogs(server: ServerRecord, job: JobRecord): Promise<{ stdout: string; stderr: string }> {
  checkTarget(server, job)
  const remote = remotePath(job)
  const [stdout, stderr] = await Promise.all([
    ssh(server, `test -f "${remote}/stdout.log" && tail -c ${OUTPUT_LIMIT} "${remote}/stdout.log" || true`),
    ssh(server, `test -f "${remote}/stderr.log" && tail -c ${OUTPUT_LIMIT} "${remote}/stderr.log" || true`),
  ])
  return { stdout, stderr }
}

export async function cancelSlurm(server: ServerRecord, job: JobRecord): Promise<void> {
  checkTarget(server, job)
  if (job.slurmJobId === undefined || !SLURM_ID.test(job.slurmJobId)) throw new Error('missing Slurm job id')
  await ssh(server, `scancel ${job.slurmJobId}`)
}
