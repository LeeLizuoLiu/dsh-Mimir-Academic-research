/** Slurm transport and state mapping for Mimir's durable remote jobs. */
import { createHash } from 'node:crypto'
import { cp, lstat, mkdtemp, readdir, readFile, readlink, realpath, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, relative, sep } from 'node:path'
import type { JobRecord, ServerRecord } from '../types.ts'

import { sharedSsh, type SharedSshTransport } from './ssh-transport.ts'
const SSH_TOKEN = /^[a-zA-Z0-9._:-]+$/
const SSH_USER = /^[a-zA-Z0-9._-]+$/
const JOB_ID = /^job-[a-z0-9]+-\d+$/
const SLURM_ID = /^\d+$/
const REMOTE_TIMEOUT_MS = 20_000
const OUTPUT_LIMIT = 8192
const SOURCE_EXCLUDES = new Set(['.git', 'node_modules', '.venv', '__pycache__', '.DS_Store'])

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

export function configurationHash(script: string, parameters: Parameters, sourceSha256?: string): string {
  return createHash('sha256').update(canonical({ script, parameters, ...(sourceSha256 === undefined ? {} : { sourceSha256 }) })).digest('hex')
}

export interface SourceTreeDigest { readonly path: string; readonly sha256: string; readonly fileCount: number; readonly cleanup: () => Promise<void> }

/** Hash the exact regular files, symlinks, and relative paths that rsync includes. */
export async function digestSourceTree(sourceDirectory: string): Promise<SourceTreeDigest> {
  if (sourceDirectory.trim() === '' || sourceDirectory.length > 4096) throw new Error('source directory must be a non-empty local path')
  const root = await realpath(sourceDirectory)
  if (!(await lstat(root)).isDirectory()) throw new Error('source directory must point to a directory')
  const hash = createHash('sha256')
  let fileCount = 0
  const visit = async (relative: string): Promise<void> => {
    const absolute = relative === '' ? root : join(root, relative)
    const entries = await readdir(absolute, { withFileTypes: true })
    entries.sort((a, b) => a.name.localeCompare(b.name))
    for (const entry of entries) {
      if (SOURCE_EXCLUDES.has(entry.name)) continue
      const childRelative = relative === '' ? entry.name : `${relative}/${entry.name}`
      const childPath = join(root, childRelative)
      const stat = await lstat(childPath)
      const name = Buffer.from(childRelative)
      hash.update(`${stat.isDirectory() ? 'D' : stat.isSymbolicLink() ? 'L' : stat.isFile() ? 'F' : 'O'}:${name.length}:`)
      hash.update(name)
      hash.update(`:${stat.mode & 0o777}:`)
      if (stat.isDirectory()) await visit(childRelative)
      else if (stat.isSymbolicLink()) hash.update(await readlink(childPath))
      else if (stat.isFile()) {
        hash.update(await readFile(childPath))
        fileCount += 1
      } else throw new Error(`unsupported source entry: ${childRelative}`)
    }
  }
  await visit('')
  if (fileCount === 0) throw new Error('source directory has no files after excluding .git, node_modules, .venv, __pycache__, and .DS_Store')
  return { path: root, sha256: hash.digest('hex'), fileCount, cleanup: async () => {} }
}

/** Copy local inputs to a private staging tree first, then hash that exact tree. */
export async function stageSourceTree(sourceDirectory: string): Promise<SourceTreeDigest> {
  const stagingDirectory = await mkdtemp(join(tmpdir(), 'mimir-source-'))
  try {
    const input = await realpath(sourceDirectory)
    if (!(await lstat(input)).isDirectory()) throw new Error('source directory must point to a directory')
    const staged = join(stagingDirectory, 'source')
    await cp(input, staged, {
      recursive: true,
      dereference: false,
      verbatimSymlinks: true,
      filter: (entryPath) => {
        const rel = relative(input, entryPath)
        return rel === '' || !rel.split(sep).some(part => SOURCE_EXCLUDES.has(part))
      },
    })
    const digest = await digestSourceTree(staged)
    return { ...digest, cleanup: async () => { await rm(stagingDirectory, { recursive: true, force: true }) } }
  } catch (error) {
    await rm(stagingDirectory, { recursive: true, force: true })
    throw error
  }
}

function checkTarget(server: ServerRecord, job: JobRecord): void {
  if (!SSH_TOKEN.test(server.host) || server.host.startsWith('-')
    || !SSH_USER.test(server.username) || server.username.startsWith('-')
    || !Number.isInteger(server.port) || server.port < 1 || server.port > 65535
    || !JOB_ID.test(job.id)) {
    throw new Error('invalid stored SSH target or job id')
  }
}

async function ssh(server: ServerRecord, command: string, transport: SharedSshTransport): Promise<string> {
  return (await transport.ssh(server, command, REMOTE_TIMEOUT_MS)).stdout
}

function remotePath(job: JobRecord): string {
  if (!JOB_ID.test(job.id)) throw new Error('invalid job id')
  // Old records used ~/..., while new records retain the configured directory
  // so later server edits never move a running or completed job.
  const path = (job.remoteDir ?? `~/.mimir/jobs/${job.id}`).replace(/^~\//, '$HOME/')
  if (!/^(?:\$HOME\/|\/)[a-zA-Z0-9._/-]+$/.test(path)
    || path.split('/').some(part => part === '.' || part === '..')
    || !path.endsWith(`/${job.id}`)) {
    throw new Error('invalid stored remote job directory')
  }
  return path
}

/** Transfer tools interpret a home-relative path after `user@host:`. */
function transferPath(remote: string): string {
  return remote.startsWith('$HOME/') ? remote.slice('$HOME/'.length) : remote
}

/** Stage an immutable script, parameter file and manifest; then submit exactly once. */
export async function submitSlurm(server: ServerRecord, job: JobRecord, parameters: Parameters, source?: SourceTreeDigest, transport = sharedSsh): Promise<string> {
  checkTarget(server, job)
  const localDir = await mkdtemp(join(tmpdir(), 'mimir-slurm-'))
  const remote = remotePath(job)
  const script = job.command.startsWith('#!') ? job.command : `#!/bin/bash\n${job.command}\n`
  const manifest = {
    mimirJobId: job.id,
    experimentId: job.experimentId ?? null,
    configHash: job.configHash,
    scriptSha256: createHash('sha256').update(script).digest('hex'),
    sourceSha256: source?.sha256 ?? null,
    sourceFileCount: source?.fileCount ?? null,
    parameters,
    createdAt: job.createdAt,
  }
  try {
    await Promise.all([
      writeFile(join(localDir, 'job.sh'), script, { mode: 0o600 }),
      writeFile(join(localDir, 'parameters.json'), `${canonical(parameters)}\n`, { mode: 0o600 }),
      writeFile(join(localDir, 'manifest.json'), `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 }),
    ])
    await ssh(server, `mkdir -p "${remote}"`, transport)
    if (source !== undefined) {
      const sourceRemote = `${transferPath(remote)}/source/`
      await ssh(server, `mkdir -p "${remote}/source"`, transport)
      await transport.run(server, 'rsync', options => [
        '--archive', '--checksum', '--exclude=.git/', '--exclude=node_modules/', '--exclude=.venv/',
        '--exclude=__pycache__/', '--exclude=.DS_Store', '-e', ['ssh', ...options, '-p', String(server.port)].map(arg => `'${arg.replaceAll("'", "'\"'\"'")}'`).join(' '),
        `${source.path}/`, `${server.username}@${server.host.includes(':') ? `[${server.host}]` : server.host}:${sourceRemote}`,
      ], 10 * 60_000)
    }
    await transport.run(server, 'scp', options => [
      '-q', ...options, '-P', String(server.port),
      join(localDir, 'job.sh'), join(localDir, 'parameters.json'), join(localDir, 'manifest.json'),
      `${server.username}@${server.host.includes(':') ? `[${server.host}]` : server.host}:${transferPath(remote)}/`,
    ], REMOTE_TIMEOUT_MS)
    // Each job runs from its own immutable source snapshot when provided.
    try {
      const reply = await ssh(server,
        `sbatch --parsable --export=ALL,MIMIR_PARAMETERS_FILE="${remote}/parameters.json" --chdir="${source === undefined ? remote : `${remote}/source`}" --output="${remote}/stdout.log" --error="${remote}/stderr.log" "${remote}/job.sh"`, transport)
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
export async function probeSlurm(server: ServerRecord, job: JobRecord, transport = sharedSsh): Promise<SlurmProbe> {
  checkTarget(server, job)
  const id = job.slurmJobId
  if (id === undefined || !SLURM_ID.test(id)) throw new Error('missing Slurm job id')
  const queueOutput = (await ssh(server, `squeue -h -j ${id} -o '%T'`, transport)).trim()
  // Some sites print a cache-age notice on stdout before the requested state.
  // Read all lines and consult accounting when no line is a Slurm state.
  for (const line of queueOutput.split(/\r?\n/)) {
    const state = mapState(line, null)
    if (state !== null) return state
  }
  const accounting = await ssh(server, `sacct -n -P -j ${id} --format=JobIDRaw,State,ExitCode`, transport)
  const row = accounting.split('\n').map(line => line.trim().split('|')).find(parts => parts[0] === id)
  if (row === undefined) return {
    status: job.status === 'running' ? 'running' : 'queued',
    note: queueOutput === ''
      ? 'job absent from squeue and sacct; status pending confirmation'
      : `no Slurm state in squeue output: ${queueOutput.slice(0, 200)}`,
  }
  const exitText = row[2]?.split(':')[0] ?? ''
  const exitCode = /^\d+$/.test(exitText) ? Number(exitText) : null
  const state = mapState(row[1] ?? '', exitCode)
  return state ?? { status: job.status === 'running' ? 'running' : 'queued', note: `unrecognized sacct state: ${row[1] ?? ''}` }
}

export async function slurmLogs(server: ServerRecord, job: JobRecord, transport = sharedSsh): Promise<{ stdout: string; stderr: string }> {
  checkTarget(server, job)
  const remote = remotePath(job)
  const [stdout, stderr] = await Promise.all([
    ssh(server, `test -f "${remote}/stdout.log" && tail -c ${OUTPUT_LIMIT} "${remote}/stdout.log" || true`, transport),
    ssh(server, `test -f "${remote}/stderr.log" && tail -c ${OUTPUT_LIMIT} "${remote}/stderr.log" || true`, transport),
  ])
  return { stdout, stderr }
}

export async function cancelSlurm(server: ServerRecord, job: JobRecord, transport = sharedSsh): Promise<void> {
  checkTarget(server, job)
  if (job.slurmJobId === undefined || !SLURM_ID.test(job.slurmJobId)) throw new Error('missing Slurm job id')
  await ssh(server, `scancel ${job.slurmJobId}`, transport)
}
