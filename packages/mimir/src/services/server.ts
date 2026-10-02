/**
 * Server domain module: remembered compute servers CRUD, the two-stage
 * checkServer probe (TCP then best-effort ssh GPU readout), and the remote
 * job lifecycle (submitJob queues and drives a job over batch-mode ssh in
 * the background; the panel polls listJobs for the flips). The job counter
 * lives on an explicit `ServiceState`. Thin forwarding of the `server.*`
 * Remote namespace lives in `service.ts`.
 * @module dsh-mimir/src/services/server
 */

import { execFile } from 'node:child_process'
import { appendFile } from 'node:fs/promises'
import { connect } from 'node:net'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { emitEvent, PANEL_ACTOR, SERVICE_ACTOR } from '../ledger.ts'
import { isActiveJobStatus } from '../job-lifecycle.ts'
import type { ResearchWikiDomain } from '../store.ts'
import type {
  ExperimentJobOutcome,
  ExperimentStatus,
  JobRecord,
  ResearchCheckServerResult,
  ResearchDeleteJobResult,
  ResearchDeleteServerResult,
  ResearchListJobsResult,
  ResearchListServersResult,
  ResearchSaveServerResult,
  ResearchSubmitJobResult,
  ServerGpuView,
  ServerInput,
  ServerRecord,
  ServerStatusView,
} from '../types.ts'
import { DEFAULT_REMOTE_JOB_ROOT } from '../types.ts'
import { sharedSsh, type SharedSshTransport } from './ssh-transport.ts'
import { nextRecordId, rejected, success } from './common.ts'
import type { ServiceState } from './common.ts'
import { cancelSlurm, configurationHash, parseParameters, probeSlurm, slurmLogs, SlurmSubmissionError, stageSourceTree, submitSlurm } from './slurm.ts'
import type { Parameters, SourceTreeDigest } from './slurm.ts'

/** Everything the Server domain functions need from the service scope. */
export interface ServerDeps {
  readonly sshTransport?: SharedSshTransport
  /** Absolute research workspace root (for the settled-job experiment log append). */
  readonly workspaceDir: string
  readonly domain: ResearchWikiDomain
  /**
   * Job-limit overrides (tests); absent, {@link SSH_JOB_TIMEOUT_MS} and
   * {@link SSH_JOB_MAX_BUFFER_BYTES} apply — see job-lifecycle.ts for how
   * exceeding either settles the record.
   */
  readonly jobs?: {
    readonly timeoutMs?: number | undefined
    readonly maxBufferBytes?: number | undefined
  } | undefined
}

/** TCP reachability probe timeout; part of the checkServer probe contract. */
const TCP_PROBE_TIMEOUT_MS = 4000
/** The remote command whose CSV output feeds the GPU table. */
const NVIDIA_SMI_QUERY = 'nvidia-smi --query-gpu=name,utilization.gpu,memory.used,memory.total --format=csv,noheader,nounits'
/** Check the tools Mimir actually needs to submit and observe Slurm jobs. */
const SLURM_QUERY = 'command -v sbatch >/dev/null && command -v squeue >/dev/null'

/** Hard cap of one submitted command line (the durable record stores it verbatim). */
const JOB_COMMAND_MAX_CHARS = 4000
const SLURM_SCRIPT_MAX_CHARS = 32_768
/**
 * Kill timeout of one remote job's ssh session. Hitting it settles the job
 * `unknown` (#225): the session is dropped, so the remote outcome — including
 * whether the process still runs — was never observed.
 */
const SSH_JOB_TIMEOUT_MS = 30 * 60_000
/**
 * execFile buffer cap of one job's combined stdout/stderr. Hitting it kills
 * the session and settles the job `failed` with an explicit note (#225) —
 * output capture never silently truncates.
 */
const SSH_JOB_MAX_BUFFER_BYTES = 4 * 1024 * 1024
/** Characters kept of one settled job's stdout/stderr tails. */
const JOB_OUTPUT_TAIL_CHARS = 8192

const execFileAsync = promisify(execFile)

/** Keep the trailing window of one job output stream. */
function tailOf(text: string): string {
  return text.length <= JOB_OUTPUT_TAIL_CHARS ? text : text.slice(text.length - JOB_OUTPUT_TAIL_CHARS)
}

/** Outcome of one TCP reachability probe. */
type TcpProbeOutcome =
  | { readonly ok: true; readonly latencyMs: number }
  | { readonly ok: false; readonly message: string }

/**
 * Connect to `host:port` once, measuring the handshake latency. The probe
 * never throws: every failure mode (refused, unreachable, timed out) settles
 * as the `ok: false` branch carrying the socket's own message.
 * @param host - server host name or address.
 * @param port - server TCP port.
 * @returns the connected latency, or the failure message.
 */
function probeTcp(host: string, port: number): Promise<TcpProbeOutcome> {
  return new Promise<TcpProbeOutcome>((settle) => {
    const startedAt = Date.now()
    let done = false
    const socket = connect({ host, port })
    const finish = (outcome: TcpProbeOutcome): void => {
      if (done) return
      done = true
      socket.destroy()
      settle(outcome)
    }
    socket.once('connect', () => { finish({ ok: true, latencyMs: Date.now() - startedAt }) })
    socket.once('error', (error) => { finish({ ok: false, message: error.message }) })
    socket.setTimeout(TCP_PROBE_TIMEOUT_MS, () => {
      finish({ ok: false, message: `tcp connect timed out after ${String(TCP_PROBE_TIMEOUT_MS)}ms` })
    })
  })
}

/** Outcome of the best-effort GPU readout over ssh. */
type GpuProbeOutcome =
  | { readonly ok: true; readonly gpus: readonly ServerGpuView[] }
  | { readonly ok: false; readonly stage: 'ssh' | 'gpu'; readonly message: string }

type SlurmProbeOutcome =
  | { readonly ok: true }
  | { readonly ok: false; readonly stage: 'ssh' | 'slurm'; readonly message: string }

/** A login node is checked for scheduler tools, never for a local GPU. */
async function probeSlurmCommands(record: ServerRecord, transport: SharedSshTransport): Promise<SlurmProbeOutcome> {
  try {
    await transport.ssh(record, SLURM_QUERY)
    return { ok: true }
  } catch (error) {
    const code = (error as { code?: unknown }).code
    const stderr = typeof error === 'object' && error !== null && 'stderr' in error
      ? String((error as { stderr: unknown }).stderr).trim()
      : ''
    return {
      ok: false,
      stage: code === 255 ? 'ssh' : 'slurm',
      message: stderr || (code === 255
        ? 'SSH connection or authentication failed'
        : 'sbatch or squeue is unavailable in the remote shell'),
    }
  }
}

/**
 * Read one server's GPU table over a batch-mode ssh call. Best-effort: an ssh
 * or `nvidia-smi` failure is the `ok: false` branch, never a rejection, so the
 * caller can still report the server itself as reachable.
 * @param record - the server to probe (host, port, and login user).
 * @returns the parsed GPU rows, or the failure stage (`ssh` session vs `gpu`
 * readout) plus the failure message.
 */
async function probeGpus(record: ServerRecord, transport: SharedSshTransport): Promise<GpuProbeOutcome> {
  try {
    const { stdout } = await transport.ssh(record, NVIDIA_SMI_QUERY)
    const gpus: ServerGpuView[] = []
    for (const line of stdout.split('\n')) {
      const trimmed = line.trim()
      if (trimmed === '') continue
      const [name, utilizationPct, memoryUsedMb, memoryTotalMb] = trimmed.split(',').map(field => field.trim())
      gpus.push({
        name: name ?? '',
        utilizationPct: Number(utilizationPct),
        memoryUsedMb: Number(memoryUsedMb),
        memoryTotalMb: Number(memoryTotalMb),
      })
    }
    return { ok: true, gpus: Object.freeze(gpus) }
  } catch (error) {
    // execFile failures carry the child's stderr; prefer it over the generic
    // "Command failed" wrapper so the panel shows the ssh client's own words.
    const stderr = typeof error === 'object' && error !== null && 'stderr' in error
      ? String((error as { stderr: unknown }).stderr).trim()
      : ''
    const message = stderr !== '' ? stderr : error instanceof Error ? error.message : 'ssh probe failed'
    // Stage classification: the ssh client exits 255 when the SESSION itself
    // failed (connect, auth, or the connect timeout); any other exit code is
    // the remote command's own status, and a timeout kill / spawn error only
    // surfaces after the 8s budget — past the 5s connect window — so both
    // land on the `gpu` stage.
    const code = (error as { code?: unknown }).code
    return { ok: false, stage: code === 255 ? 'ssh' : 'gpu', message }
  }
}

/**
 * Character whitelist for the ssh login user / host: anything else
 * (whitespace, shell punctuation) is rejected, and a leading dash would make
 * the `user@host` argument look like an ssh option even under execFile.
 */
const SSH_TOKEN_RE = /^[a-zA-Z0-9._:-]+$/
const SSH_USERNAME_RE = /^[a-zA-Z0-9._-]+$/
const REMOTE_ROOT_RE = /^[a-zA-Z0-9._/-]+$/

/** Accept a home-relative or absolute path without shell syntax or traversal. */
function validRemoteJobRoot(root: string): boolean {
  if (root === '' || root.length > 512 || root === '/' || !REMOTE_ROOT_RE.test(root)) return false
  const parts = root.split('/')
  return parts.every((part, index) => (part !== '' || (index === 0 && root.startsWith('/')))
    && part !== '.' && part !== '..')
}

/** First invalid-input message for one server upsert payload, or null when valid. */
function validateServerInput(input: ServerInput): string | null {
  if (input.name.trim().length === 0) return 'name must be non-empty'
  if (input.host.trim().length === 0) return 'host must be non-empty'
  if (input.host.startsWith('-') || !SSH_TOKEN_RE.test(input.host)) {
    return 'host must be a plain hostname or IP (letters, digits, dots, dashes, colons)'
  }
  if (input.username !== undefined && input.username.trim() !== ''
    && (input.username.startsWith('-') || !SSH_USERNAME_RE.test(input.username))) {
    return 'username must be a plain ssh login user (letters, digits, dots, dashes, underscores)'
  }
  if (input.role !== undefined && input.role !== 'login' && input.role !== 'compute') {
    return 'role must be login or compute'
  }
  if (input.remoteJobRoot !== undefined && !validRemoteJobRoot(input.remoteJobRoot)) {
    return 'remote job directory must be a home-relative or absolute path without spaces, shell syntax, or .. segments'
  }
  if (!Number.isInteger(input.port) || input.port < 1 || input.port > 65535) {
    return 'port must be an integer between 1 and 65535'
  }
  return null
}

/**
 * List every remembered compute server, most recently updated first.
 * @param deps - open wiki domain.
 * @returns the server cards for the panel's servers view.
 */
export function listServers(deps: ServerDeps): Promise<ResearchListServersResult> {
  const servers = [...deps.domain.table('servers').entries()]
    .map(([, record]) => record)
    .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
  return Promise.resolve(success({ servers: Object.freeze(servers) }))
}

/**
 * Upsert one compute server. An `id` in the payload selects the update form
 * (the existing record's `createdAt` survives; `updatedAt` refreshes); its
 * absence creates a record with a generated id. Name and host must be
 * non-empty and the port a valid TCP port — violations are `invalid-input`,
 * an unknown update id is `server-not-found`. A present `tags` list is
 * trimmed, emptied out, and deduped before it replaces the record's tags;
 * an omitted list keeps them.
 * @param deps - open wiki domain.
 * @param state - per-instance counters backing the collision-safe id.
 * @param request - the server fields, with `id` marking the update form.
 * @returns the stored record.
 */
export async function saveServer(
  deps: ServerDeps,
  state: ServiceState,
  request: { server: ServerInput },
): Promise<ResearchSaveServerResult> {
  const input = request.server
  const invalid = validateServerInput(input)
  if (invalid !== null) return rejected({ code: 'invalid-input', message: invalid })
  const table = deps.domain.table('servers')
  const now = new Date().toISOString()
  // Tags are trimmed, emptied out, and deduped (the updatePaper cleaning).
  const tags = input.tags === undefined
    ? undefined
    : [...new Set(input.tags.map(tag => tag.trim()).filter(tag => tag !== ''))]
  if (input.id !== undefined) {
    const existing = table.get(input.id)
    if (existing === undefined) return rejected({ code: 'server-not-found', id: input.id })
    const next: ServerRecord = {
      ...existing,
      name: input.name,
      host: input.host,
      port: input.port,
      username: input.username,
      role: input.role ?? existing.role,
      remoteJobRoot: input.remoteJobRoot ?? existing.remoteJobRoot ?? DEFAULT_REMOTE_JOB_ROOT,
      note: input.note,
      tags: tags ?? existing.tags,
      updatedAt: now,
    }
    await table.put(input.id, next)
    await emitEvent(deps.domain, {
      actor: PANEL_ACTOR,
      action: 'compute.server.saved',
      refs: { serverId: next.id },
      payload: { name: next.name, host: next.host, created: false },
    })
    return success({ server: next })
  }
  const created: ServerRecord = {
    id: nextRecordId(state, 'srv'),
    name: input.name,
    host: input.host,
    port: input.port,
    username: input.username,
    role: input.role ?? 'compute',
    remoteJobRoot: input.remoteJobRoot ?? DEFAULT_REMOTE_JOB_ROOT,
    note: input.note,
    tags: tags ?? [],
    createdAt: now,
    updatedAt: now,
  }
  await table.put(created.id, created)
  await emitEvent(deps.domain, {
    actor: PANEL_ACTOR,
    action: 'compute.server.saved',
    refs: { serverId: created.id },
    payload: { name: created.name, host: created.host, created: true },
  })
  return success({ server: created })
}

/**
 * Delete one remembered server; an unknown id is `server-not-found`.
 * @param deps - open wiki domain.
 * @param request - the record id.
 * @returns the deleted id.
 */
export async function deleteServer(
  deps: ServerDeps,
  request: { id: string },
): Promise<ResearchDeleteServerResult> {
  const table = deps.domain.table('servers')
  const removed = table.get(request.id)
  if (removed === undefined) {
    return rejected({ code: 'server-not-found', id: request.id })
  }
  await table.delete(request.id)
  await emitEvent(deps.domain, {
    actor: PANEL_ACTOR,
    action: 'compute.server.deleted',
    refs: { serverId: removed.id },
    payload: { name: removed.name, destructive: true },
  })
  return success({ id: request.id })
}

/**
 * Check the capability appropriate to the
 * server role over authenticated SSH: Slurm commands on a login node or nvidia-smi on a compute
 * node. Only servers without a username use TCP-only reachability.
 * @param deps - open wiki domain.
 * @param request - the record id; an unknown id is `server-not-found`.
 * @returns the settled probe view.
 */
export async function checkServer(
  deps: ServerDeps,
  request: { id: string },
): Promise<ResearchCheckServerResult> {
  const record = deps.domain.table('servers').get(request.id)
  if (record === undefined) {
    return rejected({ code: 'server-not-found', id: request.id })
  }
  // Never open an unauthenticated TCP probe for an SSH-capable server.
  if (record.username === '') {
    const tcp = await probeTcp(record.host, record.port)
    return success<ServerStatusView>({
      state: tcp.ok ? 'online' : 'offline',
      latencyMs: tcp.ok ? tcp.latencyMs : null,
      gpus: Object.freeze([]), checkedAt: new Date().toISOString(),
      message: tcp.ok ? null : tcp.message, stage: 'tcp',
      ...(tcp.ok ? { tcpLatencyMs: tcp.latencyMs } : {}),
    })
  }
  const startedAt = Date.now()
  const transport = deps.sshTransport ?? sharedSsh
  if (record.role === 'login') {
    const slurm = await probeSlurmCommands(record, transport)
    return success<ServerStatusView>({
      state: !slurm.ok && slurm.stage === 'ssh' ? 'offline' : 'online',
      latencyMs: slurm.ok ? Date.now() - startedAt : null,
      gpus: Object.freeze([]), checkedAt: new Date().toISOString(),
      message: slurm.ok ? null : slurm.message,
      stage: slurm.ok ? 'slurm' : slurm.stage,
      slurmLatencyMs: Date.now() - startedAt,
    })
  }
  const gpu = await probeGpus(record, transport)
  return success<ServerStatusView>({
    state: !gpu.ok && gpu.stage === 'ssh' ? 'offline' : 'online',
    latencyMs: gpu.ok ? Date.now() - startedAt : null,
    gpus: gpu.ok ? gpu.gpus : Object.freeze([]), checkedAt: new Date().toISOString(),
    message: gpu.ok ? null : `gpu probe failed: ${gpu.message}`,
    stage: gpu.ok ? 'gpu' : gpu.stage,
    gpuLatencyMs: Date.now() - startedAt,
  })
}

/**
 * Settle every job left active by a host restart. The SSH child belongs to the
 * old process, so its remote outcome is unknowable and must never be guessed.
 * @param deps - open wiki domain plus workspace root.
 */
export async function recoverInterruptedJobs(deps: ServerDeps): Promise<void> {
  const jobs = deps.domain.table('jobs')
  const experiments = deps.domain.table('experiments')
  const now = new Date().toISOString()
  for (const [, job] of jobs.entries()) {
    if (!isActiveJobStatus(job.status)) continue
    if (job.mode === 'slurm' && job.slurmJobId !== undefined) continue
    const recovered: JobRecord = {
      ...job,
      status: job.mode === 'slurm' ? 'unknown' : 'interrupted',
      stderrTail: job.mode === 'slurm'
        ? 'host restarted during Slurm submission; inspect the remote directory before retrying'
        : 'host restarted before the job outcome was known',
      finishedAt: now,
    }
    await jobs.put(job.id, recovered)
    if (job.experimentId === undefined || hasNewerLinkedJob(deps, job)) continue
    const experiment = experiments.get(job.experimentId)
    if (experiment?.status === 'running') {
      await experiments.put(experiment.id, { ...experiment, status: 'failed', updatedAt: now })
    }
  }
}

/**
 * Submit one remote command to a remembered server. The record lands
 * `queued` and the run starts in the background: this call returns once
 * the record is durable, and the panel polls `listJobs` for the status
 * flips (`running`, then `succeeded`/`failed`). The command must be
 * non-empty and at most {@link JOB_COMMAND_MAX_CHARS} characters
 * (`invalid-input`), and the server must name an ssh login user (a
 * TCP-only record cannot run jobs). A given `experimentId` must name an
 * experiment record (`experiment-not-found`): a linked experiment flips
 * to `running` with the server link on submit, then to
 * `success`/`failed` when the job settles — unless a newer job linked to
 * the same experiment supersedes the settle.
 * @param deps - open wiki domain.
 * @param state - the service's mutable job counter (incremented here).
 * @param request - the target server, the command line, and the optional
 * experiment link.
 * @returns the queued record.
 */
export async function submitJob(
  deps: ServerDeps,
  state: ServiceState,
  request: {
    serverId: string
    command: string
    experimentId?: string | undefined
    mode?: 'ssh' | 'slurm' | undefined
    parametersJson?: string | undefined
    sourceDirectory?: string | undefined
  },
): Promise<ResearchSubmitJobResult> {
  const server = deps.domain.table('servers').get(request.serverId)
  if (server === undefined) {
    return rejected({ code: 'server-not-found', id: request.serverId })
  }
  const command = request.command.trim()
  if (command === '') return rejected({ code: 'invalid-input', message: 'command must be non-empty' })
  const commandLimit = request.mode === 'slurm' ? SLURM_SCRIPT_MAX_CHARS : JOB_COMMAND_MAX_CHARS
  if (command.length > commandLimit) {
    return rejected({ code: 'invalid-input', message: `command must be at most ${String(commandLimit)} characters` })
  }
  if (request.mode !== undefined && request.mode !== 'ssh' && request.mode !== 'slurm') {
    return rejected({ code: 'invalid-input', message: 'mode must be ssh or slurm' })
  }
  let parameters: Parameters = {}
  let source: SourceTreeDigest | undefined
  if (request.mode === 'slurm') {
    try { parameters = parseParameters(request.parametersJson) }
    catch (error) {
      return rejected({ code: 'invalid-input', message: error instanceof Error ? error.message : 'invalid parameters JSON' })
    }
  } else if ((request.parametersJson !== undefined && request.parametersJson.trim() !== '') || request.sourceDirectory !== undefined) {
    return rejected({ code: 'invalid-input', message: 'parameters JSON and source directory require Slurm mode' })
  }
  if (server.username === '') {
    return rejected({ code: 'invalid-input', message: `server ${server.name} has no ssh login user` })
  }
  if (request.experimentId !== undefined
    && deps.domain.table('experiments').get(request.experimentId) === undefined) {
    return rejected({ code: 'experiment-not-found', id: request.experimentId })
  }
  if (request.mode === 'slurm' && request.sourceDirectory !== undefined && request.sourceDirectory.trim() !== '') {
    try { source = await stageSourceTree(request.sourceDirectory) }
    catch (error) { return rejected({ code: 'invalid-input', message: error instanceof Error ? error.message : 'could not read source directory' }) }
  }
  state.jobSeq += 1
  const jobId = `job-${Date.now().toString(36)}-${String(state.jobSeq)}`
  const remoteRoot = server.remoteJobRoot ?? DEFAULT_REMOTE_JOB_ROOT
  const remoteBase = remoteRoot.startsWith('/') ? remoteRoot : `$HOME/${remoteRoot}`
  const remoteDir = `${remoteBase}/${jobId}`
  const job: JobRecord = {
    id: jobId,
    ...(request.mode === 'slurm' ? { mode: 'slurm' as const } : {}),
    serverId: server.id,
    command,
    status: 'queued',
    // Absent, never `undefined`: an explicit undefined key would pollute the
    // stored record and trip the gateway's JSON boundary validation.
    ...(request.experimentId === undefined ? {} : { experimentId: request.experimentId }),
    exitCode: null,
    stdoutTail: '',
    stderrTail: '',
    createdAt: new Date().toISOString(),
    ...(request.mode === 'slurm' ? {
      remoteDir,
      ...(source === undefined ? {} : {
        remoteSourceDir: `${remoteDir}/source`,
        sourceSha256: source.sha256,
        sourceFileCount: source.fileCount,
      }),
      configHash: configurationHash(command, parameters, source?.sha256),
    } : {}),
  }
  try { await deps.domain.table('jobs').put(job.id, job) }
  catch (error) { await source?.cleanup(); throw error }
  if (request.experimentId !== undefined) {
    await markExperiment(deps, request.experimentId, 'running', server.id)
  }
  await emitEvent(deps.domain, {
    actor: PANEL_ACTOR,
    action: 'compute.job.submitted',
    refs: {
      jobId: job.id,
      serverId: server.id,
      ...(request.experimentId === undefined ? {} : { experimentId: request.experimentId }),
    },
    payload: { command: command.slice(0, 200) },
  })
  // Register ownership before yielding so deletion can cancel the queued race.
  if (job.mode === 'slurm') {
    void runSlurmSubmission(deps, state, job.id, parameters, source)
  } else {
    const abort = new AbortController()
    state.jobAborts.set(job.id, abort)
    void runJob(deps, state, job.id, abort.signal)
  }
  return success({ job })
}

/**
 * List submitted remote jobs, most recently submitted first, optionally
 * filtered to one server (an unknown id is `server-not-found`). This is
 * the panel's polling read.
 * @param deps - open wiki domain.
 * @param request - the optional server filter.
 * @returns the job rows.
 */
export async function listJobs(
  deps: ServerDeps,
  state: ServiceState,
  request: { serverId?: string; refresh?: boolean },
): Promise<ResearchListJobsResult> {
  if (request.serverId !== undefined
    && deps.domain.table('servers').get(request.serverId) === undefined) {
    return rejected({ code: 'server-not-found', id: request.serverId })
  }
  await Promise.all([...deps.domain.table('jobs').entries()]
    .map(([, job]) => job)
    .filter(job => request.refresh === true && job.mode === 'slurm' && job.slurmJobId !== undefined && isActiveJobStatus(job.status)
      && (request.serverId === undefined || job.serverId === request.serverId))
    .map(job => refreshSlurmJob(deps, state, job)))
  const jobs = [...deps.domain.table('jobs').entries()]
    .map(([, record]) => record)
    .filter(record => request.serverId === undefined || record.serverId === request.serverId)
    .sort((left, right) => right.createdAt.localeCompare(left.createdAt))
  return success({ jobs: Object.freeze(jobs) })
}

/**
 * Delete one terminal job record. Deleting an active job instead aborts the
 * locally-owned SSH client and retains a `cancelled` record because the remote
 * process outcome cannot be proven from a disconnected session.
 * @param deps - open wiki domain.
 * @param state - owning service state and SSH abort handles.
 * @param request - the record id.
 * @returns the affected id.
 */
export async function deleteJob(
  deps: ServerDeps,
  state: ServiceState,
  request: { id: string },
): Promise<ResearchDeleteJobResult> {
  const table = deps.domain.table('jobs')
  const existing = table.get(request.id)
  if (existing === undefined) {
    return rejected({ code: 'job-not-found', id: request.id })
  }
  if (existing.mode === 'slurm' && isActiveJobStatus(existing.status)) {
    if (existing.slurmJobId === undefined) {
      return rejected({ code: 'invalid-input', message: 'Slurm submission is still in progress; retry cancellation once its job id appears' })
    }
    const server = deps.domain.table('servers').get(existing.serverId)
    if (server === undefined) return rejected({ code: 'server-not-found', id: existing.serverId })
    try {
      await cancelSlurm(server, existing, deps.sshTransport)
      await table.put(existing.id, { ...existing, statusNote: 'scancel requested; awaiting scheduler confirmation' })
      return success({ id: existing.id })
    } catch (error) {
      return rejected({ code: 'invalid-input', message: `scancel failed: ${error instanceof Error ? error.message : String(error)}` })
    }
  }
  const abort = state.jobAborts.get(request.id)
  if (abort !== undefined && isActiveJobStatus(existing.status)) {
    // Deleting an active row is a cancel request: the local SSH session is
    // aborted and the record settles `cancelled` (the remote process outcome
    // is unknowable once the session drops). The row stays visible until that
    // terminal flip, so the user can see what was cancelled.
    state.jobStopStatus.set(request.id, 'cancelled')
    abort.abort()
    return success({ id: request.id })
  }
  await table.delete(request.id)
  return success({ id: request.id })
}

/** Background stage and sbatch handshake; the durable record carries errors. */
async function runSlurmSubmission(deps: ServerDeps, state: ServiceState, id: string, parameters: Parameters, source?: SourceTreeDigest): Promise<void> {
  const table = deps.domain.table('jobs')
  const job = table.get(id)
  if (job === undefined) { await source?.cleanup(); return }
  const server = deps.domain.table('servers').get(job.serverId)
  if (server === undefined) {
    await finishJob(deps, state, { ...job, status: 'failed', stderrTail: 'server record deleted before Slurm submission', finishedAt: new Date().toISOString() })
    await source?.cleanup()
    return
  }
  try {
    const slurmJobId = await submitSlurm(server, job, parameters, source, deps.sshTransport)
    const current = table.get(id)
    if (current !== undefined && isActiveJobStatus(current.status)) {
      await table.put(id, { ...current, slurmJobId, statusNote: `Slurm job ${slurmJobId} accepted` })
    }
  } catch (error) {
    const current = table.get(id)
    if (current === undefined || !isActiveJobStatus(current.status)) return
    const uncertain = error instanceof SlurmSubmissionError && error.maybeSubmitted
    await finishJob(deps, state, {
      ...current,
      status: uncertain ? 'unknown' : 'failed',
      stderrTail: `${error instanceof Error ? error.message : String(error)}${uncertain ? '\nsbatch may have accepted the job; inspect the remote directory before retrying' : ''}`,
      finishedAt: new Date().toISOString(),
    })
  } finally {
    await source?.cleanup()
  }
}

const slurmRefreshes = new WeakMap<ResearchWikiDomain, Map<string, Promise<void>>>()

/** Refresh one scheduler-owned job once even if panel and agent poll together. */
function refreshSlurmJob(deps: ServerDeps, state: ServiceState, job: JobRecord): Promise<void> {
  let pending = slurmRefreshes.get(deps.domain)
  if (pending === undefined) {
    pending = new Map()
    slurmRefreshes.set(deps.domain, pending)
  }
  const existing = pending.get(job.id)
  if (existing !== undefined) return existing
  const inFlight = syncSlurmJob(deps, state, job).finally(() => { pending?.delete(job.id) })
  pending.set(job.id, inFlight)
  return inFlight
}

async function syncSlurmJob(deps: ServerDeps, state: ServiceState, job: JobRecord): Promise<void> {
  const table = deps.domain.table('jobs')
  const current = table.get(job.id)
  if (current === undefined || !isActiveJobStatus(current.status)) return
  const server = deps.domain.table('servers').get(job.serverId)
  if (server === undefined) {
    await table.put(job.id, { ...current, statusNote: 'server record missing; Slurm status cannot be checked' })
    return
  }
  try {
    const probe = await probeSlurm(server, current, deps.sshTransport)
    const latest = table.get(job.id)
    if (latest === undefined || !isActiveJobStatus(latest.status)) return
    let logs: { stdout: string; stderr: string } | undefined
    try { logs = await slurmLogs(server, latest, deps.sshTransport) }
    catch { /* Scheduler state remains usable when log files are unavailable. */ }
    const next: JobRecord = {
      ...latest,
      status: probe.status,
      ...(probe.status === 'running' && latest.startedAt === undefined ? { startedAt: new Date().toISOString() } : {}),
      ...(logs === undefined ? {} : { stdoutTail: tailOf(logs.stdout), stderrTail: tailOf(logs.stderr) }),
      ...('note' in probe && probe.note !== undefined ? { statusNote: probe.note } : { statusNote: '' }),
      ...('exitCode' in probe ? { exitCode: probe.exitCode, finishedAt: new Date().toISOString() } : {}),
    }
    if (isActiveJobStatus(next.status)) await table.put(job.id, next)
    else await finishJob(deps, state, next)
  } catch (error) {
    const latest = table.get(job.id)
    if (latest !== undefined && isActiveJobStatus(latest.status)) {
      await table.put(job.id, { ...latest, statusNote: `Slurm status unavailable: ${error instanceof Error ? error.message : String(error)}` })
    }
  }
}

/**
 * Drive one queued job to its terminal state over a batch-mode ssh call:
 * flip the record `running`, wait on the remote command, then settle per the
 * lifecycle contract (#225, see job-lifecycle.ts): `succeeded` (exit 0),
 * `failed` (remote non-zero exit, a pre-command session failure, or the
 * capture cap hit — with an explicit note), `unknown` (the duration-cap
 * kill: observation lost, remote possibly alive), or `cancelled` /
 * `interrupted` when the local session was aborted. Never rejects — the
 * record is the panel's only channel.
 * @param deps - open wiki domain.
 * @param state - owning service state and SSH abort handles.
 * @param id - the job record id.
 * @param signal - abort handle whose abort cancels the SSH child.
 */
async function runJob(
  deps: ServerDeps,
  state: ServiceState,
  id: string,
  signal: AbortSignal,
): Promise<void> {
  const table = deps.domain.table('jobs')
  const queued = table.get(id)
  if (queued === undefined) {
    state.jobAborts.delete(id)
    state.jobStopStatus.delete(id)
    return
  }
  if (signal.aborted) {
    await settleStoppedJob(deps, state, queued, state.jobStopStatus.get(id) ?? 'cancelled')
    return
  }
  const server = deps.domain.table('servers').get(queued.serverId)
  if (server === undefined) {
    await table.put(id, {
      ...queued,
      status: 'failed',
      stderrTail: 'server record deleted before the job started',
      finishedAt: new Date().toISOString(),
    })
    state.jobAborts.delete(id)
    state.jobStopStatus.delete(id)
    return
  }
  const running: JobRecord = { ...queued, status: 'running', startedAt: new Date().toISOString() }
  await table.put(id, running)
  if (signal.aborted) {
    await settleStoppedJob(deps, state, running, state.jobStopStatus.get(id) ?? 'cancelled')
    return
  }
  let settled: JobRecord
  try {
    const { stdout, stderr } = await execFileAsync('ssh', [
      '-o', 'BatchMode=yes',
      '-o', `ConnectTimeout=${String(15)}`,
      '-p', String(server.port),
      '--',
      `${server.username}@${server.host}`,
      running.command,
    ], {
      timeout: deps.jobs?.timeoutMs ?? SSH_JOB_TIMEOUT_MS,
      maxBuffer: deps.jobs?.maxBufferBytes ?? SSH_JOB_MAX_BUFFER_BYTES,
      signal,
    })
    settled = {
      ...running, status: 'succeeded', exitCode: 0,
      stdoutTail: tailOf(stdout), stderrTail: tailOf(stderr),
      finishedAt: new Date().toISOString(),
    }
  } catch (error) {
    if (signal.aborted) {
      await settleStoppedJob(deps, state, running, state.jobStopStatus.get(id) ?? 'cancelled', error)
      return
    }
    // execFile failures carry the child's exit code and captured output;
    // the ssh client propagates the remote command's exit code as its
    // own, so a numeric code IS the remote exit code. A non-numeric code
    // means the session itself failed (connect refused, spawn error, or
    // the timeout kill) — then the message stands in for stderr.
    const carrier = error as { code?: unknown; killed?: unknown; stdout?: unknown; stderr?: unknown }
    const stdout = typeof carrier.stdout === 'string' ? carrier.stdout : ''
    const stderrText = typeof carrier.stderr === 'string' ? carrier.stderr : ''
    // The lifecycle contract (#225) splits unobserved sessions by cause.
    if (carrier.code === 'ERR_CHILD_PROCESS_STDIO_MAXBUFFER') {
      // Output past the capture cap: the session is killed and the record
      // fails LOUDLY — capture never silently truncates.
      const capBytes = deps.jobs?.maxBufferBytes ?? SSH_JOB_MAX_BUFFER_BYTES
      settled = {
        ...running, status: 'failed', exitCode: null,
        stdoutTail: tailOf(stdout),
        stderrTail: tailOf(`${stderrText}\noutput exceeded the ${String(Math.round(capBytes / 1024 / 1024))} MiB capture limit; the session was killed`.trim()),
        finishedAt: new Date().toISOString(),
      }
    } else if (carrier.killed === true && typeof carrier.code !== 'number') {
      // The duration-cap kill: observation was lost without user intent, so
      // the job settles `unknown` — the remote process may still be running.
      const capMs = deps.jobs?.timeoutMs ?? SSH_JOB_TIMEOUT_MS
      settled = {
        ...running, status: 'unknown', exitCode: null,
        stdoutTail: tailOf(stdout),
        stderrTail: tailOf(`${stderrText}\nsession exceeded the ${String(Math.round(capMs / 60_000))}-minute limit and was dropped; the remote process may still be running (outcome unobserved)`.trim()),
        finishedAt: new Date().toISOString(),
      }
    } else {
      const exitCode = typeof carrier.code === 'number' ? carrier.code : null
      const stderr = stderrText.trim() !== ''
        ? stderrText
        : error instanceof Error ? error.message : 'ssh job failed'
      settled = {
        ...running, status: 'failed', exitCode,
        stdoutTail: tailOf(stdout), stderrTail: tailOf(stderr),
        finishedAt: new Date().toISOString(),
      }
    }
  }
  await finishJob(deps, state, settled)
}

/** Abort every active SSH session this service owns (host dispose path). */
export function stopOwnedJobs(state: ServiceState): void {
  for (const [id, controller] of state.jobAborts) {
    // Set the intended terminal status before aborting so the settle races
    // can distinguish host stop (`interrupted`) from a user cancel.
    state.jobStopStatus.set(id, 'interrupted')
    controller.abort()
  }
}

/**
 * Settle an aborted SSH session without claiming a remote process outcome:
 * `cancelled` for a user/delete cancel, `interrupted` for host dispose.
 * The preserved output tails ride any stderr the child captured before the
 * abort; a fabricated success/failure is never written for a session whose
 * remote outcome is unknowable.
 */
async function settleStoppedJob(
  deps: ServerDeps,
  state: ServiceState,
  job: JobRecord,
  status: 'cancelled' | 'interrupted',
  error?: unknown,
): Promise<void> {
  const current = deps.domain.table('jobs').get(job.id)
  if (current === undefined) {
    state.jobAborts.delete(job.id)
    state.jobStopStatus.delete(job.id)
    return
  }
  const carrier = error as { stdout?: unknown; stderr?: unknown } | undefined
  const stdout = typeof carrier?.stdout === 'string' ? carrier.stdout : ''
  const note = status === 'cancelled'
    ? 'cancelled locally; remote process outcome is unknown'
    : 'host stopped before the job outcome was known'
  const stderr = typeof carrier?.stderr === 'string' && carrier.stderr.trim() !== ''
    ? `${carrier.stderr}\n${note}`
    : note
  await finishJob(deps, state, {
    ...current,
    status,
    exitCode: null,
    stdoutTail: tailOf(stdout),
    stderrTail: tailOf(stderr),
    finishedAt: new Date().toISOString(),
  })
}

/**
 * Persist one terminal job, emit its settle event, write back the linked
 * experiment, and release the local SSH ownership for the job id.
 */
async function finishJob(deps: ServerDeps, state: ServiceState, settled: JobRecord): Promise<void> {
  state.jobAborts.delete(settled.id)
  state.jobStopStatus.delete(settled.id)
  const table = deps.domain.table('jobs')
  // A delete during the run already dropped the record: the remote command
  // still finished, but nothing is written back.
  if (table.get(settled.id) === undefined) return
  await table.put(settled.id, settled)
  const startedMs = settled.startedAt === undefined ? null : Date.parse(settled.startedAt)
  const finishedMs = settled.finishedAt === undefined ? null : Date.parse(settled.finishedAt)
  await emitEvent(deps.domain, {
    actor: SERVICE_ACTOR,
    action: 'compute.job.settled',
    refs: {
      jobId: settled.id,
      serverId: settled.serverId,
      ...(settled.experimentId === undefined ? {} : { experimentId: settled.experimentId }),
    },
    payload: {
      status: settled.status,
      exitCode: settled.exitCode,
      durationMs: startedMs !== null && finishedMs !== null
        && Number.isFinite(startedMs) && Number.isFinite(finishedMs)
        ? Math.max(0, finishedMs - startedMs)
        : null,
      summary: jobSummaryOf(settled),
    },
  })
  await writeBackExperiment(deps, settled)
}

/**
 * Write one settled job's outcome back to its linked experiment: flip the
 * lifecycle status (`succeeded` → `success`, otherwise `failed`), record
 * the outcome (exit code, wall-clock duration, finished timestamp,
 * trailing log excerpt) as the record's `lastJob`, and append one line to
 * the workspace's `EXPERIMENT_LOG.md`. An unlinked or deleted experiment
 * is skipped; the log append is best-effort and never fails the settle.
 *
 * The experiment lifecycle is deliberately coarser than the job's (#225):
 * `cancelled`/`interrupted`/`unknown` all land as `failed` there, because
 * the run produced no usable result — the job record keeps the precise
 * cause.
 *
 * Stale-settle guard: when a NEWER job is linked to the same experiment
 * (submitted after this one, in any status), that job owns the
 * experiment's state, so this late settle leaves the record untouched and
 * only lands in the log — otherwise an old job finishing last would flip
 * the status back over the newer job's write-back.
 * @param deps - open wiki domain plus workspace root.
 * @param job - the settled job record.
 */
async function writeBackExperiment(deps: ServerDeps, job: JobRecord): Promise<void> {
  if (job.experimentId === undefined) return
  const table = deps.domain.table('experiments')
  const existing = table.get(job.experimentId)
  if (existing === undefined) return
  const finishedAt = job.finishedAt ?? new Date().toISOString()
  const startedMs = job.startedAt !== undefined ? Date.parse(job.startedAt) : Number.NaN
  const finishedMs = Date.parse(finishedAt)
  const outcome: ExperimentJobOutcome = {
    jobId: job.id,
    status: job.status === 'succeeded' ? 'succeeded' : job.status === 'cancelled' ? 'cancelled' : 'failed',
    exitCode: job.exitCode,
    durationMs: Number.isFinite(startedMs) && Number.isFinite(finishedMs)
      ? Math.max(0, finishedMs - startedMs)
      : null,
    finishedAt,
    summary: jobSummaryOf(job),
  }
  if (hasNewerLinkedJob(deps, job)) {
    await appendExperimentLog(deps, existing.name, outcome).catch(() => {})
    return
  }
  await table.put(existing.id, {
    ...existing,
    status: outcome.status === 'succeeded' ? 'success' : 'failed',
    lastJob: outcome,
    updatedAt: new Date().toISOString(),
  })
  // Best-effort: a read-only workspace must not break the settle.
  await appendExperimentLog(deps, existing.name, outcome).catch(() => {})
}

/**
 * Whether another job linked to the same experiment was submitted after
 * `job` (compared on `createdAt`, then on the id's sequence suffix —
 * submits within the same millisecond share a `createdAt`). A deleted
 * newer record is invisible here; its documented contract is already
 * "the outcome is written nowhere".
 * @param deps - open wiki domain.
 * @param job - the settled job record.
 * @returns true when a newer linked job supersedes this settle.
 */
function hasNewerLinkedJob(deps: ServerDeps, job: JobRecord): boolean {
  for (const [, other] of deps.domain.table('jobs').entries()) {
    if (other.id === job.id || other.experimentId !== job.experimentId) continue
    if (other.createdAt !== job.createdAt) {
      if (other.createdAt > job.createdAt) return true
    } else if (jobSeqOf(other.id) > jobSeqOf(job.id)) {
      return true
    }
  }
  return false
}

/** Numeric sequence suffix of one job id (`job-<ms>-<seq>`); 0 when absent. */
function jobSeqOf(id: string): number {
  const match = /-(\d+)$/.exec(id)
  return match === null ? 0 : Number(match[1])
}

/** Non-empty lines kept of one settled job's log summary written back to the experiment. */
const JOB_SUMMARY_LINES = 5
/** Character cap of one settled job's log summary. */
const JOB_SUMMARY_MAX_CHARS = 400

/**
 * Trailing log excerpt of one settled job: the last non-empty lines of the
 * stream that explains the outcome (stderr on failure, stdout on success,
 * the other stream as the fallback), capped at {@link JOB_SUMMARY_LINES}
 * lines and {@link JOB_SUMMARY_MAX_CHARS} characters. Silent jobs yield an
 * empty summary.
 * @param job - the settled job record.
 * @returns the log excerpt.
 */
function jobSummaryOf(job: JobRecord): string {
  const primary = job.status === 'failed' ? job.stderrTail : job.stdoutTail
  const fallback = job.status === 'failed' ? job.stdoutTail : job.stderrTail
  const source = primary.trim() !== '' ? primary : fallback
  const lines = source.split('\n').map(line => line.trimEnd()).filter(line => line.trim() !== '')
  const summary = lines.slice(-JOB_SUMMARY_LINES).join('\n')
  return summary.length <= JOB_SUMMARY_MAX_CHARS ? summary : `…${summary.slice(summary.length - JOB_SUMMARY_MAX_CHARS)}`
}

/**
 * Append one settled-job line to the workspace's `EXPERIMENT_LOG.md`
 * (created on first write): timestamp, experiment name, job id and
 * outcome, exit code, wall-clock seconds, and the summary's first line
 * (truncated to 120 characters).
 * @param deps - open wiki domain plus workspace root.
 * @param experimentName - the linked experiment's display name.
 * @param outcome - the settled job's write-back outcome.
 */
async function appendExperimentLog(deps: ServerDeps, experimentName: string, outcome: ExperimentJobOutcome): Promise<void> {
  const seconds = outcome.durationMs === null ? null : (outcome.durationMs / 1000).toFixed(1)
  const firstLine = outcome.summary.split('\n')[0] ?? ''
  const detail = firstLine.length > 120 ? `${firstLine.slice(0, 119)}…` : firstLine
  const line = `- ${outcome.finishedAt} \`${experimentName}\` job ${outcome.jobId} ${outcome.status}`
    + ` (exit ${outcome.exitCode === null ? 'n/a' : String(outcome.exitCode)}`
    + `${seconds === null ? '' : `, ${seconds}s`})`
    + `${detail === '' ? '' : `: ${detail}`}`
  await appendFile(join(deps.workspaceDir, 'EXPERIMENT_LOG.md'), `${line}\n`, 'utf8')
}

/**
 * Flip one linked experiment's status, linking the server on submit; a
 * deleted experiment is skipped.
 * @param deps - open wiki domain.
 * @param experimentId - the experiment record id.
 * @param status - the next lifecycle status.
 * @param serverId - the executing server, set on the submit flip only.
 */
async function markExperiment(
  deps: ServerDeps,
  experimentId: string,
  status: ExperimentStatus,
  serverId?: string,
): Promise<void> {
  const table = deps.domain.table('experiments')
  const existing = table.get(experimentId)
  if (existing === undefined) return
  await table.put(experimentId, {
    ...existing,
    status,
    serverId: serverId ?? existing.serverId,
    updatedAt: new Date().toISOString(),
  })
}
