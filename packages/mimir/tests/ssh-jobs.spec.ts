/**
 * Behavior tests for the ssh job remotes: submitJob validation and the
 * background run's status flips (queued → running → succeeded/failed) with
 * the output tails and the linked-experiment write-back, plus listJobs
 * ordering/filtering and deleteJob. Real memory-backed domain, real temp
 * workspace; the ssh client itself is exercised through a fake `ssh`
 * executable shimmed onto PATH (a real ssh against a refused port covers
 * the session-failure path) — no module mocks.
 */

import { chmod, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it, vi, afterEach } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import Storage, { storageBackendServiceKey } from '@deepseek-ai/dsh-storage'
import { DomainFacility } from '@deepseek-ai/dsh-storage-domain'
import { MemoryMediaPool, MemoryStorageBackend } from './helpers/memory-backend.ts'
import { researchWikiDomainSpec } from '../src/store.ts'
import { recoverInterruptedJobs } from '../src/services/server.ts'
import { isActiveJobStatus, isTerminalJobStatus, JOB_ACTIVE_STATUSES, JOB_STATUSES } from '../src/job-lifecycle.ts'
import { ResearchService } from '../src/service.ts'
import { configurationHash } from '../src/services/slurm.ts'
import type { ExperimentRecord, JobRecord } from '../src/types.ts'

/** Boot a service over a memory-backed domain and a fresh temp workspace. */
async function harness(jobs?: { timeoutMs?: number; maxBufferBytes?: number }) {
  const ctx = new Context()
  await ctx.plugin(Storage)
  const backend = new MemoryStorageBackend(new MemoryMediaPool())
  ctx.storage.backend.register('memory', backend)
  ctx.provide(storageBackendServiceKey('memory'), backend)
  const facility = new DomainFacility(ctx, { backend: 'memory', routes: {} })
  ctx.storage.mount('domain', facility)
  const domain = await facility.open(researchWikiDomainSpec)
  const workspaceDir = await mkdtemp(join(tmpdir(), 'mimir-jobs-'))
  const service = new ResearchService(ctx, {
    workspaceDir,
    domain,
    latex: { engine: 'auto', timeoutMs: 1000 },
    ...(jobs === undefined ? {} : { jobs }),
  })
  return { ctx, domain, workspaceDir, service }
}

const SERVER_INPUT = { name: 'gpu01', host: '127.0.0.1', port: 22, username: 'ops', note: '' }

const EXPERIMENT: ExperimentRecord = {
  id: 'exp-1',
  projectId: 'p1',
  name: 'baseline',
  status: 'failed',
  metrics: {},
  updatedAt: '2026-08-20T00:00:00.000Z',
}

/**
 * Shim a fake `ssh` onto PATH: it echoes the remote command (its last
 * argument) to stdout, writes one stderr line, sleeps a moment when the
 * command contains `mimir-slow`, floods stdout past any small capture cap
 * when it contains `mimir-flood`, and exits 3 when the command contains
 * `mimir-fail`.
 * @returns the harness cleanup; PATH restores via `vi.unstubAllEnvs`.
 */
async function stubFakeSsh(): Promise<void> {
  const binDir = await mkdtemp(join(tmpdir(), 'mimir-fake-ssh-'))
  const script = [
    '#!/bin/bash',
    'while [ $# -gt 1 ]; do shift; done',
    'echo "fake-ssh stdout: $1"',
    'echo "fake-ssh stderr line" >&2',
    'case "$1" in *mimir-fail*) exit 3 ;; esac',
    'case "$1" in *mimir-flood*) head -c 65536 /dev/zero | tr "\\0" "x" ;; esac',
    'case "$1" in *mimir-slow*) sleep 0.5 ;; esac',
    'exit 0',
    '',
  ].join('\n')
  await writeFile(join(binDir, 'ssh'), script)
  await chmod(join(binDir, 'ssh'), 0o755)
  vi.stubEnv('PATH', `${binDir}:${process.env.PATH ?? ''}`)
}

/** Poll listJobs until one job reaches a terminal status. */
async function settleJob(service: ResearchService, id: string): Promise<JobRecord> {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const listed = await service.listJobs({})
    if (listed.ok) {
      const job = listed.value.jobs.find(record => record.id === id)
      if (job !== undefined && !isActiveJob(job)) return job
    }
    await new Promise(resolve => setTimeout(resolve, 25))
  }
  throw new Error(`job ${id} did not settle`)
}

/** A queued/running job still owns an in-process SSH session. */
function isActiveJob(job: JobRecord): boolean {
  return job.status === 'queued' || job.status === 'running'
}

afterEach(() => { vi.unstubAllEnvs() })

/** Scheduler simulation that exercises actual child processes and staging files. */
async function stubFakeSlurm(): Promise<{ stateFile: string; uploadDir: string }> {
  const binDir = await mkdtemp(join(tmpdir(), 'mimir-fake-slurm-'))
  const uploadDir = join(binDir, 'uploaded')
  const stateFile = join(binDir, 'state')
  await mkdir(uploadDir)
  await writeFile(stateFile, 'PENDING')
  const ssh = [
    '#!/bin/bash',
    'while [ $# -gt 1 ]; do shift; done',
    'command=$1',
    'state=$(cat "$MIMIR_FAKE_SLURM_STATE")',
    'case "$command" in',
    '  sbatch*)',
    '    case "$state" in',
    '      SUBMIT_LOST) echo "connection lost after send" >&2; exit 255 ;;',
    '      REJECT) echo "invalid partition" >&2; exit 2 ;;',
    '    esac',
    '    echo "4242" ;;',
    '  squeue*)',
    '    case "$state" in',
    '      ERROR) echo "scheduler unavailable" >&2; exit 255 ;;',
    '      PENDING|RUNNING) echo "$state" ;;',
    '    esac ;;',
    '  sacct*) echo "4242|$state|0:0" ;;',
    '  scancel*) echo "CANCELLED" > "$MIMIR_FAKE_SLURM_STATE" ;;',
    '  *stdout.log*) echo "result saved" ;;',
    '  *stderr.log*) : ;;',
    'esac',
    '',
  ].join('\n')
  const scp = [
    '#!/bin/bash',
    'for arg in "$@"; do',
    '  case "$arg" in',
    '    */job.sh|*/parameters.json|*/manifest.json) cp "$arg" "$MIMIR_FAKE_SLURM_UPLOAD/" ;;',
    '  esac',
    'done',
    '',
  ].join('\n')
  await writeFile(join(binDir, 'ssh'), ssh)
  await writeFile(join(binDir, 'scp'), scp)
  await chmod(join(binDir, 'ssh'), 0o755)
  await chmod(join(binDir, 'scp'), 0o755)
  vi.stubEnv('MIMIR_FAKE_SLURM_STATE', stateFile)
  vi.stubEnv('MIMIR_FAKE_SLURM_UPLOAD', uploadDir)
  vi.stubEnv('PATH', `${binDir}:${process.env.PATH ?? ''}`)
  return { stateFile, uploadDir }
}

async function waitForSlurmId(service: ResearchService, id: string): Promise<JobRecord> {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const listed = await service.listJobs({})
    if (listed.ok) {
      const job = listed.value.jobs.find(record => record.id === id)
      if (job?.slurmJobId !== undefined) return job
    }
    await new Promise(resolve => setTimeout(resolve, 20))
  }
  throw new Error('Slurm submission did not return an id')
}

describe('Slurm job integration', () => {
  it('stages script and canonical parameters, survives recovery, and writes back only after scheduler completion', async () => {
    const { stateFile, uploadDir } = await stubFakeSlurm()
    const { service, domain, workspaceDir } = await harness()
    const saved = await service.saveServer({ server: SERVER_INPUT })
    if (!saved.ok) throw new Error('server setup failed')
    await domain.table('experiments').put(EXPERIMENT.id, EXPERIMENT)
    const script = '#!/bin/bash\n#SBATCH --time=00:05:00\npython train.py parameters.json'
    const submitted = await service.submitJob({
      serverId: saved.value.server.id, command: script, mode: 'slurm',
      parametersJson: '{"beta":2,"alpha":1}', experimentId: EXPERIMENT.id,
    })
    if (!submitted.ok) throw new Error('submit failed')
    const id = submitted.value.job.id
    const pending = await waitForSlurmId(service, id)
    expect(pending).toMatchObject({ mode: 'slurm', slurmJobId: '4242', status: 'queued' })
    expect(pending.configHash).toBe(configurationHash(script, { alpha: 1, beta: 2 }))
    expect(JSON.parse(await readFile(join(uploadDir, 'parameters.json'), 'utf8'))).toEqual({ alpha: 1, beta: 2 })
    expect((await readFile(join(uploadDir, 'job.sh'), 'utf8'))).toContain('#SBATCH --time=00:05:00')
    expect(JSON.parse(await readFile(join(uploadDir, 'manifest.json'), 'utf8')).configHash).toBe(pending.configHash)
    await recoverInterruptedJobs({ domain, workspaceDir })
    expect(domain.table('jobs').get(id)?.status).toBe('queued')
    await writeFile(stateFile, 'RUNNING')
    await service.listJobs({})
    expect(domain.table('jobs').get(id)?.status).toBe('running')
    await writeFile(stateFile, 'ERROR')
    await service.listJobs({})
    expect(domain.table('jobs').get(id)?.status).toBe('running')
    expect(domain.table('jobs').get(id)?.statusNote).toContain('unavailable')
    await writeFile(stateFile, 'COMPLETED')
    const settled = await settleJob(service, id)
    expect(settled).toMatchObject({ status: 'succeeded', exitCode: 0, stdoutTail: 'result saved\n' })
    expect(domain.table('experiments').get(EXPERIMENT.id)?.status).toBe('success')
  })

  it('uses scancel and waits for scheduler confirmation', async () => {
    await stubFakeSlurm()
    const { service } = await harness()
    const saved = await service.saveServer({ server: SERVER_INPUT })
    if (!saved.ok) throw new Error('server setup failed')
    const submitted = await service.submitJob({ serverId: saved.value.server.id, command: 'true', mode: 'slurm' })
    if (!submitted.ok) throw new Error('submit failed')
    const job = await waitForSlurmId(service, submitted.value.job.id)
    await expect(service.deleteJob({ id: job.id })).resolves.toEqual({ ok: true, value: { id: job.id } })
    const settled = await settleJob(service, job.id)
    expect(settled.status).toBe('cancelled')
  })

  it('distinguishes an ambiguous sbatch transport loss from a definite rejection', async () => {
    const { stateFile } = await stubFakeSlurm()
    const { service } = await harness()
    const saved = await service.saveServer({ server: SERVER_INPUT })
    if (!saved.ok) throw new Error('server setup failed')
    const serverId = saved.value.server.id
    await expect(service.submitJob({ serverId, command: 'true', mode: 'slurm', parametersJson: '[]' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'invalid-input' } })
    await writeFile(stateFile, 'SUBMIT_LOST')
    const ambiguous = await service.submitJob({ serverId, command: 'true', mode: 'slurm' })
    if (!ambiguous.ok) throw new Error('submit failed')
    expect((await settleJob(service, ambiguous.value.job.id)).status).toBe('unknown')
    await writeFile(stateFile, 'REJECT')
    const rejected = await service.submitJob({ serverId, command: 'true', mode: 'slurm' })
    if (!rejected.ok) throw new Error('submit failed')
    expect((await settleJob(service, rejected.value.job.id)).status).toBe('failed')
  })
})

describe('job lifecycle contract (#225)', () => {
  it('defines exactly the seven statuses, active vs terminal', () => {
    expect(JOB_STATUSES).toEqual(['queued', 'running', 'succeeded', 'failed', 'cancelled', 'interrupted', 'unknown'])
    expect(JOB_ACTIVE_STATUSES).toEqual(['queued', 'running'])
    for (const status of JOB_STATUSES) {
      expect(isActiveJobStatus(status)).toBe(status === 'queued' || status === 'running')
      expect(isTerminalJobStatus(status)).toBe(!isActiveJobStatus(status))
    }
  })

  it('keeps the store schema open for every status of the machine', async () => {
    const { domain } = await harness()
    for (const status of JOB_STATUSES) {
      const record: JobRecord = {
        id: `job-${status}`, serverId: 'srv-1', command: 'hostname', status,
        exitCode: null, stdoutTail: '', stderrTail: '', createdAt: '2026-08-20T00:00:00.000Z',
      }
      await domain.table('jobs').put(record.id, record)
      expect(domain.table('jobs').get(record.id)?.status).toBe(status)
    }
  })
})

describe('ResearchService.submitJob validation', () => {
  it('rejects an unknown server, an empty/overlong command, a TCP-only server, and an unknown experiment', async () => {
    const { service } = await harness()
    await expect(service.submitJob({ serverId: 'srv-missing', command: 'nvidia-smi' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'server-not-found', id: 'srv-missing' } })
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    const serverId = created.value.server.id
    await expect(service.submitJob({ serverId, command: '   ' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'invalid-input' } })
    await expect(service.submitJob({ serverId, command: 'x'.repeat(4001) }))
      .resolves.toMatchObject({ ok: false, error: { code: 'invalid-input' } })
    await expect(service.submitJob({ serverId, command: 'nvidia-smi', experimentId: 'exp-missing' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'experiment-not-found', id: 'exp-missing' } })
    const tcpOnly = await service.saveServer({ server: { ...SERVER_INPUT, username: '' } })
    if (!tcpOnly.ok) throw new Error('create failed')
    await expect(service.submitJob({ serverId: tcpOnly.value.server.id, command: 'nvidia-smi' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'invalid-input' } })
    // Nothing was queued by the rejections.
    await expect(service.listJobs({})).resolves.toEqual({ ok: true, value: { jobs: [] } })
  })
})

describe('ResearchService job recovery', () => {
  it('marks pre-restart active jobs interrupted and releases their linked experiment', async () => {
    const { domain, workspaceDir } = await harness()
    const job: JobRecord = {
      id: 'job-running', serverId: 'srv-1', command: 'python train.py', status: 'running',
      experimentId: EXPERIMENT.id, exitCode: null, stdoutTail: '', stderrTail: '',
      createdAt: '2026-08-21T00:00:00.000Z', startedAt: '2026-08-21T00:00:01.000Z',
    }
    await domain.table('experiments').put(EXPERIMENT.id, { ...EXPERIMENT, status: 'running' })
    await domain.table('jobs').put(job.id, job)

    await recoverInterruptedJobs({ workspaceDir, domain })

    expect(domain.table('jobs').get(job.id)).toMatchObject({
      status: 'interrupted', exitCode: null, stderrTail: 'host restarted before the job outcome was known',
    })
    expect(domain.table('experiments').get(EXPERIMENT.id)).toMatchObject({ status: 'failed' })
  })

  it('does not overwrite an experiment owned by a newer settled job', async () => {
    const { domain, workspaceDir } = await harness()
    const stale: JobRecord = {
      id: 'job-old-1', serverId: 'srv-1', command: 'python old.py', status: 'running',
      experimentId: EXPERIMENT.id, exitCode: null, stdoutTail: '', stderrTail: '',
      createdAt: '2026-08-21T00:00:00.000Z',
    }
    const newer: JobRecord = {
      ...stale, id: 'job-new-2', command: 'python new.py', status: 'succeeded', exitCode: 0,
      createdAt: '2026-08-21T00:01:00.000Z', finishedAt: '2026-08-21T00:02:00.000Z',
    }
    await domain.table('experiments').put(EXPERIMENT.id, { ...EXPERIMENT, status: 'success' })
    await domain.table('jobs').put(stale.id, stale)
    await domain.table('jobs').put(newer.id, newer)

    await recoverInterruptedJobs({ workspaceDir, domain })

    expect(domain.table('jobs').get(stale.id)?.status).toBe('interrupted')
    expect(domain.table('jobs').get(newer.id)?.status).toBe('succeeded')
    expect(domain.table('experiments').get(EXPERIMENT.id)?.status).toBe('success')
  })
})
describe('ResearchService job lifecycle', () => {
  it('omits experimentId from an unlinked job instead of writing undefined', async () => {
    const { domain, service } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    // `undefined` values would trip the gateway's JSON boundary validation
    // and pollute the stored record, so the key must be absent entirely.
    const submitted = await service.submitJob({ serverId: created.value.server.id, command: 'echo unlinked' })
    if (!submitted.ok) throw new Error('submit rejected')
    expect('experimentId' in submitted.value.job).toBe(false)
    expect('experimentId' in domain.table('jobs').get(submitted.value.job.id)!).toBe(false)
    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('succeeded')
  })

  it('runs a submitted job to succeeded, keeps the output tails, and writes back the linked experiment', async () => {
    const { domain, service, workspaceDir } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    const serverId = created.value.server.id
    await domain.table('experiments').put(EXPERIMENT.id, EXPERIMENT)

    const submitted = await service.submitJob({
      serverId,
      command: 'python train.py --epochs 1',
      experimentId: EXPERIMENT.id,
    })
    if (!submitted.ok) throw new Error('submit rejected')
    expect(submitted.value.job.status).toBe('queued')
    expect(submitted.value.job.id).toMatch(/^job-/)
    expect(submitted.value.job.experimentId).toBe(EXPERIMENT.id)
    // The submit flip: the linked experiment runs on the job's server.
    expect(domain.table('experiments').get(EXPERIMENT.id)).toMatchObject({
      status: 'running',
      serverId,
    })

    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('succeeded')
    expect(settled.exitCode).toBe(0)
    expect(settled.stdoutTail).toContain('fake-ssh stdout: python train.py --epochs 1')
    expect(settled.stderrTail).toContain('fake-ssh stderr line')
    expect(Date.parse(settled.startedAt ?? '')).not.toBeNaN()
    expect(Date.parse(settled.finishedAt ?? '')).not.toBeNaN()
    // The settle flip: the linked experiment lands on success, carrying the
    // job's outcome (exit code, duration, log excerpt) as `lastJob`.
    const experiment = domain.table('experiments').get(EXPERIMENT.id)
    expect(experiment?.status).toBe('success')
    expect(experiment?.lastJob).toMatchObject({
      jobId: settled.id,
      status: 'succeeded',
      exitCode: 0,
      finishedAt: settled.finishedAt,
    })
    expect(experiment?.lastJob?.durationMs).not.toBeNull()
    expect(experiment?.lastJob?.summary).toContain('fake-ssh stdout: python train.py --epochs 1')
    // The workspace's EXPERIMENT_LOG.md records the settle as one line.
    const log = await readFile(join(workspaceDir, 'EXPERIMENT_LOG.md'), 'utf8')
    expect(log).toContain(`job ${settled.id} succeeded (exit 0`)
    expect(log).toContain('fake-ssh stdout: python train.py --epochs 1')
  })

  it('settles a non-zero remote exit as failed with the exit code and flips the experiment to failed', async () => {
    const { domain, service, workspaceDir } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    await domain.table('experiments').put(EXPERIMENT.id, EXPERIMENT)

    const submitted = await service.submitJob({
      serverId: created.value.server.id,
      command: 'echo mimir-fail',
      experimentId: EXPERIMENT.id,
    })
    if (!submitted.ok) throw new Error('submit rejected')
    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('failed')
    expect(settled.exitCode).toBe(3)
    expect(settled.stderrTail).toContain('fake-ssh stderr line')
    const experiment = domain.table('experiments').get(EXPERIMENT.id)
    expect(experiment?.status).toBe('failed')
    // The failure summary prefers the stderr tail.
    expect(experiment?.lastJob).toMatchObject({ jobId: settled.id, status: 'failed', exitCode: 3 })
    expect(experiment?.lastJob?.summary).toContain('fake-ssh stderr line')
    const log = await readFile(join(workspaceDir, 'EXPERIMENT_LOG.md'), 'utf8')
    expect(log).toContain(`job ${settled.id} failed (exit 3`)
  })

  it('settles an ssh session failure as failed with the client message and a null exit code', async () => {
    const { service } = await harness()
    // No shim: the real ssh hits a refused port and fails fast.
    const created = await service.saveServer({
      server: { ...SERVER_INPUT, host: '127.0.0.1', port: 19999 },
    })
    if (!created.ok) throw new Error('create failed')
    const submitted = await service.submitJob({ serverId: created.value.server.id, command: 'hostname' })
    if (!submitted.ok) throw new Error('submit rejected')
    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('failed')
    expect(settled.stderrTail).toBeTruthy()
  }, 20_000)
})

describe('ResearchService linked-experiment stale-settle guard', () => {
  /** Boot the harness with one server and one linked experiment. */
  async function linkedHarness() {
    const { ctx, domain, workspaceDir, service } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    await domain.table('experiments').put(EXPERIMENT.id, EXPERIMENT)
    const serverId = created.value.server.id
    const submit = async (command: string): Promise<JobRecord> => {
      const submitted = await service.submitJob({ serverId, command, experimentId: EXPERIMENT.id })
      if (!submitted.ok) throw new Error('submit rejected')
      return submitted.value.job
    }
    return { ctx, domain, workspaceDir, service, submit }
  }

  it('does not let an old job settling last overwrite the newer job’s write-back', async () => {
    const { domain, service, workspaceDir, submit } = await linkedHarness()
    const older = await submit('mimir-slow python train.py --epochs 20')
    const newer = await submit('python train.py --epochs 1')

    // The newer job settles first and owns the experiment's state.
    const settledNewer = await settleJob(service, newer.id)
    expect(settledNewer.status).toBe('succeeded')
    expect(domain.table('experiments').get(EXPERIMENT.id)).toMatchObject({
      status: 'success',
      lastJob: { jobId: newer.id, status: 'succeeded' },
    })

    // The older job settles late: its outcome lands only in the log, the
    // record keeps the newer job's status and `lastJob`.
    const settledOlder = await settleJob(service, older.id)
    expect(settledOlder.status).toBe('succeeded')
    // Give the (skipped) write-back a beat to prove it stays skipped.
    await new Promise(resolve => setTimeout(resolve, 100))
    expect(domain.table('experiments').get(EXPERIMENT.id)).toMatchObject({
      status: 'success',
      lastJob: { jobId: newer.id, status: 'succeeded' },
    })
    const log = await readFile(join(workspaceDir, 'EXPERIMENT_LOG.md'), 'utf8')
    expect(log).toContain(`job ${newer.id} succeeded`)
    expect(log).toContain(`job ${older.id} succeeded`)
  })

  it('does not let an old job settling first flip the status while the newer job still runs', async () => {
    const { domain, service, submit } = await linkedHarness()
    const older = await submit('echo mimir-fail')
    const newer = await submit('mimir-slow python train.py --epochs 20')

    // The older job fails fast; the newer job still runs and owns the
    // experiment, so the failed settle leaves the record `running`.
    const settledOlder = await settleJob(service, older.id)
    expect(settledOlder.status).toBe('failed')
    await new Promise(resolve => setTimeout(resolve, 100))
    const midway = domain.table('experiments').get(EXPERIMENT.id)
    expect(midway?.status).toBe('running')
    expect(midway?.lastJob).toBeUndefined()

    // The newer job's settle then writes back normally.
    const settledNewer = await settleJob(service, newer.id)
    expect(settledNewer.status).toBe('succeeded')
    expect(domain.table('experiments').get(EXPERIMENT.id)).toMatchObject({
      status: 'success',
      lastJob: { jobId: newer.id, status: 'succeeded' },
    })
  })
})

describe('ResearchService.listJobs / deleteJob', () => {
  it('lists newest first, filters by server, and rejects an unknown filter id', async () => {
    const { domain, service } = await harness()
    const first = await service.saveServer({ server: SERVER_INPUT })
    // saveServer ids are millisecond-based; keep the two servers distinct.
    await new Promise(resolve => setTimeout(resolve, 2))
    const second = await service.saveServer({ server: { ...SERVER_INPUT, name: 'gpu02' } })
    if (!first.ok || !second.ok) throw new Error('create failed')
    const older: JobRecord = {
      id: 'job-old', serverId: first.value.server.id, command: 'hostname', status: 'succeeded',
      exitCode: 0, stdoutTail: '', stderrTail: '',
      createdAt: '2026-08-20T00:00:00.000Z', finishedAt: '2026-08-20T00:00:01.000Z',
    }
    const newer: JobRecord = {
      ...older, id: 'job-new', serverId: second.value.server.id,
      createdAt: '2026-08-21T00:00:00.000Z', finishedAt: '2026-08-21T00:00:01.000Z',
    }
    await domain.table('jobs').put(older.id, older)
    await domain.table('jobs').put(newer.id, newer)
    const all = await service.listJobs({})
    if (!all.ok) throw new Error('list rejected')
    expect(all.value.jobs.map(job => job.id)).toEqual(['job-new', 'job-old'])
    const filtered = await service.listJobs({ serverId: first.value.server.id })
    if (!filtered.ok) throw new Error('list rejected')
    expect(filtered.value.jobs.map(job => job.id)).toEqual(['job-old'])
    await expect(service.listJobs({ serverId: 'srv-missing' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'server-not-found', id: 'srv-missing' } })
  })

  it('cancels an active job instead of deleting an unowned remote command', async () => {
    const { service } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    const submitted = await service.submitJob({
      serverId: created.value.server.id,
      command: 'mimir-slow python train.py --epochs 20',
    })
    if (!submitted.ok) throw new Error('submit rejected')
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const listed = await service.listJobs({})
      if (listed.ok && listed.value.jobs.some(job => job.id === submitted.value.job.id && job.status === 'running')) break
      await new Promise(resolve => setTimeout(resolve, 25))
    }

    await expect(service.deleteJob({ id: submitted.value.job.id })).resolves.toEqual({
      ok: true,
      value: { id: submitted.value.job.id },
    })
    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled).toMatchObject({
      status: 'cancelled',
      exitCode: null,
      stderrTail: expect.stringContaining('remote process outcome is unknown'),
    })
  })

  it('interrupts an active job when its host service is disposed', async () => {
    const { ctx, service } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    const submitted = await service.submitJob({
      serverId: created.value.server.id,
      command: 'mimir-slow python train.py --epochs 20',
    })
    if (!submitted.ok) throw new Error('submit rejected')

    await ctx.fiber.dispose()
    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled).toMatchObject({
      status: 'interrupted',
      exitCode: null,
      stderrTail: expect.stringContaining('host stopped'),
    })
  })

  it('settles a duration-cap kill as unknown, never claiming a remote outcome (#225)', async () => {
    const { domain, service } = await harness({ timeoutMs: 150 })
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    await domain.table('experiments').put(EXPERIMENT.id, EXPERIMENT)
    const submitted = await service.submitJob({
      serverId: created.value.server.id,
      command: 'mimir-slow python train.py --epochs 20',
      experimentId: EXPERIMENT.id,
    })
    if (!submitted.ok) throw new Error('submit rejected')

    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('unknown')
    expect(settled.exitCode).toBeNull()
    expect(settled.stderrTail).toContain('may still be running')
    // The coarser experiment lifecycle maps the unobserved settle to failed.
    expect(domain.table('experiments').get(EXPERIMENT.id)).toMatchObject({
      status: 'failed',
      lastJob: { jobId: settled.id, status: 'failed', exitCode: null },
    })
  })

  it('settles output past the capture cap as an explicit failure, never a silent truncation (#225)', async () => {
    const { service } = await harness({ maxBufferBytes: 512 })
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    const submitted = await service.submitJob({
      serverId: created.value.server.id,
      command: 'mimir-flood python train.py',
    })
    if (!submitted.ok) throw new Error('submit rejected')

    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('failed')
    expect(settled.exitCode).toBeNull()
    expect(settled.stderrTail).toContain('capture limit')
  })

  it('writes a cancelled linked job back to the experiment without a schema rejection (#247)', async () => {
    const { domain, service } = await harness()
    await stubFakeSsh()
    const created = await service.saveServer({ server: SERVER_INPUT })
    if (!created.ok) throw new Error('create failed')
    await domain.table('experiments').put(EXPERIMENT.id, EXPERIMENT)
    const submitted = await service.submitJob({
      serverId: created.value.server.id,
      command: 'mimir-slow python train.py --epochs 20',
      experimentId: EXPERIMENT.id,
    })
    if (!submitted.ok) throw new Error('submit rejected')
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const listed = await service.listJobs({})
      if (listed.ok && listed.value.jobs.some(job => job.id === submitted.value.job.id && job.status === 'running')) break
      await new Promise(resolve => setTimeout(resolve, 25))
    }

    await expect(service.deleteJob({ id: submitted.value.job.id })).resolves.toEqual({
      ok: true,
      value: { id: submitted.value.job.id },
    })
    const settled = await settleJob(service, submitted.value.job.id)
    expect(settled.status).toBe('cancelled')
    // The store schema must accept the cancelled outcome: the write-back
    // lands instead of dying on the enum (#247). The job's terminal write
    // precedes the write-back inside one settle, so poll for the latter.
    let experiment = domain.table('experiments').get(EXPERIMENT.id)
    for (let attempt = 0; attempt < 200 && experiment?.lastJob === undefined; attempt += 1) {
      await new Promise(resolve => setTimeout(resolve, 25))
      experiment = domain.table('experiments').get(EXPERIMENT.id)
    }
    expect(experiment).toMatchObject({
      status: 'failed',
      lastJob: { jobId: settled.id, status: 'cancelled', exitCode: null },
    })
  })

  it('deletes a record and reports job-not-found on a repeat', async () => {
    const { domain, service } = await harness()
    const record: JobRecord = {
      id: 'job-1', serverId: 'srv-1', command: 'hostname', status: 'succeeded',
      exitCode: 0, stdoutTail: '', stderrTail: '', createdAt: '2026-08-20T00:00:00.000Z',
    }
    await domain.table('jobs').put(record.id, record)
    await expect(service.deleteJob({ id: 'job-1' })).resolves.toEqual({ ok: true, value: { id: 'job-1' } })
    await expect(service.deleteJob({ id: 'job-1' }))
      .resolves.toMatchObject({ ok: false, error: { code: 'job-not-found', id: 'job-1' } })
    await expect(service.listJobs({})).resolves.toEqual({ ok: true, value: { jobs: [] } })
  })
})
