/**
 * The servers view's remote-jobs section: a submit form (server select,
 * command input, and an optional experiment link from the selected project's
 * loaded experiments) above the job table — status pill, command, linked
 * server, submit time, expandable stdout/stderr tails, delete. While any job
 * is a locally owned SSH job, the section polls saved Host state every
 * {@link JOB_POLL_INTERVAL_MS}; a terminal flip observed between polls
 * surfaces as a toast (controller-side). Slurm queries require a manual refresh.
 * @module dsh-client-ui-mimir/client/JobsSection
 */

import { useEffect, useState } from 'react'
import type { ExperimentRecord, JobRecord } from 'dsh-mimir/types'
import type {
  ResearchFailureView, ResearchJobsView, ResearchProjectSlice, ResearchServersView,
} from './controller.ts'
import { failureCopy, relativeTime, type ResearchT } from './view-common.ts'
import css from './ResearchPanel.module.css'

/** Poll cadence while any job is queued/running. */
const JOB_POLL_INTERVAL_MS = 2000

/** Whether one job still has a status flip coming (the polling condition). */
function isActive(job: JobRecord): boolean {
  return job.status === 'queued' || job.status === 'running'
}

/**
 * @param props - the jobs slice, the servers slice (submit options and row
 * names), the selected project's experiments slice (the link options), the
 * ensure/poll/submit/delete verbs, and copy.
 * @returns the submit form card plus the job table.
 */
export function JobsSection({
  jobs, servers, experiments, ensureJobs, refreshJobs, submitJob, deleteJob, t,
}: {
  readonly jobs: ResearchJobsView
  readonly servers: ResearchServersView
  readonly experiments: ResearchProjectSlice<readonly ExperimentRecord[]> | null
  readonly ensureJobs: () => void
  readonly refreshJobs: (refresh?: boolean) => void
  readonly submitJob: (serverId: string, command: string, experimentId?: string, mode?: 'ssh' | 'slurm', parametersJson?: string, sourceDirectory?: string) => Promise<ResearchFailureView | null>
  readonly deleteJob: (id: string) => Promise<ResearchFailureView | null>
  readonly t: ResearchT
}) {
  const [serverId, setServerId] = useState('')
  const [experimentId, setExperimentId] = useState('')
  const [command, setCommand] = useState('')
  const [mode, setMode] = useState<'ssh' | 'slurm'>('ssh')
  const [parametersJson, setParametersJson] = useState('{}')
  const [sourceDirectory, setSourceDirectory] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [openOutput, setOpenOutput] = useState<Record<string, boolean>>({})

  useEffect(() => { ensureJobs() }, [ensureJobs])
  // Only repaint locally owned SSH jobs automatically. Slurm queries are explicit.
  useEffect(() => {
    if (jobs.status !== 'ready' || !jobs.list.some(job => isActive(job) && job.mode !== 'slurm')) return
    const timer = setInterval(() => { refreshJobs(false) }, JOB_POLL_INTERVAL_MS)
    return () => { clearInterval(timer) }
  }, [jobs, refreshJobs])

  const serverNameOf = (id: string): string =>
    servers.status === 'ready' ? servers.list.find(server => server.id === id)?.name ?? id : id
  const experimentNameOf = (id: string): string =>
    experiments !== null && experiments.status === 'ready'
      ? experiments.list.find(record => record.id === id)?.name ?? id
      : id
  // The form's server select defaults to the first listed server.
  const selectedServer = serverId !== '' ? serverId : servers.list[0]?.id ?? ''
  const experimentOptions = experiments !== null && experiments.status === 'ready' ? experiments.list : []

  const submit = (): void => {
    if (submitting || selectedServer === '' || command.trim() === '') return
    setSubmitting(true)
    setFormError(null)
    void submitJob(selectedServer, command.trim(), experimentId === '' ? undefined : experimentId,
      mode, mode === 'slurm' ? parametersJson : undefined,
      mode === 'slurm' && sourceDirectory.trim() !== '' ? sourceDirectory.trim() : undefined)
      .then((failure) => {
        if (failure !== null) {
          setFormError(failure.message)
          return
        }
        setCommand('')
        setExperimentId('')
      })
      .finally(() => { setSubmitting(false) })
  }
  const removeJob = (record: JobRecord): void => {
    if (!window.confirm(t(isActive(record) && record.mode === 'slurm' ? 'jobs.confirmCancel' : 'jobs.confirmDelete'))) return
    void deleteJob(record.id).then((failure) => {
      setActionError(failure === null ? null : `${t('jobs.deleteFailed')}：${failure.message}`)
    })
  }

  return (
    <div className={css.jobsSection}>
      <h3 className={css.sectionTitle}>{t('jobs.title')}</h3>
      <p className={css.hint}>{t('jobs.manualRefreshHelp')}</p>
      <button type="button" className={css.btn} onClick={() => { refreshJobs(true) }}>
        {t('jobs.refreshStatus')}
      </button>
      <div className={css.jobForm}>
        <label className={css.field}>
          <span className={css.fieldLabel}>{t('jobs.mode')}</span>
          <select className={css.serverSelect} value={mode} onChange={event => { setMode(event.target.value as 'ssh' | 'slurm') }}>
            <option value="ssh">SSH</option>
            <option value="slurm">Slurm</option>
          </select>
        </label>
        <label className={css.field}>
          <span className={css.fieldLabel}>{t('jobs.server')}</span>
          <select
            className={css.serverSelect}
            value={selectedServer}
            aria-label={t('jobs.server')}
            onChange={(event) => { setServerId(event.target.value) }}
          >
            {servers.list.map(server => (
              <option key={server.id} value={server.id}>{server.name}</option>
            ))}
          </select>
        </label>
        {experimentOptions.length > 0 && (
          <label className={css.field}>
            <span className={css.fieldLabel}>{t('jobs.experiment')}</span>
            <select
              className={css.serverSelect}
              value={experimentId}
              aria-label={t('jobs.experiment')}
              onChange={(event) => { setExperimentId(event.target.value) }}
            >
              <option value="">{t('jobs.noExperiment')}</option>
              {experimentOptions.map(record => (
                <option key={record.id} value={record.id}>{record.name}</option>
              ))}
            </select>
          </label>
        )}
        <label className={css.field} data-wide data-script={mode === 'slurm' || undefined}>
          <span className={css.fieldLabel}>{mode === 'slurm' ? t('jobs.script') : t('jobs.command')}</span>
          {mode === 'slurm' ? <textarea
            className={css.input}
            rows={8}
            value={command}
            placeholder={t('jobs.scriptPlaceholder')}
            onChange={event => { setCommand(event.target.value) }}
          /> : <input
            className={css.input}
            value={command}
            placeholder={t('jobs.commandPlaceholder')}
            onChange={event => { setCommand(event.target.value) }}
            onKeyDown={(event) => {
              if (event.key === 'Enter') submit()
            }}
          />}
        </label>
        {mode === 'slurm' && <label className={css.field} data-wide>
          <span className={css.fieldLabel}>{t('jobs.sourceDirectory')}</span>
          <input className={css.input} value={sourceDirectory} placeholder={t('jobs.sourceDirectoryPlaceholder')}
            onChange={event => { setSourceDirectory(event.target.value) }} />
          <small>{t('jobs.sourceDirectoryHelp')}</small>
        </label>}
        {mode === 'slurm' && <label className={css.field} data-wide>
          <span className={css.fieldLabel}>{t('jobs.parameters')}</span>
          <textarea className={css.input} rows={3} value={parametersJson}
            onChange={event => { setParametersJson(event.target.value) }} />
          <small>{t('jobs.parametersHelp')}</small>
        </label>}
        <button
          type="button"
          className={css.btnPrimary}
          disabled={submitting || selectedServer === '' || command.trim() === ''}
          onClick={submit}
        >
          {submitting ? t('jobs.submitting') : t('jobs.submit')}
        </button>
      </div>
      {formError !== null && <p className={css.failure} role="alert">{formError}</p>}
      {actionError !== null && <p className={css.failure} role="alert">{actionError}</p>}
      {jobs.status === 'cold' || jobs.status === 'loading' ? (
        <p className={css.hint}>{t('jobs.loading')}</p>
      ) : jobs.status === 'error' ? (
        <p className={css.failure} role="alert">
          {t('error.jobs')}：{failureCopy(t, jobs.failure)}
          <button type="button" className={css.btn} onClick={() => { refreshJobs(false) }}>
            {t('error.retry')}
          </button>
        </p>
      ) : jobs.list.length === 0 ? (
        <p className={css.hint}>{t('jobs.empty')}</p>
      ) : (
        <div className={css.experimentTableWrap}>
          <table className={css.experimentTable}>
            <thead>
              <tr>
                <th>{t('jobs.colCommand')}</th>
                <th>{t('jobs.colServer')}</th>
                <th>{t('jobs.colStatus')}</th>
                <th>{t('jobs.colCreated')}</th>
                <th>{t('experiments.colActions')}</th>
              </tr>
            </thead>
            <tbody>
              {jobs.list.map((record) => {
                const open = Boolean(openOutput[record.id])
                const hasOutput = record.stdoutTail !== '' || record.stderrTail !== ''
                return (
                  <tr key={record.id}>
                    <td>
                      <code className={css.jobCommand} title={record.command}>{record.command}</code>
                      {record.mode === 'slurm' && <span className={css.serverBadge}>Slurm {record.slurmJobId ?? '…'}</span>}
                      {record.configHash !== undefined && <span className={css.serverBadge} title={record.configHash}>SHA-256 {record.configHash.slice(0, 12)}</span>}
                      {record.remoteDir !== undefined && <code className={css.jobCommand} title={record.remoteDir}>{record.remoteDir}</code>}
                      {record.experimentId !== undefined && (
                        <span className={css.serverBadge}>🧪 {experimentNameOf(record.experimentId)}</span>
                      )}
                      {open && hasOutput && (
                        <div className={css.jobOutput}>
                          {record.stdoutTail !== '' && <pre>{record.stdoutTail}</pre>}
                          {record.stderrTail !== '' && <pre data-kind="stderr">{record.stderrTail}</pre>}
                        </div>
                      )}
                    </td>
                    <td>
                      <span className={css.serverBadge}>⚡ {serverNameOf(record.serverId)}</span>
                    </td>
                    <td>
                      <span className={css.jobStatus} data-status={record.status}>
                        {t(`jobStatus.${record.status}`)}
                      </span>
                      {record.exitCode !== null && record.exitCode !== 0 && (
                        <span className={css.jobExitCode}>{t('jobs.exitCode')} {record.exitCode}</span>
                      )}
                      {record.statusNote !== undefined && record.statusNote !== '' && <small title={record.statusNote}>{record.statusNote}</small>}
                    </td>
                    <td>{relativeTime(t, record.createdAt)}</td>
                    <td>
                      {hasOutput && (
                        <button
                          type="button"
                          className={css.metricsToggle}
                          aria-expanded={open}
                          onClick={() => {
                            setOpenOutput(prev => ({ ...prev, [record.id]: !prev[record.id] }))
                          }}
                        >
                          {t('jobs.output')}
                        </button>
                      )}
                      <button
                        type="button"
                        className={css.btn}
                        data-danger
                        onClick={() => { removeJob(record) }}
                      >
                        {isActive(record) && record.mode === 'slurm' ? t('jobs.cancel') : t('jobs.delete')}
                      </button>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
