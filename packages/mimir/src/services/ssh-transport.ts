/** Reuse authenticated SSH connections without background remote commands. */
import { execFile } from 'node:child_process'
import { createHash } from 'node:crypto'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import type { ServerRecord } from '../types.ts'

const exec = promisify(execFile)
type Target = Pick<ServerRecord, 'host' | 'port' | 'username'>
interface Session {
  target: Target
  path: string
  pending: Promise<unknown>
  retryAt: number
}

export class SharedSshTransport {
  private directory: Promise<string> | undefined
  private readonly sessions = new Map<string, Session>()
  private readonly abort = new AbortController()
  private closed = false

  private async session(target: Target): Promise<Session> {
    if (this.closed) throw new Error('SSH connections are closed')
    if (!/^[a-zA-Z0-9._:-]+$/.test(target.host) || target.host.startsWith('-')
      || !/^[a-zA-Z0-9._-]+$/.test(target.username) || target.username.startsWith('-')
      || !Number.isInteger(target.port) || target.port < 1 || target.port > 65535) {
      throw new Error('invalid SSH target')
    }
    // macOS TMPDIR can exceed the Unix-domain socket path length limit.
    this.directory ??= mkdtemp(join(process.platform === 'darwin' ? '/tmp' : tmpdir(), 'mimir-ssh-'))
    const directory = await this.directory
    if (this.closed) throw new Error('SSH connections are closed')
    const key = createHash('sha256').update(JSON.stringify([target.username, target.host, target.port])).digest('hex').slice(0, 24)
    let session = this.sessions.get(key)
    if (session === undefined) {
      session = { target, path: join(directory, key), pending: Promise.resolve(), retryAt: 0 }
      this.sessions.set(key, session)
    }
    return session
  }

  /** Serialize commands/transfers for a target so cold requests create one master. */
  async run(target: Target, program: 'ssh' | 'scp' | 'rsync', args: (options: string[]) => string[], timeout = 20_000): Promise<{ stdout: string; stderr: string }> {
    const session = await this.session(target)
    const operation = session.pending.catch(() => {}).then(async () => {
      if (this.closed) throw new Error('SSH connections are closed')
      if (Date.now() < session.retryAt) {
        throw Object.assign(new Error('SSH connection failed recently. Automatic retries are paused; try manually again after 5 minutes.'), { code: 255 })
      }
      const options = [
        '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-o', 'ConnectionAttempts=1',
        '-o', 'ControlMaster=auto', '-o', `ControlPath=${session.path}`, '-o', 'ControlPersist=1800',
        '-o', 'ServerAliveInterval=60', '-o', 'ServerAliveCountMax=2',
      ]
      try {
        return await exec(program, args(options), { timeout, maxBuffer: 1024 * 1024, signal: this.abort.signal })
      } catch (error) {
        const failure = error as { code?: unknown; killed?: boolean }
        // Failed transfers can also hide a transport failure (scp/rsync use other exit codes).
        if (failure.code === 255 || failure.killed || program !== 'ssh') session.retryAt = Date.now() + 300_000
        throw error
      }
    })
    session.pending = operation
    return operation
  }

  ssh(target: Target, command: string, timeout = 20_000): Promise<{ stdout: string; stderr: string }> {
    return this.run(target, 'ssh', options => [...options, '-p', String(target.port), '--', `${target.username}@${target.host}`, command], timeout)
  }

  /** Local control-socket commands only: unloading never opens a network connection. */
  async close(): Promise<void> {
    this.closed = true
    this.abort.abort()
    const sessions = [...this.sessions.values()]
    await Promise.allSettled(sessions.map(session => session.pending))
    await Promise.allSettled(sessions.map(session => exec('ssh', [
      '-S', session.path, '-O', 'exit', '-p', String(session.target.port), '--', `${session.target.username}@${session.target.host}`,
    ], { timeout: 2000 })))
    if (this.directory !== undefined) await rm(await this.directory, { recursive: true, force: true })
  }
}

/** Standalone helpers also share connections; service instances own their own pool. */
export const sharedSsh = new SharedSshTransport()
