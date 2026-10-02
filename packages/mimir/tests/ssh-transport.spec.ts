import { chmod, mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { afterEach, expect, it, vi } from 'vitest'
import { SharedSshTransport } from '../src/services/ssh-transport.ts'

const target = { host: 'example.invalid', port: 2222, username: 'researcher' }
const cleanups: Array<() => Promise<void>> = []
afterEach(async () => {
  for (const cleanup of cleanups.splice(0)) await cleanup()
  vi.unstubAllEnvs()
  vi.restoreAllMocks()
})

async function setup() {
  const directory = await mkdtemp(join(tmpdir(), 'mimir-mux-test-'))
  const log = join(directory, 'calls')
  const marker = join(directory, 'fail')
  const program = `#!/usr/bin/env node
const fs = require('node:fs');
const args = process.argv.slice(2);
fs.appendFileSync(process.env.MIMIR_MUX_LOG, JSON.stringify(args) + '\\n');
if (args.includes('-O')) process.exit(0);
if (fs.existsSync(process.env.MIMIR_MUX_FAIL)) {
  console.error('Not allowed at this time'); process.exit(255);
}
setTimeout(() => { console.log(args.at(-1)); }, 30);
`
  for (const name of ['ssh', 'scp', 'rsync']) {
    await writeFile(join(directory, name), program)
    await chmod(join(directory, name), 0o755)
  }
  vi.stubEnv('PATH', `${directory}:${process.env.PATH ?? ''}`)
  vi.stubEnv('MIMIR_MUX_LOG', log)
  vi.stubEnv('MIMIR_MUX_FAIL', marker)
  const transport = new SharedSshTransport()
  cleanups.push(async () => { await transport.close(); await rm(directory, { recursive: true, force: true }) })
  const calls = async (): Promise<string[][]> => (await readFile(log, 'utf8')).trim().split('\n').map(line => JSON.parse(line))
  return { transport, calls, marker }
}

it('uses one private control socket for commands and transfers and closes it locally', async () => {
  const { transport, calls } = await setup()
  await Promise.all([
    transport.ssh(target, 'squeue'),
    transport.ssh(target, 'logs'),
    transport.run(target, 'scp', options => [...options, 'file']),
  ])
  const commands = await calls()
  expect(commands.map(args => args.at(-1))).toEqual(['squeue', 'logs', 'file'])
  const sockets = commands.map(args => args.find(arg => arg.startsWith('ControlPath=')))
  expect(new Set(sockets).size).toBe(1)
  expect(commands.every(args => args.includes('ControlMaster=auto') && args.includes('ConnectionAttempts=1'))).toBe(true)
  const path = sockets[0]!.slice('ControlPath='.length)
  expect((await stat(dirname(path))).mode & 0o777).toBe(0o700)
  await transport.ssh({ ...target, username: 'other' }, 'check')
  expect((await calls())[3]?.find(arg => arg.startsWith('ControlPath='))).not.toBe(sockets[0])
  await transport.close()
  expect((await calls()).filter(args => args.includes('-O')).length).toBe(2)
  await expect(stat(dirname(path))).rejects.toThrow()
  await expect(transport.ssh(target, 'after-close')).rejects.toThrow('closed')
})

it('stops queued requests after one transport failure and retries only on a later explicit call', async () => {
  const { transport, calls, marker } = await setup()
  await writeFile(marker, '')
  const result = await Promise.allSettled([
    transport.ssh(target, 'first'), transport.ssh(target, 'second'), transport.ssh(target, 'third'),
  ])
  expect(result.every(item => item.status === 'rejected')).toBe(true)
  expect(await calls()).toHaveLength(1)
  await rm(marker)
  await expect(transport.ssh(target, 'too-soon')).rejects.toThrow('5 minutes')
  expect(await calls()).toHaveLength(1)
  const now = Date.now()
  vi.spyOn(Date, 'now').mockReturnValue(now + 300_001)
  expect((await transport.ssh(target, 'manual-retry')).stdout).toContain('manual-retry')
  expect(await calls()).toHaveLength(2)
})
