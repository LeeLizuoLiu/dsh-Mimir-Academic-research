/** Install this built checkout into the user's existing DSH web profile. */
import { spawnSync } from 'node:child_process'
import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { dirname, join, resolve } from 'node:path'

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const profile = process.argv[2] ?? '/Users/leeliu/.dsh/profiles/web'
const source = join(repo, 'packages/mimir')
const store = '/Users/leeliu/Library/pnpm/store/v11'
const backup = join(repo, '..', `web-profile-backup-${new Date().toISOString().replace(/[:.]/g, '-')}`)
await mkdir(backup, { recursive: true })
for (const name of ['package.json', 'pnpm-workspace.yaml', 'pnpm-lock.yaml']) {
  await copyFile(join(profile, name), join(backup, name))
}
const packagePath = join(profile, 'package.json')
const workspacePath = join(profile, 'pnpm-workspace.yaml')
const pkg = JSON.parse(await readFile(packagePath, 'utf8'))
const localDependency = `file:${source}`
if (pkg.dependencies?.['dsh-mimir'] !== '0.20.0' && pkg.dependencies?.['dsh-mimir'] !== localDependency) {
  throw new Error(`unexpected current Mimir dependency; backup is ${backup}`)
}
const workspace = await readFile(workspacePath, 'utf8')
const patched = workspace.replace(/\npatchedDependencies:\n  dsh-mimir@0\.20\.0: patches\/dsh-mimir@0\.20\.0\.patch\n?/, '\n')
if (patched === workspace && pkg.dependencies['dsh-mimir'] === '0.20.0') {
  throw new Error(`profile codec patch declaration not found; backup is ${backup}`)
}
pkg.dependencies['dsh-mimir'] = localDependency
await writeFile(packagePath, `${JSON.stringify(pkg, null, 2)}\n`)
await writeFile(workspacePath, patched)
const result = spawnSync('/Users/leeliu/.local/bin/pnpm', [
  '--pm-on-fail=ignore', 'install', '--store-dir', store, '--prefer-offline',
], { cwd: profile, stdio: 'inherit' })
if (result.status !== 0) {
  for (const name of ['package.json', 'pnpm-workspace.yaml', 'pnpm-lock.yaml']) {
    await copyFile(join(backup, name), join(profile, name))
  }
  throw new Error(`profile install failed and manifests were restored; backup is ${backup}`)
}
process.stdout.write(`Installed local Mimir. Previous manifests: ${backup}\n`)
