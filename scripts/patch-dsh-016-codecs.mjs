/** Adapt generated Mimir strict codecs to the DSH 0.1.6 create() contract. */
import { readFile, writeFile } from 'node:fs/promises'
import { join } from 'node:path'

const root = new URL('../packages/mimir/lib/', import.meta.url)
const files = ['typert.host.js', 'typert.remote-client.js', 'client.js']
for (const file of files) {
  const path = join(root.pathname, file)
  const source = await readFile(path, 'utf8')
  let count = 0
  const patched = source.replace(/\bschema:\s*(dsh_mimir_[A-Za-z0-9_$]+\$schema)\b/g, (_match, name) => {
    count += 1
    return `create: () => ${name}`
  })
  const factories = [...patched.matchAll(/\bcreate:\s*\(\) => dsh_mimir_[A-Za-z0-9_$]+\$schema\b/g)].length
  if (factories < 100 || (count !== 0 && factories !== count)) {
    throw new Error(`${file}: expected strict generated codec factories, found ${String(factories)} after replacing ${String(count)}`)
  }
  if (count !== 0) await writeFile(path, patched)
  process.stdout.write(`${file}: ${String(factories)} factories (${String(count)} patched)\n`)
}
