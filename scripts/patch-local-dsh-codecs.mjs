/** Keep generated Typert codecs compatible with the installed DSH 0.1.6. */
import { readFile, writeFile } from 'node:fs/promises'

const directory = new URL('../packages/mimir/lib/', import.meta.url)
const files = ['client.js', 'typert.host.js', 'typert.remote-client.js']
const schemaField = /^(\s*)schema: ([A-Za-z_$][\w$]*\$schema\d*)(,?)$/gm
const factoryField = /^\s*create: \(\) => [A-Za-z_$][\w$]*\$schema\d*,?$/gm

for (const file of files) {
  const path = new URL(file, directory)
  const source = await readFile(path, 'utf8')
  const patched = source.replace(schemaField, (_, indent, schema, comma) =>
    `${indent}create: () => ${schema}${comma}`)
  const factories = [...patched.matchAll(factoryField)].length
  if (factories < 100 || schemaField.test(patched)) {
    throw new Error(`${file}: generated codec shape changed; refusing an incomplete compatibility patch`)
  }
  if (patched !== source) await writeFile(path, patched)
  process.stdout.write(`${file}: ${factories} codec factories ready\n`)
}
