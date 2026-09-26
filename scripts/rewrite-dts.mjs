/**
 * Rewrite relative import specifiers in generated `.d.ts` files.
 *
 * The source uses `.ts` extensions (required by Node ESM + type stripping).
 * TypeScript preserves those specifiers verbatim in emitted declarations, so a
 * consumer resolving `dist/types/index.d.ts` would look for `./errors.ts`
 * inside `dist/types` and not find it. This script rewrites relative specifiers
 * to `.js` so they resolve to the sibling `.d.ts` files (`./errors.js` ->
 * `errors.d.ts`), which is what TypeScript consumers need without any special
 * configuration.
 *
 * Only relative/absolute specifiers are touched; bare package imports and
 * `node:` specifiers are left alone.
 */
import { readdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'

// Default to `<cwd>/dist/types`; callers may pass an explicit target dir.
const root = path.resolve(process.cwd(), process.argv[2] ?? 'dist/types')

/** Rewrite `from './x.ts'` / `export * from './x.ts'` / `import('./x.ts')`. */
function rewrite (content) {
  // import ... from './x.ts'  and  export * from './x.ts'
  content = content.replace(/from\s+(['"])((?:\/|\.\.?\/)[^'"]*?)(\.(?:m|c)?ts)\1/g, (_m, q, spec, ext) => `from ${q}${spec}${ext === '.mts' ? '.mjs' : ext === '.cts' ? '.cjs' : '.js'}${q}`)
  // dynamic import('./x.ts')
  content = content.replace(/import\s*\(\s*(['"])((?:\/|\.\.?\/)[^'"]*?)(\.(?:m|c)?ts)\1\s*\)/g, (_m, q, spec, ext) => `import(${q}${spec}${ext === '.mts' ? '.mjs' : ext === '.cts' ? '.cjs' : '.js'}${q})`)
  return content
}

async function walk (dir) {
  const entries = await readdir(dir, { withFileTypes: true })
  for (const entry of entries) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) {
      await walk(full)
    } else if (entry.isFile() && entry.name.endsWith('.d.ts')) {
      const content = await readFile(full, 'utf8')
      const next = rewrite(content)
      if (next !== content) await writeFile(full, next, 'utf8')
    }
  }
}

await walk(root)
console.log(`rewrote declaration specifiers under ${root}`)
