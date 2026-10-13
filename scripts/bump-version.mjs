/**
 * Bump the version of every published package in the workspace and commit.
 *
 * Usage: node scripts/bump-version.mjs <version|patch|minor|major> [github-actor]
 *
 * The root package and every `packages/*` package are released in lockstep,
 * so they all receive the same version. `workspace:*` dependencies are
 * rewritten to the concrete version by `pnpm publish`.
 */
import { execFileSync } from 'node:child_process'
import { readdir, readFile, writeFile } from 'node:fs/promises'
import path from 'node:path'

const users = {
  mcollina: ['Matteo Collina', 'hello@matteocollina.com'],
  ShogunPanda: ['Paolo Insogna', 'paolo@cowtech.it'],
}

function getUserInfo () {
  const username = process.argv[3] ?? process.env.GITHUB_ACTOR
  return users[username] ?? users.mcollina
}

function inc (version, release) {
  const match = /^(\d+)\.(\d+)\.(\d+)/.exec(version)
  if (!match) throw new Error(`Cannot increment invalid version: ${version}`)
  let [major, minor, patch] = match.slice(1).map(Number)
  if (release === 'major') {
    major++
    minor = 0
    patch = 0
  } else if (release === 'minor') {
    minor++
    patch = 0
  } else {
    patch++
  }
  return `${major}.${minor}.${patch}`
}

async function readJson (file) {
  return JSON.parse(await readFile(file, 'utf8'))
}

async function getVersion () {
  const input = process.argv[2]
  if (!input) throw new Error('Missing version argument (version, patch, minor or major).')

  if (['major', 'minor', 'patch'].includes(input)) {
    const { version } = await readJson('package.json')
    return inc(version, input)
  }

  const version = input.replace(/^v/, '')
  if (!/^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`Invalid version: ${input}`)
  }
  return version
}

async function getPackageFiles () {
  const entries = await readdir('packages', { withFileTypes: true })
  return [
    'package.json',
    ...entries.filter((e) => e.isDirectory()).map((e) => path.join('packages', e.name, 'package.json')),
  ]
}

const userInfo = getUserInfo()
const version = await getVersion()

for (const file of await getPackageFiles()) {
  const pkg = await readJson(file)
  pkg.version = version
  await writeFile(file, JSON.stringify(pkg, null, 2) + '\n')
}

if (process.env.GITHUB_ACTIONS === 'true') {
  execFileSync('git', ['config', '--global', 'user.name', userInfo[0]])
  execFileSync('git', ['config', '--global', 'user.email', userInfo[1]])
}

execFileSync('git', [
  'commit', '-a',
  '-m', `chore: Bumped v${version}.`,
  '-m', `Signed-off-by: ${userInfo[0]} <${userInfo[1]}>`,
], { stdio: 'inherit' })
