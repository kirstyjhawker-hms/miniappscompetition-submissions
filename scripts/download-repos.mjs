#!/usr/bin/env node
// Downloads a snapshot ZIP of every submission's repo at its current default-branch
// HEAD, plus a manifest pinning the exact commit each ZIP came from.
//
//   <out>/<owner>-<repo>.zip   source archive (GitHub zipball of a specific SHA)
//   <out>/manifest.json        what was downloaded, from where, at which commit
//
// Repos are resolved through the GitHub API first, so a renamed or re-cased repo
// still lands under its canonical owner/repo name. Downloads use the resolved SHA
// rather than the branch name, so the manifest describes exactly what is on disk.
//
// Auth comes from the `gh` CLI (`gh auth status` must be logged in) — the 5000
// req/hr authenticated rate limit comfortably covers a full run.
//
// Usage: node scripts/download-repos.mjs [outDir] [--force] [--only <folder>] [--cycle <name>]
import { execFile, spawn } from 'node:child_process'
import { createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { parse } from 'yaml'

const CONCURRENCY = 6

function parseArgs(argv) {
  const opts = { outDir: null, force: false, only: null, cycle: 'cycle1' }
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]
    if (arg === '--force') opts.force = true
    else if (arg === '--only') opts.only = argv[++i]
    else if (arg === '--cycle') opts.cycle = argv[++i]
    else if (arg.startsWith('-')) throw new Error(`unknown flag: ${arg}`)
    else if (opts.outDir === null) opts.outDir = arg
    else throw new Error(`unexpected argument: ${arg}`)
  }
  if (opts.only == null && argv.includes('--only')) throw new Error('--only needs a submission folder name')
  if (opts.cycle == null) throw new Error('--cycle needs a cycle name')
  return opts
}

let opts
try {
  opts = parseArgs(process.argv.slice(2))
} catch (err) {
  console.error(`${err.message}\nusage: download-repos.mjs [outDir] [--force] [--only <folder>] [--cycle <name>]`)
  process.exit(2)
}

const root = process.cwd()
const cycleDir = join(root, opts.cycle)
const outDir = opts.outDir ? join(root, opts.outDir) : join(root, 'downloads', opts.cycle)

if (!existsSync(cycleDir)) {
  console.error(`no such directory: ${opts.cycle} (run from the repo root)`)
  process.exit(2)
}

// --- gh helpers -------------------------------------------------------------

const gh = (args, opts = {}) => new Promise((resolve, reject) => {
  execFile('gh', args, { maxBuffer: 32 * 1024 * 1024, encoding: 'utf8', ...opts }, (err, stdout, stderr) => {
    if (err) {
      const detail = (stderr || err.message || '').trim().split('\n')[0]
      reject(new Error(detail || `gh ${args[0]} failed`))
      return
    }
    resolve(stdout)
  })
})

const childClosed = (child) => new Promise((resolve, reject) => {
  child.on('close', resolve)
  child.on('error', reject)
})

// Streams a binary response straight to disk. Uses spawn, not execFile: execFile
// defaults to encoding 'utf8', which setEncoding()s stdout and silently mangles
// every non-UTF8 byte of the archive.
const ghDownload = async (endpoint, destPath) => {
  const child = spawn('gh', ['api', endpoint])
  let stderr = ''
  child.stderr.on('data', (chunk) => { stderr += chunk })
  // Wait for BOTH the process to exit and the file to finish flushing. The child's
  // 'close' can fire either side of the write stream's, so waiting on one alone
  // races — and can leave this promise unsettled forever.
  let code
  try {
    ;[code] = await Promise.all([childClosed(child), pipeline(child.stdout, createWriteStream(destPath))])
  } catch (err) {
    child.kill()
    throw err
  }
  if (code !== 0) throw new Error(stderr.trim().split('\n')[0] || `gh api exited ${code}`)
}

// --- submission discovery ---------------------------------------------------

// Accepts the shapes seen in the wild: trailing slash, .git suffix, mixed case.
export function parseRepoUrl(url) {
  let u
  try { u = new URL(url) } catch { return null }
  const host = u.hostname.toLowerCase()
  if (host !== 'github.com' && host !== 'www.github.com') return null
  const parts = u.pathname.split('/').filter(Boolean)
  if (parts.length < 2) return null
  const owner = parts[0]
  const repo = parts[1].replace(/\.git$/i, '')
  if (!owner || !repo) return null
  return { owner, repo }
}

function collectSubmissions() {
  const folders = readdirSync(cycleDir, { withFileTypes: true })
    .filter((e) => e.isDirectory())
    .map((e) => e.name)
    .sort((a, b) => a.localeCompare(b))
    .filter((name) => opts.only == null || name === opts.only)

  return folders.map((folder) => {
    const yamlPath = join(cycleDir, folder, 'submission.yaml')
    if (!existsSync(yamlPath)) return { folder, error: 'no submission.yaml' }
    let repoUrl
    try {
      repoUrl = parse(readFileSync(yamlPath, 'utf8'))?.repo_url
    } catch (err) {
      return { folder, error: `unparseable submission.yaml: ${err.message}` }
    }
    if (!repoUrl) return { folder, error: 'submission.yaml has no repo_url' }
    const parsed = parseRepoUrl(repoUrl)
    if (!parsed) return { folder, repoUrl, error: 'repo_url is not a github.com repo' }
    return { folder, repoUrl, ...parsed }
  })
}

// --- per-submission work ----------------------------------------------------

async function download(sub, previous) {
  const base = { folder: sub.folder, repo_url: sub.repoUrl ?? null }

  if (sub.error) return { ...base, status: 'failed', error: sub.error }

  // Canonical casing + default branch. A renamed repo resolves to its new name here.
  let info
  try {
    info = JSON.parse(await gh(['api', `repos/${sub.owner}/${sub.repo}`,
      '--jq', '{owner: .owner.login, name: .name, branch: .default_branch, private: .private}']))
  } catch (err) {
    return { ...base, status: 'failed', error: `repo lookup failed: ${err.message}` }
  }

  const zipName = `${info.owner}-${info.name}.zip`
  const zipPath = join(outDir, zipName)
  const prior = previous.get(sub.folder)

  if (!opts.force && existsSync(zipPath) && prior?.status === 'ok' && prior.zip === zipName) {
    return { ...prior, status: 'skipped' }
  }

  let sha
  try {
    sha = (await gh(['api', `repos/${info.owner}/${info.name}/commits/${info.branch}`, '--jq', '.sha'])).trim()
  } catch (err) {
    return { ...base, status: 'failed', error: `could not resolve ${info.branch} HEAD: ${err.message}` }
  }

  // Download to a temp name so an interrupted run never leaves a truncated .zip
  // that the next run's skip check would accept as complete.
  const tmpPath = `${zipPath}.partial`
  try {
    await ghDownload(`repos/${info.owner}/${info.name}/zipball/${sha}`, tmpPath)
    renameSync(tmpPath, zipPath)
  } catch (err) {
    rmSync(tmpPath, { force: true })
    return { ...base, status: 'failed', error: `download failed: ${err.message}` }
  }

  return {
    ...base,
    status: 'ok',
    owner: info.owner,
    repo: info.name,
    branch: info.branch,
    sha,
    zip: zipName,
    bytes: statSync(zipPath).size,
    downloaded_at: new Date().toISOString(),
  }
}

async function runPool(items, limit, worker) {
  const results = new Array(items.length)
  let next = 0
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (next < items.length) {
      const i = next++
      results[i] = await worker(items[i], i)
    }
  })
  await Promise.all(runners)
  return results
}

// --- main -------------------------------------------------------------------

try {
  await gh(['auth', 'status'])
} catch {
  console.error('gh is not authenticated — run `gh auth login` first.')
  process.exit(2)
}

const submissions = collectSubmissions()
if (submissions.length === 0) {
  console.error(opts.only ? `no submission folder named ${opts.only} in ${opts.cycle}/` : `no submissions in ${opts.cycle}/`)
  process.exit(2)
}

mkdirSync(outDir, { recursive: true })

const manifestPath = join(outDir, 'manifest.json')
const previous = new Map()
if (existsSync(manifestPath)) {
  try {
    for (const entry of JSON.parse(readFileSync(manifestPath, 'utf8')).submissions ?? []) {
      previous.set(entry.folder, entry)
    }
  } catch {
    console.warn('warning: existing manifest.json is unreadable — treating every repo as new')
  }
}

console.log(`${opts.cycle}: ${submissions.length} submission(s) → ${outDir}`)

const entries = await runPool(submissions, CONCURRENCY, async (sub) => {
  const entry = await download(sub, previous)
  const mark = { ok: '✓', skipped: '·', failed: '✗' }[entry.status]
  const note = entry.status === 'ok' ? `${entry.sha.slice(0, 7)}  ${(entry.bytes / 1024 / 1024).toFixed(1)} MB`
    : entry.status === 'skipped' ? 'already downloaded'
    : entry.error
  console.log(`  ${mark} ${sub.folder.padEnd(24)} ${note}`)
  return entry
})

// Carry over entries for submissions this run did not touch (e.g. --only), so the
// manifest always describes the full contents of the output directory.
const touched = new Set(entries.map((e) => e.folder))
const all = [...entries, ...[...previous.values()].filter((e) => !touched.has(e.folder))]
  .sort((a, b) => a.folder.localeCompare(b.folder))

writeFileSync(manifestPath, `${JSON.stringify({
  cycle: opts.cycle,
  generated_at: new Date().toISOString(),
  submissions: all,
}, null, 2)}\n`)

const count = (status) => entries.filter((e) => e.status === status).length
const failed = count('failed')
console.log(`\n${count('ok')} downloaded, ${count('skipped')} skipped, ${failed} failed → ${manifestPath}`)
if (failed > 0) {
  console.log('Re-run to retry failures; use --force to refresh everything.')
  process.exit(1)
}
