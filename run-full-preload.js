#!/usr/bin/env node
'use strict';

// Cross-platform replacement for run-full-preload.bat
// Usage: node run-full-preload.js dev|uat|live [smartlife] [opensearch]

const { spawnSync, spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const isWin = process.platform === 'win32';
const isMac = process.platform === 'darwin';
const home = os.homedir();

// Configuration comes from a .env file next to this script (KEY=value lines, # comments, optional quotes)
function loadDotEnv(file) {
  const result = {};
  if (!fs.existsSync(file)) return result;
  for (const line of fs.readFileSync(file, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^\s*(?:export\s+)?([\w.-]+)\s*=\s*(.*?)\s*$/);
    if (!m) continue;
    let value = m[2];
    if (/^(["']).*\1$/.test(value)) value = value.slice(1, -1);
    else value = value.replace(/\s+#.*$/, '');
    result[m[1]] = value;
  }
  return result;
}

const env = loadDotEnv(path.join(__dirname, '.env'));

// ========== CHECK/SET THESE CONFIGURATION ENTRIES ==========
// Every entry can be overridden by a same-named entry in the .env file (which is git-ignored).
// Secrets (TRUD_API_KEY, Q_AUTH, OPENSEARCH_AUTH) are NOT stored in this file: put them in .env.
const JAVA_VERSION = env.JAVA_VERSION || 'corretto-21.0.8';
const IMPORT_DATA = env.IMPORT_DATA || path.resolve(__dirname, '..', 'ImportData');
const TRUD_API_KEY = env.TRUD_API_KEY;
const Q_AUTH = env.Q_AUTH;
const OPENSEARCH_AUTH = env.OPENSEARCH_AUTH;
const OPENSEARCH_URL = env.OPENSEARCH_URL || 'https://search.endeavourhealth.net/';

// ========== SET REMAINING CONFIG BASED ON THE ABOVE ==========
const JAVA_HOME = env.JAVA_HOME ||path.join(home, '.jdks', JAVA_VERSION);
const TRUD_DATA_DIR = env.TRUD_DATA_DIR || path.join(IMPORT_DATA, 'TRUD');
const PRELOAD_TEMP = env.PRELOAD_TEMP || path.join(IMPORT_DATA, '.tmp');

// GraphDB Desktop locations differ per OS; override with env vars if yours differ.
const defaults = isWin
  ? {
      bin: path.join(home, 'AppData', 'Local', 'GraphDB Desktop', 'app', 'bin'),
      data: path.join(home, 'AppData', 'Roaming', 'GraphDB', 'data', 'repositories'),
      start: path.join(home, 'AppData', 'Local', 'GraphDB Desktop', 'GraphDB Desktop.exe'),
      process: 'GraphDB Desktop.exe'
    }
  : isMac
    ? {
        bin: '/Applications/GraphDB Desktop.app/Contents/app/bin',
        data: path.join(home, 'Library', 'Application Support', 'GraphDB', 'data', 'repositories'),
        start: '/Applications/GraphDB Desktop.app/Contents/MacOS/GraphDB Desktop',
        process: 'GraphDB Desktop'
      }
    : {
        bin: path.join(home, 'graphdb-desktop', 'app', 'bin'),
        data: path.join(home, '.graphdb', 'data', 'repositories'),
        start: path.join(home, 'graphdb-desktop', 'bin', 'graphdb-desktop'),
        process: 'graphdb-desktop'
      };

const GRAPHDB_BIN = env.GRAPHDB_BIN || defaults.bin;
const GRAPHDB_DATA = env.GRAPHDB_DATA || defaults.data;
const GRAPHDB_START_CMD = env.GRAPHDB_START_CMD || defaults.start;
const GRAPHDB_PROCESS = env.GRAPHDB_PROCESS || defaults.process;
const GRAPH_REPO = env.GRAPH_REPO || 'im';
const GRAPH_SERVER = env.GRAPH_SERVER || 'http://localhost:7200/';
const Q_URL = env.Q_URL || 'https://api.apiqcodes.org/production';

const targets = {
  dev: { branch: 'develop', index: 'conceptdev' },
  uat: { branch: 'uat', index: 'conceptuat' },
  live: { branch: 'live', index: 'concept' }
};

// ========== COMMAND LINE ARGUMENTS ==========
function usage() {
  console.error('Incorrect arguments!');
  console.error(`Usage: node ${path.basename(__filename)} dev|uat|live <smartlife> <opensearch>`);
  process.exit(1);
}

const [target, arg1, arg2, ...extra] = process.argv.slice(2);
if (extra.length > 0 || !Object.hasOwn(targets, target)) usage();

const smartlife = arg1 === 'smartlife' || arg2 === 'smartlife';
const opensearch = arg1 === 'opensearch' || arg2 === 'opensearch';
const { branch } = targets[target];

// Environment passed to every child process (the .bat's SET entries were inherited the same way)
const childEnv = {
  ...process.env,
  JAVA_HOME,
  TRUD_API_KEY,
  Q_AUTH,
  OPENSEARCH_AUTH,
  OPENSEARCH_INDEX: targets[target].index,
  OPENSEARCH_URL,
  TRUD_DATA_DIR,
  PRELOAD_TEMP,
  GRAPHDB_BIN,
  GRAPHDB_DATA,
  GRAPHDB_START_CMD,
  GRAPH_REPO,
  GRAPH_SERVER,
  Q_URL
};

// ========== HELPERS ==========
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function run(cmd, args, opts = {}) {
  const res = spawnSync(cmd, args, { stdio: 'inherit', env: childEnv, ...opts });
  if (res.error) {
    console.error(`Failed to run ${cmd}: ${res.error.message}`);
    process.exit(1);
  }
  if (res.status !== 0) process.exit(res.status ?? 1);
}

const git = (...args) => run('git', args);

const java = path.join(JAVA_HOME, 'bin', isWin ? 'java.exe' : 'java');
const javaRun = (...args) => run(java, args);

function gradle(dir, task) {
  if (isWin) run(`"${path.join(dir, 'gradlew.bat')}"`, [task], { cwd: dir, shell: true });
  else run(path.join(dir, 'gradlew'), [task], { cwd: dir });
}

function fetchRepo(label, dir, url, cloneBranch) {
  console.log(`Fetching ${label}`);
  const checkout = cloneBranch || 'main';
  if (fs.existsSync(dir)) {
    git('-C', dir, 'checkout', checkout);
    git('-C', dir, 'pull', 'origin', checkout);
  } else {
    git('clone', url, dir, ...(cloneBranch ? ['-b', cloneBranch, '--single-branch'] : []));
  }
}

function killGraphDb() {
  console.log('Shutting down graphdb');
  if (isWin) spawnSync('taskkill', ['/f', '/im', GRAPHDB_PROCESS], { stdio: 'inherit' });
  else spawnSync('pkill', ['-f', GRAPHDB_PROCESS], { stdio: 'inherit' });
}

function zipRepository(zipName) {
  // bsdtar (Windows, macOS) can write zip with -a; elsewhere fall back to zip
  if (isWin || isMac) run('tar', ['-a', '-v', '-cf', zipName, GRAPH_REPO], { cwd: GRAPHDB_DATA });
  else run('zip', ['-r', zipName, GRAPH_REPO], { cwd: GRAPHDB_DATA });
}

// Writes a small script that opens GraphDB in its own terminal window and returns immediately, so its
// output is visible and is not piped into (and potentially blocking on) the Java process that starts it.
// Set GRAPHDB_NEW_WINDOW=false in .env to launch GraphDB directly instead.
function createGraphDbLauncher() {
  if (env.GRAPHDB_NEW_WINDOW === 'false') return GRAPHDB_START_CMD;
  const q = GRAPHDB_START_CMD;
  const file = path.join(os.tmpdir(), isWin ? 'im-graphdb-launcher.cmd' : 'im-graphdb-launcher.sh');
  let body;
  if (isWin) {
    body = `@echo off\r\nstart "GraphDB" cmd /k ""${q}""\r\n`;
  } else if (isMac) {
    body = `#!/bin/sh\nosascript -e 'tell application "Terminal" to do script "\\"${q}\\""'\n`;
  } else {
    body =
      '#!/bin/sh\n' +
      `if command -v gnome-terminal >/dev/null; then gnome-terminal -- "${q}" &\n` +
      `elif command -v xterm >/dev/null; then xterm -e "${q}" &\n` +
      `else nohup "${q}" >"${path.join(os.tmpdir(), 'graphdb.log')}" 2>&1 &\n` +
      'fi\n';
  }
  fs.writeFileSync(file, body, { mode: 0o755 });
  return file;
}

const GRAPHDB_LAUNCHER = createGraphDbLauncher();

function startGraphDb() {
  const child = spawn(GRAPHDB_LAUNCHER, [], { detached: true, stdio: 'ignore', env: childEnv, shell: isWin });
  child.on('error', err => {
    console.error(`Failed to start GraphDB: ${err.message}`);
    process.exit(1);
  });
  child.unref();
}

async function waitForGraphDb() {
  console.log('Waiting for startup');
  for (;;) {
    console.log('Pinging...');
    try {
      await fetch('http://127.0.0.1:7200', { signal: AbortSignal.timeout(1000) });
      console.log('Connected!');
      return;
    } catch {
      await sleep(1000);
    }
  }
}

async function restartGraphDb(reason) {
  console.log(`Restarting graphdb for ${reason}`);
  startGraphDb();
  await waitForGraphDb();
}

// ========== MAIN ==========
async function main() {
  if (!TRUD_API_KEY) {
    console.error('TRUD_API_KEY must be set in .env');
    process.exit(1);
  }

  const imapi = path.resolve(__dirname, '..', 'IMAPI');
  const importData = path.resolve(__dirname, '..', 'ImportData');
  const infoManager = path.resolve(__dirname, '..', 'InformationManager');

  console.log(`Proceeding with preload for ${target}`);
  if (smartlife) console.log('(include Smartlife)');
  if (opensearch) console.log('(include OpenSearch)');

  fetchRepo('ImportData', importData, 'https://github.com/endeavourhealth-discovery/ImportData');

  fetchRepo(`${target} IMAPI`, imapi, 'https://github.com/endeavourhealth-discovery/IMAPI', branch);
  gradle(imapi, 'assemble');
  gradle(imapi, 'publishToMavenLocal');

  // NOTE: as in the original .bat, an existing InformationManager checkout refreshes IMAPI again
  console.log(`Fetching ${target} InformationManager`);
  if (fs.existsSync(infoManager)) {
    git('-C', imapi, 'checkout', branch);
    git('-C', imapi, 'pull', 'origin', branch);
  } else {
    git('clone', 'https://github.com/endeavourhealth-discovery/InformationManager', infoManager);
  }
  gradle(infoManager, 'assemble');

  console.log('Performing TRUD update');
  javaRun('-jar', 'Feeds/build/libs/Feeds-1.0-SNAPSHOT.jar', TRUD_API_KEY, TRUD_DATA_DIR);

  console.log('Performing Preload');
  javaRun(
    '-Xmx14g',
    '-cp',
    'Transforms/build/libs/Transforms-1.0-SNAPSHOT-all.jar',
    'org.endeavourhealth.informationmanager.transforms.preload.Preload',
    `source=${IMPORT_DATA}`,
    `preload=${GRAPHDB_BIN}`,
    `temp=${PRELOAD_TEMP}`,
    'privacy=0',
    `cmd=${GRAPHDB_LAUNCHER}`
  );

  await sleep(5000);
  killGraphDb();

  console.log('Zipping IM (Vanilla) repository');
  zipRepository('im.zip');

  if (smartlife) {
    await restartGraphDb('SmartLife load');

    console.log('Filing Smartlife');
    javaRun(
      '-cp',
      'Transforms/build/libs/Transforms-1.0-SNAPSHOT-all.jar',
      'org.endeavourhealth.informationmanager.transforms.online.ImportApp',
      IMPORT_DATA,
      'smartlifequery',
      'skiplucene',
      'privacy=3'
    );

    await sleep(5000);
    killGraphDb();

    console.log('Zipping IM (SmartLife) repository');
    zipRepository('im-smartlife.zip');
  }

  console.log('Checking');

  if (opensearch) {
    await restartGraphDb('OpenSearch load');

    console.log('Running OpenSearch');
    javaRun(
      '-cp',
      'Utils/build/libs/Utils-1.0-SNAPSHOT-all.jar',
      'org.endeavourhealth.informationmanager.utils.opensearch.Main'
    );
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
