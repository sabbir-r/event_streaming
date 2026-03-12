"use strict";
// ─────────────────────────────────────────────────────────────────────────────
// node-event-streaming — Benchmark
// tests/benchmark.ts
//
// Run: npx ts-node tests/benchmark.ts
// ─────────────────────────────────────────────────────────────────────────────
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const server_1 = require("../src/server");
const os = __importStar(require("os"));
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
// ── Config ────────────────────────────────────────────────────────────────────
const DATA_DIR = './benchmark-data';
const streamer = new server_1.EventStreaming(DATA_DIR);
// ── Helpers ───────────────────────────────────────────────────────────────────
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const formatNum = (n) => n.toLocaleString();
const memMB = () => {
    const m = process.memoryUsage();
    return {
        heapUsed: +(m.heapUsed / 1024 / 1024).toFixed(1),
        heapTotal: +(m.heapTotal / 1024 / 1024).toFixed(1),
        rss: +(m.rss / 1024 / 1024).toFixed(1),
    };
};
const printResult = (label, count, ms, extra) => {
    const perSec = Math.round((count / ms) * 1000);
    console.log(`\n  📊  ${label}`);
    console.log(`       records    : ${formatNum(count)}`);
    console.log(`       time       : ${ms} ms`);
    console.log(`       throughput : ${formatNum(perSec)} msg/sec`);
    const mem = memMB();
    console.log(`       heap used  : ${mem.heapUsed} MB  (rss: ${mem.rss} MB)`);
    if (extra) {
        for (const [k, v] of Object.entries(extra)) {
            console.log(`       ${k.padEnd(10)} : ${v}`);
        }
    }
};
const section = (title) => {
    console.log('\n' + '─'.repeat(52));
    console.log(`  ${title}`);
    console.log('─'.repeat(52));
};
const cleanDataDir = () => {
    if (fs.existsSync(DATA_DIR))
        fs.rmSync(DATA_DIR, { recursive: true, force: true });
};
// ── Test 1: produce() — ring buffer write speed ───────────────────────────────
const test1 = () => {
    section('TEST 1 — produce()  ×100k  (ring buffer)');
    const COUNT = 100000;
    const start = Date.now();
    for (let i = 0; i < COUNT; i++) {
        streamer.produce({
            topic: 'bench-produce',
            key: String(i % 1000),
            data: { id: i, lat: '23.8041', lng: '90.4152', ts: Date.now() },
        });
    }
    printResult('produce()', COUNT, Date.now() - start);
};
// ── Test 2: produceBatch() — batched write speed ──────────────────────────────
const test2 = () => {
    section('TEST 2 — produceBatch()  ×100k  (500 per batch)');
    const COUNT = 100000;
    const BATCH = 500;
    const start = Date.now();
    for (let i = 0; i < COUNT; i += BATCH) {
        streamer.produceBatch(Array.from({ length: BATCH }, (_, j) => ({
            topic: 'bench-batch',
            key: String((i + j) % 1000),
            data: { id: i + j, lat: '23.8041', lng: '90.4152', ts: Date.now() },
        })));
    }
    printResult('produceBatch()', COUNT, Date.now() - start);
};
// ── Test 3: subscribe() — consumer throughput ─────────────────────────────────
const test3 = () => __awaiter(void 0, void 0, void 0, function* () {
    section('TEST 3 — subscribe()  eachMessage  ×50k');
    const COUNT = 50000;
    let received = 0;
    let startTime = 0;
    const unsubscribe = streamer.subscribe({ topic: 'bench-consumer', groupId: 'bench', fromBeginning: true }, () => __awaiter(void 0, void 0, void 0, function* () {
        if (received === 0)
            startTime = Date.now();
        received++;
    }));
    for (let i = 0; i < COUNT; i++) {
        streamer.produce({
            topic: 'bench-consumer',
            key: String(i % 1000),
            data: { id: i },
        });
    }
    while (received < COUNT)
        yield sleep(10);
    printResult('subscribe() handler', COUNT, Date.now() - startTime);
    unsubscribe();
});
// ── Test 4: query() — index lookup speed ─────────────────────────────────────
const test4 = () => {
    section('TEST 4 — query()  ×10k  (ring buffer, key lookup)');
    const QUERIES = 10000;
    const start = Date.now();
    for (let i = 0; i < QUERIES; i++) {
        streamer.query({
            topic: 'bench-produce',
            key: String(i % 1000),
            limit: 10,
            order: 'desc',
        });
    }
    printResult('query()', QUERIES, Date.now() - start);
};
// ── Test 5: queryStream() — stream throughput ─────────────────────────────────
const test5 = () => __awaiter(void 0, void 0, void 0, function* () {
    section('TEST 5 — queryStream()  ×10k  history');
    const COUNT = 10000;
    let received = 0;
    for (let i = 0; i < COUNT; i++) {
        streamer.produce({
            topic: 'bench-stream',
            key: '1',
            data: { id: i },
        });
    }
    const start = Date.now();
    const stream = streamer.queryStream({
        topic: 'bench-stream',
        key: '1',
        historySize: COUNT,
    });
    yield new Promise((resolve) => {
        stream.on('data', () => {
            received++;
            if (received >= COUNT) {
                stream.destroy();
                resolve();
            }
        });
    });
    printResult('queryStream()', COUNT, Date.now() - start);
});
// ── Test 6: sustained load — memory leak detection ───────────────────────────
const test6 = () => __awaiter(void 0, void 0, void 0, function* () {
    section('TEST 6 — Sustained load  10 sec  (memory leak check)');
    const DURATION_MS = 10000;
    const end = Date.now() + DURATION_MS;
    let total = 0;
    const memStart = process.memoryUsage().heapUsed;
    const snapshots = [];
    console.log('  running...');
    while (Date.now() < end) {
        streamer.produceBatch(Array.from({ length: 500 }, (_, i) => ({
            topic: 'bench-sustained',
            key: String(i % 1000),
            data: { id: i, lat: '23.8041', lng: '90.4152', ts: Date.now() },
        })));
        total += 500;
        snapshots.push(process.memoryUsage().heapUsed);
        yield sleep(1);
    }
    const memEnd = process.memoryUsage().heapUsed;
    const memDelta = +((memEnd - memStart) / 1024 / 1024).toFixed(1);
    const perSec = Math.round((total / DURATION_MS) * 1000);
    const leak = memDelta > 100;
    console.log(`\n  📊  Sustained load`);
    console.log(`       total      : ${formatNum(total)} records`);
    console.log(`       throughput : ${formatNum(perSec)} msg/sec`);
    console.log(`       mem start  : ${(memStart / 1024 / 1024).toFixed(1)} MB`);
    console.log(`       mem end    : ${(memEnd / 1024 / 1024).toFixed(1)} MB`);
    console.log(`       mem delta  : ${memDelta} MB  ${leak ? '⚠️  possible memory leak' : '✅ stable'}`);
    if (leak) {
        console.log('\n  ⚠️  Memory grew >100MB during sustained load.');
        console.log('      writeBuffer may not be draining fast enough.');
        console.log('      Consider adding backpressure or the offload queue.');
    }
});
// ── Test 7: disk flush — wait for disk and measure ───────────────────────────
const test7 = () => __awaiter(void 0, void 0, void 0, function* () {
    section('TEST 7 — Disk flush  (wait 500ms after 10k records)');
    const COUNT = 10000;
    const start = Date.now();
    for (let i = 0; i < COUNT; i++) {
        streamer.produce({
            topic: 'bench-disk',
            key: String(i % 100),
            data: { id: i, lat: '23.8041', lng: '90.4152' },
        });
    }
    // wait for flush
    yield sleep(500);
    const diskPath = path.join(DATA_DIR, 'bench-disk');
    let diskSize = 0;
    if (fs.existsSync(diskPath)) {
        diskSize = fs
            .readdirSync(diskPath)
            .filter((f) => f.endsWith('.log'))
            .reduce((s, f) => s + fs.statSync(path.join(diskPath, f)).size, 0);
    }
    printResult('produce() → disk flush', COUNT, Date.now() - start, {
        'disk size': `${(diskSize / 1024).toFixed(1)} KB`,
        'per record': `${diskSize > 0 ? (diskSize / COUNT).toFixed(0) : '?'} bytes`,
    });
});
// ── Summary ───────────────────────────────────────────────────────────────────
const printSummary = (results) => {
    console.log('\n' + '═'.repeat(52));
    console.log('  SUMMARY');
    console.log('═'.repeat(52));
    for (const r of results) {
        const bar = '█'.repeat(Math.min(30, Math.round(r.perSec / 10000)));
        console.log(`  ${r.label.padEnd(20)} ${formatNum(r.perSec).padStart(12)} msg/sec  ${bar}`);
    }
    console.log('═'.repeat(52));
};
// ── Run ───────────────────────────────────────────────────────────────────────
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    console.log('╔' + '═'.repeat(50) + '╗');
    console.log('║   node-event-streaming  —  Benchmark         ║');
    console.log('╚' + '═'.repeat(50) + '╝');
    console.log(`\n  Node.js  : ${process.version}`);
    console.log(`  OS       : ${os.platform()} ${os.arch()}`);
    console.log(`  CPU      : ${os.cpus()[0].model}`);
    console.log(`  CPUs     : ${os.cpus().length} cores`);
    console.log(`  RAM      : ${(os.totalmem() / 1024 / 1024 / 1024).toFixed(1)} GB`);
    cleanDataDir();
    test1();
    test2();
    yield test3();
    test4();
    yield test5();
    yield test6();
    yield test7();
    console.log('\n  ✅  All tests complete');
    streamer.close();
    cleanDataDir();
    process.exit(0);
});
run().catch((err) => {
    console.error('benchmark failed:', err);
    streamer.close();
    cleanDataDir();
    process.exit(1);
});
