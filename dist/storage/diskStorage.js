"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
const fs = __importStar(require("fs"));
const path = __importStar(require("path"));
const MAX_SEGMENT_BYTES = 256 * 1024 * 1024;
const FLUSH_INTERVAL_MS = 50;
const FLUSH_BATCH_SIZE = 500;
class DiskStore {
    constructor(dir, topic) {
        this.dir = dir;
        this.topic = topic;
        this.currentFd = -1;
        this.currentSegmentSize = 0;
        this.currentSegmentBase = 0;
        this.totalFlushed = 0;
        this.totalDropped = 0;
        this.diskQueue = [];
        this.MAX_QUEUE = 100000;
        this.isWorkerRunning = false;
        fs.mkdirSync(this.segDir, { recursive: true });
        this.openOrCreateActiveSegment();
        this.startWorker();
    }
    get segDir() {
        return path.join(this.dir, this.topic);
    }
    listPaths() {
        return fs
            .readdirSync(this.segDir)
            .filter((f) => f.endsWith('.log'))
            .sort()
            .map((f) => path.join(this.segDir, f));
    }
    openSegment(base) {
        if (this.currentFd !== -1)
            fs.closeSync(this.currentFd);
        const file = path.join(this.segDir, String(base).padStart(10, '0') + '.log');
        this.currentFd = fs.openSync(file, 'a');
        this.currentSegmentBase = base;
        this.currentSegmentSize = 0;
    }
    openOrCreateActiveSegment() {
        const files = this.listPaths();
        if (files.length === 0) {
            this.openSegment(0);
            return;
        }
        const last = files[files.length - 1];
        const base = parseInt(path.basename(last, '.log'), 10);
        const size = fs.statSync(last).size;
        if (size >= MAX_SEGMENT_BYTES) {
            this.openSegment(base + 1);
        }
        else {
            this.currentSegmentBase = base;
            this.currentSegmentSize = size;
            this.currentFd = fs.openSync(last, 'a');
        }
    }
    enqueue(record) {
        if (this.diskQueue.length >= this.MAX_QUEUE) {
            this.diskQueue.shift();
            this.totalDropped++;
            if (this.totalDropped % 1000 === 0) {
                console.warn(`[node-event-streaming][${this.topic}] ` +
                    `${this.totalDropped} records dropped — disk can't keep up`);
            }
        }
        this.diskQueue.push(JSON.stringify(record) + '\n');
    }
    startWorker() {
        if (this.isWorkerRunning)
            return;
        this.isWorkerRunning = true;
        const tick = () => {
            if (this.diskQueue.length === 0) {
                setTimeout(tick, FLUSH_INTERVAL_MS).unref();
                return;
            }
            const batch = this.diskQueue.splice(0, FLUSH_BATCH_SIZE);
            const payload = batch.join('');
            const byteLength = Buffer.byteLength(payload, 'utf-8');
            if (this.currentSegmentSize + byteLength >= MAX_SEGMENT_BYTES)
                this.openSegment(this.currentSegmentBase + 1);
            fs.writeSync(this.currentFd, payload, null, 'utf-8');
            this.currentSegmentSize += byteLength;
            this.totalFlushed += batch.length;
            if (this.diskQueue.length > 0)
                setImmediate(tick);
            else
                setTimeout(tick, FLUSH_INTERVAL_MS).unref();
        };
        setImmediate(tick);
    }
    flush() {
        while (this.diskQueue.length > 0) {
            const batch = this.diskQueue.splice(0, FLUSH_BATCH_SIZE);
            const payload = batch.join('');
            const byteLength = Buffer.byteLength(payload, 'utf-8');
            if (this.currentSegmentSize + byteLength >= MAX_SEGMENT_BYTES)
                this.openSegment(this.currentSegmentBase + 1);
            fs.writeSync(this.currentFd, payload, null, 'utf-8');
            this.currentSegmentSize += byteLength;
            this.totalFlushed += batch.length;
        }
    }
    *readByOffsets(offsets, getSegment) {
        var _a;
        if (offsets.size === 0)
            return;
        const bySegment = new Map();
        for (const off of offsets) {
            const seg = (_a = getSegment === null || getSegment === void 0 ? void 0 : getSegment(off)) !== null && _a !== void 0 ? _a : 0;
            if (!bySegment.has(seg))
                bySegment.set(seg, new Set());
            bySegment.get(seg).add(off);
        }
        for (const [segBase, segOffsets] of bySegment) {
            const file = path.join(this.segDir, String(segBase).padStart(10, '0') + '.log');
            if (!fs.existsSync(file))
                continue;
            const content = fs.readFileSync(file, 'utf-8');
            for (const line of content.split('\n')) {
                if (!line.trim())
                    continue;
                try {
                    const rec = JSON.parse(line);
                    if (segOffsets.has(rec.offset)) {
                        yield rec;
                        segOffsets.delete(rec.offset);
                        if (segOffsets.size === 0)
                            break;
                    }
                }
                catch (_b) {
                    /* skip */
                }
            }
        }
    }
    *replayAll() {
        for (const file of this.listPaths()) {
            const content = fs.readFileSync(file, 'utf-8');
            for (const line of content.split('\n')) {
                if (!line.trim())
                    continue;
                try {
                    yield JSON.parse(line);
                }
                catch (_a) {
                    /* skip */
                }
            }
        }
    }
    buildSegmentMetas() {
        const files = this.listPaths();
        return files.map((file) => {
            var _a, _b, _c;
            const base = parseInt(path.basename(file, '.log'), 10);
            const stat = fs.statSync(file);
            let first = null;
            let last = null;
            let count = 0;
            const content = fs.readFileSync(file, 'utf-8');
            for (const line of content.split('\n')) {
                if (!line.trim())
                    continue;
                try {
                    const rec = JSON.parse(line);
                    if (!first)
                        first = rec;
                    last = rec;
                    count++;
                }
                catch (_d) {
                    /* skip */
                }
            }
            return {
                filePath: file,
                baseOffset: base,
                lastOffset: (_a = last === null || last === void 0 ? void 0 : last.offset) !== null && _a !== void 0 ? _a : -1,
                sizeBytes: stat.size,
                oldestTimestamp: (_b = first === null || first === void 0 ? void 0 : first.timestamp) !== null && _b !== void 0 ? _b : 0,
                newestTimestamp: (_c = last === null || last === void 0 ? void 0 : last.timestamp) !== null && _c !== void 0 ? _c : 0,
                recordCount: count,
                isActive: base === this.currentSegmentBase,
            };
        });
    }
    deleteSegment(baseOffset) {
        const file = path.join(this.segDir, String(baseOffset).padStart(10, '0') + '.log');
        if (baseOffset === this.currentSegmentBase) {
            console.warn(`[stream] skipping active segment ${baseOffset}`);
            return 0;
        }
        try {
            const size = fs.statSync(file).size;
            fs.unlinkSync(file);
            return size;
        }
        catch (e) {
            console.error(`[stream] failed to delete segment ${file}:`, e);
            return 0;
        }
    }
    get totalDiskBytes() {
        return this.listPaths().reduce((s, f) => {
            try {
                return s + fs.statSync(f).size;
            }
            catch (_a) {
                return s;
            }
        }, 0);
    }
    get segmentCount() {
        return this.listPaths().length;
    }
    close() {
        this.flush();
        if (this.currentFd !== -1)
            fs.closeSync(this.currentFd);
    }
}
exports.default = DiskStore;
