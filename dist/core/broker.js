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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.EventStreaming = exports.Topic = void 0;
const fs = __importStar(require("fs"));
const path_1 = __importDefault(require("path"));
const stream_1 = require("stream");
const retentionEngine_1 = __importDefault(require("../retention/retentionEngine"));
const bufferMemory_1 = __importDefault(require("../storage/bufferMemory"));
const diskStorage_1 = __importDefault(require("../storage/diskStorage"));
const secondaryIndex_1 = __importDefault(require("../storage/secondaryIndex"));
class Topic {
    constructor(name, dataDir) {
        this.name = name;
        this.memory = new bufferMemory_1.default(50000);
        this.index = new secondaryIndex_1.default();
        this.consumers = new Map();
        this.nextOffset = 0;
        this.writeCount = 0;
        this.lastStatTs = Date.now();
        this.writesPerSec = 0;
        this.disk = new diskStorage_1.default(dataDir, name);
        this.replayFromDisk();
    }
    replayFromDisk() {
        let max = -1;
        for (const rec of this.disk.replayAll()) {
            this.index.insert({
                offset: rec.offset,
                timestamp: rec.timestamp,
                key: rec.key,
            });
            if (rec.offset > max)
                max = rec.offset;
        }
        if (max >= 0)
            this.nextOffset = max + 1;
    }
    trackWrite() {
        this.writeCount++;
        const now = Date.now();
        const elapsed = now - this.lastStatTs;
        if (elapsed >= 1000) {
            this.writesPerSec = Math.round((this.writeCount * 1000) / elapsed);
            this.writeCount = 0;
            this.lastStatTs = now;
        }
    }
}
exports.Topic = Topic;
class EventStreaming {
    constructor(dataDir = './msgbus-data', options = {}) {
        var _a;
        this.dataDir = dataDir;
        this.options = options;
        this.topics = new Map();
        this.retention = null;
        this.cleanupTimer = null;
        this.streamListeners = new Map();
        this.saveOffsetsPending = false;
        fs.mkdirSync(dataDir, { recursive: true });
        this.offsetFile = path_1.default.join(dataDir, '_consumer-offsets.json');
        if (options.retention) {
            this.retention = new retentionEngine_1.default(options.retention);
        }
        const intervalMin = (_a = options.cleanupIntervalMinutes) !== null && _a !== void 0 ? _a : 60;
        if (intervalMin > 0 && this.retention) {
            const ms = intervalMin * 60000;
            this.cleanupTimer = setInterval(() => {
                console.log('[bus] auto-cleanup running...');
                this.cleanup();
            }, ms);
            if (this.cleanupTimer.unref)
                this.cleanupTimer.unref();
            console.log(`[bus] auto-cleanup scheduled every ${intervalMin} min`);
        }
    }
    getTopic(name) {
        if (!this.topics.has(name))
            this.topics.set(name, new Topic(name, this.dataDir));
        return this.topics.get(name);
    }
    loadOffsets() {
        try {
            if (fs.existsSync(this.offsetFile))
                return JSON.parse(fs.readFileSync(this.offsetFile, 'utf-8'));
        }
        catch (_a) { }
        return {};
    }
    saveOffsets() {
        const snap = {};
        for (const [name, topic] of this.topics) {
            snap[name] = {};
            for (const [gid, c] of topic.consumers)
                snap[name][gid] = c.offset;
        }
        fs.writeFileSync(this.offsetFile, JSON.stringify(snap), 'utf-8');
    }
    scheduleOffsetSave() {
        if (this.saveOffsetsPending)
            return;
        this.saveOffsetsPending = true;
        setTimeout(() => {
            this.saveOffsetsPending = false;
            this.saveOffsets();
        }, 500).unref();
    }
    produce(opts) {
        const topic = this.getTopic(opts.topic);
        const record = {
            offset: topic.nextOffset++,
            topic: opts.topic,
            key: opts.key,
            timestamp: Date.now(),
            data: opts.data,
        };
        topic.memory.push(record);
        topic.index.insert({
            offset: record.offset,
            timestamp: record.timestamp,
            key: record.key,
        });
        topic.disk.enqueue(record);
        this.deliver(topic, record);
        topic.trackWrite();
        return record.offset;
    }
    produceBatch(records) {
        return records.map((r) => this.produce(r));
    }
    subscribe(opts, handler) {
        var _a, _b;
        const topic = this.getTopic(opts.topic);
        const saved = this.loadOffsets();
        const offset = (_b = (_a = saved[opts.topic]) === null || _a === void 0 ? void 0 : _a[opts.groupId]) !== null && _b !== void 0 ? _b : (opts.fromBeginning ? 0 : topic.nextOffset);
        const consumer = { groupId: opts.groupId, handler, offset };
        topic.consumers.set(opts.groupId, consumer);
        if (consumer.offset < topic.nextOffset) {
            setImmediate(() => this.catchUp(topic, consumer));
        }
        return () => {
            topic.consumers.delete(opts.groupId);
            this.saveOffsets();
        };
    }
    deliver(topic, record) {
        for (const consumer of topic.consumers.values()) {
            if (consumer.offset === record.offset) {
                consumer.offset++;
                try {
                    const p = consumer.handler(record);
                    if (p instanceof Promise)
                        p.then(() => this.scheduleOffsetSave()).catch((e) => console.error(`[bus][${consumer.groupId}]`, e));
                    else
                        this.scheduleOffsetSave();
                }
                catch (e) {
                    console.error(`[bus][${consumer.groupId}]`, e);
                }
            }
        }
        this.notifyStreamListeners(topic.name, record);
    }
    notifyStreamListeners(topicName, record) {
        const topicMap = this.streamListeners.get(topicName);
        if (!topicMap)
            return;
        const keyListeners = topicMap.get(record.key);
        if (keyListeners)
            for (const fn of keyListeners)
                fn(record);
        const wildcardListeners = topicMap.get('*');
        if (wildcardListeners)
            for (const fn of wildcardListeners)
                fn(record);
    }
    addStreamListener(topicName, key, fn) {
        if (!this.streamListeners.has(topicName))
            this.streamListeners.set(topicName, new Map());
        const topicMap = this.streamListeners.get(topicName);
        if (!topicMap.has(key))
            topicMap.set(key, new Set());
        topicMap.get(key).add(fn);
        return () => { var _a; return (_a = topicMap.get(key)) === null || _a === void 0 ? void 0 : _a.delete(fn); };
    }
    catchUp(topic, consumer) {
        const ringMin = topic.memory.minOffset;
        const fromDisk = new Set();
        if (ringMin !== -1 && consumer.offset < ringMin) {
            for (let o = consumer.offset; o < ringMin; o++)
                fromDisk.add(o);
        }
        for (const rec of topic.disk.readByOffsets(fromDisk)) {
            consumer.offset = rec.offset + 1;
            try {
                consumer.handler(rec);
            }
            catch (e) {
                console.error(`[bus][${consumer.groupId}] catchup`, e);
            }
        }
        for (const rec of topic.memory.from(consumer.offset)) {
            consumer.offset = rec.offset + 1;
            try {
                consumer.handler(rec);
            }
            catch (e) {
                console.error(`[bus][${consumer.groupId}] catchup`, e);
            }
        }
        this.saveOffsets();
    }
    // ── Query ─────────────────────────────────────────────────────────────────
    query(opts) {
        var _a, _b, _c;
        const topic = this.getTopic(opts.topic);
        const limit = (_a = opts.limit) !== null && _a !== void 0 ? _a : 100;
        const skip = (_b = opts.skip) !== null && _b !== void 0 ? _b : 0;
        const order = (_c = opts.order) !== null && _c !== void 0 ? _c : 'desc';
        const offsets = topic.index.query({
            key: opts.key,
            fromTime: opts.fromTime,
            toTime: opts.toTime,
            limit: limit + skip,
            skip: 0,
            order,
        });
        const paginated = offsets.slice(skip, skip + limit);
        if (paginated.length === 0)
            return [];
        const set = new Set(paginated);
        const ringMin = topic.memory.minOffset;
        const fromRing = new Set();
        const fromDisk = new Set();
        for (const off of set) {
            if (ringMin !== -1 && off >= ringMin)
                fromRing.add(off);
            else
                fromDisk.add(off);
        }
        const results = [];
        for (const rec of topic.memory) {
            if (fromRing.has(rec.offset))
                results.push(rec);
        }
        for (const rec of topic.disk.readByOffsets(fromDisk)) {
            results.push(rec);
        }
        results.sort((a, b) => order === 'asc' ? a.offset - b.offset : b.offset - a.offset);
        return results;
    }
    queryStream(opts) {
        var _a, _b, _c;
        const historySize = (_a = opts.historySize) !== null && _a !== void 0 ? _a : 1000;
        const order = (_b = opts.order) !== null && _b !== void 0 ? _b : 'asc';
        const listenerKey = (_c = opts.key) !== null && _c !== void 0 ? _c : '*';
        let removeLive = null;
        const stream = new stream_1.Readable({ objectMode: false, read() { } });
        const push = (record) => {
            if (stream.destroyed)
                return;
            if (opts.fromTime && record.timestamp < opts.fromTime)
                return;
            if (opts.toTime && record.timestamp > opts.toTime)
                return;
            stream.push(JSON.stringify(record) + '\n');
        };
        setImmediate(() => {
            if (stream.destroyed)
                return;
            const history = this.query({
                topic: opts.topic,
                key: opts.key,
                limit: historySize,
                skip: 0,
                order,
                fromTime: opts.fromTime,
                toTime: opts.toTime,
            });
            for (const record of history) {
                if (stream.destroyed)
                    return;
                push(record);
            }
            if (!stream.destroyed) {
                removeLive = this.addStreamListener(opts.topic, listenerKey, push);
            }
        });
        stream.on('close', () => removeLive === null || removeLive === void 0 ? void 0 : removeLive());
        return stream;
    }
    // ── Retention / Cleanup ───────────────────────────────────────────────────
    cleanup(topicName, policyOverride) {
        const engine = policyOverride
            ? new retentionEngine_1.default(policyOverride)
            : this.retention;
        if (!engine) {
            console.warn('[bus] cleanup() called but no retention policy is set.');
            return [];
        }
        const targets = topicName
            ? [this.getTopic(topicName)]
            : [...this.topics.values()];
        const reports = [];
        for (const topic of targets) {
            topic.disk.flush();
            const metas = topic.disk.buildSegmentMetas();
            const { toDelete, reasons } = engine.evaluate(metas);
            if (toDelete.length === 0) {
                reports.push({
                    topic: topic.name,
                    segmentsDeleted: 0,
                    recordsDropped: 0,
                    bytesFreed: 0,
                    reasons: ['nothing to delete'],
                    skippedConsumers: [],
                });
                continue;
            }
            const deletedBases = new Set(toDelete.map((s) => s.baseOffset));
            const surviving = metas.filter((m) => !deletedBases.has(m.baseOffset));
            const earliestSurviving = surviving.length > 0
                ? Math.min(...surviving.map((m) => m.baseOffset))
                : topic.nextOffset;
            // Advance any slow consumers past the gap — "delete anyway" behaviour
            const skippedConsumers = [];
            for (const consumer of topic.consumers.values()) {
                if (consumer.offset < earliestSurviving) {
                    console.warn(`[bus][${topic.name}] consumer "${consumer.groupId}" at offset ${consumer.offset} ` +
                        `skipped to ${earliestSurviving} (segment deleted)`);
                    skippedConsumers.push({
                        groupId: consumer.groupId,
                        skippedToOffset: earliestSurviving,
                    });
                    consumer.offset = earliestSurviving;
                }
            }
            // Delete segments and purge memory index
            let bytesFreed = 0;
            let recordsDropped = 0;
            for (const seg of toDelete) {
                bytesFreed += topic.disk.deleteSegment(seg.baseOffset);
                recordsDropped += seg.recordCount;
            }
            topic.index.purgeBelow(earliestSurviving);
            this.saveOffsets();
            const report = {
                topic: topic.name,
                segmentsDeleted: toDelete.length,
                recordsDropped,
                bytesFreed,
                reasons,
                skippedConsumers,
            };
            console.log(`[bus] cleanup [${topic.name}]: ${toDelete.length} segments deleted, ` +
                `${(bytesFreed / 1e6).toFixed(1)} MB freed, ${recordsDropped} records dropped`);
            reports.push(report);
        }
        return reports;
    }
    // ── Stats ─────────────────────────────────────────────────────────────────
    stats(topicName) {
        const t = this.getTopic(topicName);
        return {
            topic: topicName,
            totalRecords: t.index.totalEntries,
            latestOffset: t.nextOffset - 1,
            consumers: [...t.consumers.keys()],
            indexedKeys: t.index.totalKeys,
            memoryRecords: t.memory.size,
            writesPerSec: t.writesPerSec,
            diskSegments: t.disk.segmentCount,
            diskSizeBytes: t.disk.totalDiskBytes,
        };
    }
    close() {
        if (this.cleanupTimer)
            clearInterval(this.cleanupTimer);
        this.saveOffsets();
        for (const topic of this.topics.values())
            topic.disk.close();
    }
}
exports.EventStreaming = EventStreaming;
