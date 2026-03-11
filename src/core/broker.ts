import * as fs from 'fs';
import path from 'path';
import { Readable } from 'stream';
import {
  StreamOptions,
  CleanupReport,
  ConsumerOptions,
  LogRecord,
  MessageHandler,
  ProduceOptions,
  QueryOptions,
  QueryStreamOptions,
  RetentionPolicy,
  TopicStats,
} from '../interface/interface';
import RetentionEngine from '../retention/retentionEngine';
import BufferMemory from '../storage/bufferMemory';
import DiskStore from '../storage/diskStorage';
import SecondaryIndex from '../storage/secondaryIndex';

interface Consumer<T> {
  groupId: string;
  handler: MessageHandler<T>;
  offset: number;
}

export class Topic<T = Record<string, unknown>> {
  memory = new BufferMemory<T>(50_000);
  index = new SecondaryIndex();
  disk: DiskStore;
  consumers = new Map<string, Consumer<T>>();
  nextOffset = 0;
  private writeCount = 0;
  private lastStatTs = Date.now();
  writesPerSec = 0;

  constructor(
    public readonly name: string,
    dataDir: string,
  ) {
    this.disk = new DiskStore(dataDir, name);
    this.replayFromDisk();
  }

  private replayFromDisk(): void {
    let max = -1;
    for (const rec of this.disk.replayAll()) {
      this.index.insert({
        offset: rec.offset,
        timestamp: rec.timestamp,
        key: rec.key,
      });
      if (rec.offset > max) max = rec.offset;
    }
    if (max >= 0) this.nextOffset = max + 1;
  }

  trackWrite(): void {
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

export class EventStreaming {
  private topics = new Map<string, Topic>();
  private offsetFile: string;
  private retention: RetentionEngine | null = null;
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;
  private streamListeners = new Map<
    string,
    Map<string, Set<(rec: LogRecord) => void>>
  >();
  private saveOffsetsPending = false;

  constructor(
    private readonly dataDir: string = './msgbus-data',
    private readonly options: StreamOptions = {},
  ) {
    fs.mkdirSync(dataDir, { recursive: true });
    this.offsetFile = path.join(dataDir, '_consumer-offsets.json');

    if (options.retention) {
      this.retention = new RetentionEngine(options.retention);
    }

    const intervalMin = options.cleanupIntervalMinutes ?? 60;
    if (intervalMin > 0 && this.retention) {
      const ms = intervalMin * 60_000;
      this.cleanupTimer = setInterval(() => {
        console.log('[bus] auto-cleanup running...');
        this.cleanup();
      }, ms);
      if ((this.cleanupTimer as any).unref) (this.cleanupTimer as any).unref();
      console.log(`[bus] auto-cleanup scheduled every ${intervalMin} min`);
    }
  }

  private getTopic(name: string): Topic {
    if (!this.topics.has(name))
      this.topics.set(name, new Topic(name, this.dataDir));
    return this.topics.get(name)!;
  }

  private loadOffsets(): Record<string, Record<string, number>> {
    try {
      if (fs.existsSync(this.offsetFile))
        return JSON.parse(fs.readFileSync(this.offsetFile, 'utf-8'));
    } catch {}
    return {};
  }

  private saveOffsets(): void {
    const snap: Record<string, Record<string, number>> = {};
    for (const [name, topic] of this.topics) {
      snap[name] = {};
      for (const [gid, c] of topic.consumers) snap[name][gid] = c.offset;
    }
    fs.writeFileSync(this.offsetFile, JSON.stringify(snap), 'utf-8');
  }

  private scheduleOffsetSave(): void {
    if (this.saveOffsetsPending) return;
    this.saveOffsetsPending = true;
    setTimeout(() => {
      this.saveOffsetsPending = false;
      this.saveOffsets();
    }, 500).unref();
  }

  produce(opts: ProduceOptions): number {
    const topic = this.getTopic(opts.topic);

    const record: LogRecord = {
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

  produceBatch(records: ProduceOptions[]): number[] {
    return records.map((r) => this.produce(r));
  }

  subscribe<T = Record<string, unknown>>(
    opts: ConsumerOptions,
    handler: MessageHandler<T>,
  ): () => void {
    const topic = this.getTopic(opts.topic) as Topic<T>;
    const saved = this.loadOffsets();
    const offset =
      saved[opts.topic]?.[opts.groupId] ??
      (opts.fromBeginning ? 0 : topic.nextOffset);

    const consumer: Consumer<T> = { groupId: opts.groupId, handler, offset };
    topic.consumers.set(opts.groupId, consumer);

    if (consumer.offset < topic.nextOffset) {
      setImmediate(() =>
        this.catchUp(
          topic as unknown as Topic,
          consumer as unknown as Consumer<unknown>,
        ),
      );
    }

    return () => {
      topic.consumers.delete(opts.groupId);
      this.saveOffsets();
    };
  }

  private deliver(topic: Topic, record: LogRecord): void {
    for (const consumer of topic.consumers.values()) {
      if (consumer.offset === record.offset) {
        consumer.offset++;
        try {
          const p = consumer.handler(record);
          if (p instanceof Promise)
            p.then(() => this.scheduleOffsetSave()).catch((e) =>
              console.error(`[bus][${consumer.groupId}]`, e),
            );
          else this.scheduleOffsetSave();
        } catch (e) {
          console.error(`[bus][${consumer.groupId}]`, e);
        }
      }
    }

    this.notifyStreamListeners(topic.name, record);
  }

  private notifyStreamListeners(topicName: string, record: LogRecord): void {
    const topicMap = this.streamListeners.get(topicName);

    if (!topicMap) return;

    const keyListeners = topicMap.get(record.key);

    if (keyListeners) for (const fn of keyListeners) fn(record);

    const wildcardListeners = topicMap.get('*');

    if (wildcardListeners) for (const fn of wildcardListeners) fn(record);
  }

  private addStreamListener(
    topicName: string,
    key: string,
    fn: (rec: LogRecord) => void,
  ): () => void {
    if (!this.streamListeners.has(topicName))
      this.streamListeners.set(topicName, new Map());

    const topicMap = this.streamListeners.get(topicName)!;

    if (!topicMap.has(key)) topicMap.set(key, new Set());

    topicMap.get(key)!.add(fn);

    return () => topicMap.get(key)?.delete(fn);
  }

  private catchUp(topic: Topic, consumer: Consumer<unknown>): void {
    const ringMin = topic.memory.minOffset;
    const fromDisk = new Set<number>();

    if (ringMin !== -1 && consumer.offset < ringMin) {
      for (let o = consumer.offset; o < ringMin; o++) fromDisk.add(o);
    }

    for (const rec of topic.disk.readByOffsets(fromDisk)) {
      consumer.offset = rec.offset + 1;
      try {
        consumer.handler(rec);
      } catch (e) {
        console.error(`[bus][${consumer.groupId}] catchup`, e);
      }
    }

    for (const rec of topic.memory.from(consumer.offset)) {
      consumer.offset = rec.offset + 1;
      try {
        consumer.handler(rec);
      } catch (e) {
        console.error(`[bus][${consumer.groupId}] catchup`, e);
      }
    }

    this.saveOffsets();
  }

  // ── Query ─────────────────────────────────────────────────────────────────

  query(opts: QueryOptions): LogRecord[] {
    const topic = this.getTopic(opts.topic);
    const limit = opts.limit ?? 100;
    const skip = opts.skip ?? 0;
    const order = opts.order ?? 'desc';

    const offsets = topic.index.query({
      key: opts.key,
      fromTime: opts.fromTime,
      toTime: opts.toTime,
      limit: limit + skip,
      skip: 0,
      order,
    });

    const paginated = offsets.slice(skip, skip + limit);
    if (paginated.length === 0) return [];

    const set = new Set(paginated);
    const ringMin = topic.memory.minOffset;
    const fromRing = new Set<number>();
    const fromDisk = new Set<number>();

    for (const off of set) {
      if (ringMin !== -1 && off >= ringMin) fromRing.add(off);
      else fromDisk.add(off);
    }

    const results: LogRecord[] = [];
    for (const rec of topic.memory) {
      if (fromRing.has(rec.offset)) results.push(rec);
    }

    for (const rec of topic.disk.readByOffsets(fromDisk)) {
      results.push(rec);
    }

    results.sort((a, b) =>
      order === 'asc' ? a.offset - b.offset : b.offset - a.offset,
    );
    return results;
  }

  queryStream(opts: QueryStreamOptions): Readable {
    const historySize = opts.historySize ?? 1000;
    const order = opts.order ?? 'asc';
    const listenerKey = opts.key ?? '*';

    let removeLive: (() => void) | null = null;

    const stream = new Readable({ objectMode: false, read() {} });

    const push = (record: LogRecord): void => {
      if (stream.destroyed) return;
      if (opts.fromTime && record.timestamp < opts.fromTime) return;
      if (opts.toTime && record.timestamp > opts.toTime) return;
      stream.push(JSON.stringify(record) + '\n');
    };

    setImmediate(() => {
      if (stream.destroyed) return;

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
        if (stream.destroyed) return;
        push(record);
      }

      if (!stream.destroyed) {
        removeLive = this.addStreamListener(opts.topic, listenerKey, push);
      }
    });

    stream.on('close', () => removeLive?.());

    return stream;
  }

  // ── Retention / Cleanup ───────────────────────────────────────────────────

  private cleanup(
    topicName?: string,
    policyOverride?: RetentionPolicy,
  ): CleanupReport[] {
    const engine = policyOverride
      ? new RetentionEngine(policyOverride)
      : this.retention;

    if (!engine) {
      console.warn('[bus] cleanup() called but no retention policy is set.');
      return [];
    }

    const targets = topicName
      ? [this.getTopic(topicName)]
      : [...this.topics.values()];

    const reports: CleanupReport[] = [];

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
      const earliestSurviving =
        surviving.length > 0
          ? Math.min(...surviving.map((m) => m.baseOffset))
          : topic.nextOffset;

      // Advance any slow consumers past the gap — "delete anyway" behaviour
      const skippedConsumers: CleanupReport['skippedConsumers'] = [];
      for (const consumer of topic.consumers.values()) {
        if (consumer.offset < earliestSurviving) {
          console.warn(
            `[bus][${topic.name}] consumer "${consumer.groupId}" at offset ${consumer.offset} ` +
              `skipped to ${earliestSurviving} (segment deleted)`,
          );
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

      const report: CleanupReport = {
        topic: topic.name,
        segmentsDeleted: toDelete.length,
        recordsDropped,
        bytesFreed,
        reasons,
        skippedConsumers,
      };

      console.log(
        `[bus] cleanup [${topic.name}]: ${toDelete.length} segments deleted, ` +
          `${(bytesFreed / 1e6).toFixed(1)} MB freed, ${recordsDropped} records dropped`,
      );

      reports.push(report);
    }

    return reports;
  }

  // ── Stats ─────────────────────────────────────────────────────────────────
  stats(topicName: string): TopicStats {
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

  close(): void {
    if (this.cleanupTimer) clearInterval(this.cleanupTimer);
    this.saveOffsets();
    for (const topic of this.topics.values()) topic.disk.close();
  }
}
