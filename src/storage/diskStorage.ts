import * as fs from 'fs';
import * as path from 'path';
import { LogRecord } from '../interface/interface';

interface SegmentMeta {
  filePath: string;
  baseOffset: number;
  lastOffset: number;
  sizeBytes: number;
  oldestTimestamp: number;
  newestTimestamp: number;
  recordCount: number;
  isActive: boolean;
}

const MAX_SEGMENT_BYTES = 256 * 1024 * 1024;
const FLUSH_INTERVAL_MS = 50;
const FLUSH_BATCH_SIZE = 500;

export default class DiskStore {
  private writeBuffer: string[] = [];
  private currentFd: number = -1;
  private currentSegmentSize = 0;
  private currentSegmentBase = 0;
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private isFlushing = false;
  public totalFlushed = 0;

  constructor(
    private readonly dir: string,
    private readonly topic: string,
  ) {
    fs.mkdirSync(this.segDir, { recursive: true });
    this.openOrCreateActiveSegment();
    this.startFlushLoop();
  }

  private get segDir(): string {
    return path.join(this.dir, this.topic);
  }

  private listPaths(): string[] {
    return fs
      .readdirSync(this.segDir)
      .filter((f) => f.endsWith('.log'))
      .sort()
      .map((f) => path.join(this.segDir, f));
  }

  private openSegment(base: number): void {
    if (this.currentFd !== -1) fs.closeSync(this.currentFd);
    const file = path.join(
      this.segDir,
      String(base).padStart(10, '0') + '.log',
    );
    this.currentFd = fs.openSync(file, 'a');
    this.currentSegmentBase = base;
    this.currentSegmentSize = 0;
  }

  private openOrCreateActiveSegment(): void {
    const files = this.listPaths();
    if (files.length === 0) {
      this.openSegment(0);
      return;
    }
    const last = files[files.length - 1];
    const base = parseInt(path.basename(last, '.log'), 10);
    const size = fs.statSync(last).size;
    if (size >= MAX_SEGMENT_BYTES) this.openSegment(base + 1);
    else {
      this.currentSegmentBase = base;
      this.currentSegmentSize = size;
      this.currentFd = fs.openSync(last, 'a');
    }
  }

  enqueue(record: LogRecord): void {
    this.writeBuffer.push(JSON.stringify(record) + '\n');
    if (this.writeBuffer.length >= FLUSH_BATCH_SIZE) this.flush();
  }

  private startFlushLoop(): void {
    this.flushTimer = setInterval(() => this.flush(), FLUSH_INTERVAL_MS);
    if ((this.flushTimer as any).unref) (this.flushTimer as any).unref();
  }

  flush(): void {
    if (this.isFlushing || this.writeBuffer.length === 0) return;
    this.isFlushing = true;
    const batch = this.writeBuffer.splice(0);
    const payload = batch.join('');
    const byteLength = Buffer.byteLength(payload, 'utf-8');
    if (this.currentSegmentSize + byteLength >= MAX_SEGMENT_BYTES)
      this.openSegment(this.currentSegmentBase + 1);
    fs.writeSync(this.currentFd, payload, null, 'utf-8');
    this.currentSegmentSize += byteLength;
    this.totalFlushed += batch.length;
    this.isFlushing = false;
  }

  *readByOffsets(offsets: Set<number>): Generator<LogRecord> {
    if (offsets.size === 0) return;
    const remaining = new Set(offsets);
    for (const file of this.listPaths()) {
      if (remaining.size === 0) break;
      const content = fs.readFileSync(file, 'utf-8');
      for (const line of content.split('\n')) {
        if (!line.trim()) continue;
        try {
          const rec: LogRecord = JSON.parse(line);
          if (remaining.has(rec.offset)) {
            yield rec;
            remaining.delete(rec.offset);
          }
        } catch {
          /* skip */
        }
      }
    }
  }

  *replayAll(): Generator<LogRecord> {
    for (const file of this.listPaths()) {
      const content = fs.readFileSync(file, 'utf-8');
      for (const line of content.split('\n')) {
        if (!line.trim()) continue;
        try {
          yield JSON.parse(line) as LogRecord;
        } catch {
          /* skip */
        }
      }
    }
  }

  buildSegmentMetas(): SegmentMeta[] {
    const files = this.listPaths();
    return files.map((file) => {
      const base = parseInt(path.basename(file, '.log'), 10);
      const stat = fs.statSync(file);
      let first: LogRecord | null = null;
      let last: LogRecord | null = null;
      let count = 0;

      const content = fs.readFileSync(file, 'utf-8');
      for (const line of content.split('\n')) {
        if (!line.trim()) continue;
        try {
          const rec: LogRecord = JSON.parse(line);
          if (!first) first = rec;
          last = rec;
          count++;
        } catch {
          /* skip */
        }
      }

      return {
        filePath: file,
        baseOffset: base,
        lastOffset: last?.offset ?? -1,
        sizeBytes: stat.size,
        oldestTimestamp: first?.timestamp ?? 0,
        newestTimestamp: last?.timestamp ?? 0,
        recordCount: count,
        isActive: base === this.currentSegmentBase,
      };
    });
  }

  deleteSegment(baseOffset: number): number {
    const file = path.join(
      this.segDir,
      String(baseOffset).padStart(10, '0') + '.log',
    );
    if (baseOffset === this.currentSegmentBase) {
      console.warn(`[bus] skipping active segment ${baseOffset}`);
      return 0;
    }
    try {
      const size = fs.statSync(file).size;
      fs.unlinkSync(file);
      return size;
    } catch (e) {
      console.error(`[bus] failed to delete segment ${file}:`, e);
      return 0;
    }
  }

  get totalDiskBytes(): number {
    return this.listPaths().reduce((s, f) => {
      try {
        return s + fs.statSync(f).size;
      } catch {
        return s;
      }
    }, 0);
  }

  get segmentCount(): number {
    return this.listPaths().length;
  }

  close(): void {
    this.flush();
    if (this.flushTimer) clearInterval(this.flushTimer);
    if (this.currentFd !== -1) fs.closeSync(this.currentFd);
  }
}
