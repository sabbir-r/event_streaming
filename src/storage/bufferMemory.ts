import { LogRecord } from '../interface/interface';

export default class BufferMemory<T = Record<string, unknown>> {
  private buffer: Array<LogRecord<T> | undefined>;
  private head = 0;
  private _size = 0;

  constructor(private readonly capacity: number) {
    this.buffer = new Array(capacity);
  }

  push(record: LogRecord<T>): void {
    this.buffer[this.head] = record;
    this.head = (this.head + 1) % this.capacity;
    if (this._size < this.capacity) this._size++;
  }

  *[Symbol.iterator](): Iterator<LogRecord<T>> {
    if (this._size === 0) return;
    const start = this._size < this.capacity ? 0 : this.head;
    for (let i = 0; i < this._size; i++) {
      const rec = this.buffer[(start + i) % this.capacity];
      if (rec !== undefined) yield rec;
    }
  }

  *from(fromOffset: number): Generator<LogRecord<T>> {
    for (const rec of this) if (rec.offset >= fromOffset) yield rec;
  }

  get size(): number {
    return this._size;
  }

  get minOffset(): number {
    for (const r of this) return r.offset;
    return -1;
  }
}
