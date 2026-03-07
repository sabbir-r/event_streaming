interface IndexEntry {
  offset: number;
  timestamp: number;
  key: string;
}

export default class SecondaryIndex {
  private keyIndex = new Map<string, number[]>();
  private timeIndex: Array<[number, number]> = [];
  private offsetMap = new Map<number, IndexEntry>();

  insert(entry: IndexEntry): void {
    if (!this.keyIndex.has(entry.key)) this.keyIndex.set(entry.key, []);
    this.keyIndex.get(entry.key)!.push(entry.offset);
    this.timeIndex.push([entry.timestamp, entry.offset]);
    this.offsetMap.set(entry.offset, entry);
  }

  purgeBelow(minOffset: number): number {
    let dropped = 0;
    for (const [offset] of this.offsetMap) {
      if (offset < minOffset) {
        this.offsetMap.delete(offset);
        dropped++;
      }
    }
    for (const [key, offsets] of this.keyIndex) {
      const filtered = offsets.filter((o) => o >= minOffset);
      if (filtered.length === 0) this.keyIndex.delete(key);
      else this.keyIndex.set(key, filtered);
    }
    this.timeIndex = this.timeIndex.filter(([, o]) => o >= minOffset);
    return dropped;
  }

  query(opts: {
    key?: string;
    fromTime?: number;
    toTime?: number;
    limit: number;
    skip: number;
    order: 'asc' | 'desc';
  }): number[] {
    let candidates: number[] =
      opts.key !== undefined
        ? (this.keyIndex.get(opts.key) ?? [])
        : this.timeIndex.map(([, o]) => o);

    if (opts.fromTime !== undefined || opts.toTime !== undefined) {
      candidates = candidates.filter((offset) => {
        const e = this.offsetMap.get(offset);
        if (!e) return false;
        if (opts.fromTime !== undefined && e.timestamp < opts.fromTime)
          return false;
        if (opts.toTime !== undefined && e.timestamp > opts.toTime)
          return false;
        return true;
      });
    }

    if (opts.order === 'desc') candidates = [...candidates].reverse();
    return candidates.slice(opts.skip, opts.skip + opts.limit);
  }

  get totalKeys(): number {
    return this.keyIndex.size;
  }
  get totalEntries(): number {
    return this.offsetMap.size;
  }
}
