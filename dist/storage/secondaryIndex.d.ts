interface IndexEntry {
    offset: number;
    timestamp: number;
    key: string;
}
export default class SecondaryIndex {
    private keyIndex;
    private timeIndex;
    private offsetMap;
    insert(entry: IndexEntry): void;
    purgeBelow(minOffset: number): number;
    query(opts: {
        key?: string;
        fromTime?: number;
        toTime?: number;
        limit: number;
        skip: number;
        order: 'asc' | 'desc';
    }): number[];
    get totalKeys(): number;
    get totalEntries(): number;
}
export {};
