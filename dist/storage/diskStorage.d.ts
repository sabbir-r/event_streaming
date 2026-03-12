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
export default class DiskStore {
    private readonly dir;
    private readonly topic;
    private currentFd;
    private currentSegmentSize;
    private currentSegmentBase;
    totalFlushed: number;
    totalDropped: number;
    private diskQueue;
    private readonly MAX_QUEUE;
    private isWorkerRunning;
    constructor(dir: string, topic: string);
    private get segDir();
    private listPaths;
    private openSegment;
    private openOrCreateActiveSegment;
    enqueue(record: LogRecord): void;
    private startWorker;
    flush(): void;
    readByOffsets(offsets: Set<number>, getSegment?: (offset: number) => number | undefined): Generator<LogRecord>;
    replayAll(): Generator<LogRecord>;
    buildSegmentMetas(): SegmentMeta[];
    deleteSegment(baseOffset: number): number;
    get totalDiskBytes(): number;
    get segmentCount(): number;
    close(): void;
}
export {};
