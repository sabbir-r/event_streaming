import { Readable } from 'stream';
import { StreamOptions, ConsumerOptions, LogRecord, MessageHandler, ProduceOptions, QueryOptions, QueryStreamOptions, TopicStats } from '../interface/interface';
import BufferMemory from '../storage/bufferMemory';
import DiskStore from '../storage/diskStorage';
import SecondaryIndex from '../storage/secondaryIndex';
interface Consumer<T> {
    groupId: string;
    handler: MessageHandler<T>;
    offset: number;
}
export declare class Topic<T = Record<string, unknown>> {
    readonly name: string;
    memory: BufferMemory<T>;
    index: SecondaryIndex;
    disk: DiskStore;
    consumers: Map<string, Consumer<T>>;
    nextOffset: number;
    private writeCount;
    private lastStatTs;
    writesPerSec: number;
    constructor(name: string, dataDir: string);
    private replayFromDisk;
    trackWrite(): void;
}
export declare class EventStreaming {
    private readonly dataDir;
    private readonly options;
    private topics;
    private offsetFile;
    private retention;
    private cleanupTimer;
    private streamListeners;
    private saveOffsetsPending;
    private saveOffsetTimer;
    constructor(dataDir?: string, options?: StreamOptions);
    private getTopic;
    private loadOffsets;
    private saveOffsets;
    private scheduleOffsetSave;
    produce(opts: ProduceOptions): number;
    produceBatch(records: ProduceOptions[]): number[];
    subscribe<T = Record<string, unknown>>(opts: ConsumerOptions, handler: MessageHandler<T>): () => void;
    private deliver;
    private notifyStreamListeners;
    private addStreamListener;
    private catchUp;
    query(opts: QueryOptions): LogRecord[];
    queryStream(opts: QueryStreamOptions): Readable;
    private cleanup;
    stats(topicName: string): TopicStats;
    close(): void;
}
export {};
