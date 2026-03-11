import { LogRecord } from '../interface/interface';
export default class BufferMemory<T = Record<string, unknown>> {
    private readonly capacity;
    private buffer;
    private head;
    private _size;
    constructor(capacity: number);
    push(record: LogRecord<T>): void;
    [Symbol.iterator](): Iterator<LogRecord<T>>;
    from(fromOffset: number): Generator<LogRecord<T>>;
    get size(): number;
    get minOffset(): number;
}
