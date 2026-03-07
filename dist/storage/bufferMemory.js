"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class BufferMemory {
    constructor(capacity) {
        this.capacity = capacity;
        this.head = 0;
        this._size = 0;
        this.buffer = new Array(capacity);
    }
    push(record) {
        this.buffer[this.head] = record;
        this.head = (this.head + 1) % this.capacity;
        if (this._size < this.capacity)
            this._size++;
    }
    *[Symbol.iterator]() {
        if (this._size === 0)
            return;
        const start = this._size < this.capacity ? 0 : this.head;
        for (let i = 0; i < this._size; i++) {
            const rec = this.buffer[(start + i) % this.capacity];
            if (rec !== undefined)
                yield rec;
        }
    }
    *from(fromOffset) {
        for (const rec of this)
            if (rec.offset >= fromOffset)
                yield rec;
    }
    get size() {
        return this._size;
    }
    get minOffset() {
        for (const r of this)
            return r.offset;
        return -1;
    }
}
exports.default = BufferMemory;
