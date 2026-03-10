# node-event-streaming

A lightweight, zero-dependency event streaming library for Node.js. Pub/sub, persistent storage, offset tracking, and KSQL-style live streaming — all embedded, no external broker required.

[![npm version](https://img.shields.io/npm/v/node-event-streaming.svg)](https://www.npmjs.com/package/node-event-streaming)

---

## Why node-event-streaming?

Most event streaming solutions require running a separate broker (Kafka, RabbitMQ, Redis Streams). **node-event-streaming** is fully embedded inside your Node.js process — no broker, no Docker, no configuration files. Just install and use.

| Feature            | node-event-streaming | Kafka       | Redis Streams |
| ------------------ | -------------------- | ----------- | ------------- |
| External broker    | ❌ Not needed        | ✅ Required | ✅ Required   |
| Persistent storage | ✅                   | ✅          | ✅            |
| Pub / Sub          | ✅                   | ✅          | ✅            |
| Live streaming     | ✅                   | ✅          | ✅            |
| Zero dependencies  | ✅                   | ❌          | ❌            |
| Setup time         | ~1 min               | ~30 min     | ~15 min       |

---

## Installation

```bash
npm install node-event-streaming
```

---

## Quick Start

```typescript
import { EventStreaming } from 'node-event-streaming';

const streamer = new EventStreaming('./data');

// subscribe first
streamer.subscribe(
  { topic: 'user-location', groupId: 'my-service' },
  async (record) => {
    console.log(record.data);
    // { key: '1', lat: '23.78', lon: '90.41' }
  },
);

// produce
streamer.produce({
  topic: 'user-location',
  key: '1',
  data: { lat: '23.78', lon: '90.41' },
});
```

---

## Core Concepts

| Concept       | Description                                                    |
| ------------- | -------------------------------------------------------------- |
| **Topic**     | A named channel — e.g. `user-location`, `order-placed`         |
| **Key**       | Identifies the entity — e.g. user ID, order ID                 |
| **Offset**    | Auto-incrementing position of each record in the topic         |
| **Group ID**  | Unique name per consumer — tracks its own offset independently |
| **Retention** | Auto-delete old data by age, size, or record count             |

---

## API Reference

### `new EventStreaming(dataDir, options?)`

Creates a new instance. All data is persisted to `dataDir`.

```typescript
import { EventStreaming } from 'node-event-streaming';

const streamer = new EventStreaming('./data', {
  retention: {
    maxAgeDays: 7, // delete records older than 7 days
    maxSizeGB: 10, // keep total disk usage under 10 GB
    maxRecords: 5_000_000, // keep at most 5 million records
  },
  cleanupIntervalMinutes: 60, // run cleanup every hour automatically
});
```

---

### `streamer.produce(opts)` — Publish a single record

```typescript
streamer.produce({
  topic: 'user-location',
  key: '1', // identifies the entity (e.g. user ID)
  data: {
    // any object
    lat: '23.78',
    lon: '90.41',
  },
});
```

---

### `streamer.produceBatch(records[])` — Publish multiple records

Faster than looping `produce()` — all records share one disk flush.

```typescript
streamer.produceBatch([
  { topic: 'user-location', key: '1', data: { lat: '23.78', lon: '90.41' } },
  { topic: 'user-location', key: '2', data: { lat: '24.10', lon: '91.20' } },
  { topic: 'user-location', key: '3', data: { lat: '22.50', lon: '89.30' } },
]);
```

---

### `streamer.subscribe(opts, handler)` — Consume events

Returns an `unsubscribe` function. Call it to stop consuming.

```typescript
const unsubscribe = streamer.subscribe(
  {
    topic: 'user-location',
    groupId: 'cache-updater', // unique name for this consumer
    fromBeginning: false, // true = replay all history on connect
  },
  async (record) => {
    console.log(record.offset); // 42
    console.log(record.key); // "1"
    console.log(record.timestamp); // 1720000000000
    console.log(record.data); // { lat: '23.78', lon: '90.41' }
  },
);

// stop consuming
unsubscribe();
```

**Multiple consumers on the same topic** — each tracks its own offset independently:

```typescript
// consumer 1 — updates cache
streamer.subscribe(
  { topic: 'user-location', groupId: 'cache-updater' },
  async (record) => {
    await redis.set(`user_${record.key}`, record.data);
  },
);

// consumer 2 — saves to database
streamer.subscribe(
  { topic: 'user-location', groupId: 'db-writer' },
  async (record) => {
    await db.insert('locations', record.data);
  },
);

// consumer 3 — sends notifications
streamer.subscribe(
  { topic: 'user-location', groupId: 'notifier' },
  async (record) => {
    await notify(record.key, record.data);
  },
);
```

---

### `streamer.query(opts)` — Query historical records

Returns an array of matching records. Useful for REST endpoints.

```typescript
// last 100 records for user 1
const records = streamer.query({
  topic: 'user-location',
  key: '1',
  limit: 100,
  order: 'desc', // newest first
});

// time range query
const records = streamer.query({
  topic: 'user-location',
  key: '1',
  fromTime: Date.now() - 3_600_000, // last 1 hour
  toTime: Date.now(),
  limit: 1000,
});

// paginate
const page2 = streamer.query({
  topic: 'user-location',
  key: '1',
  limit: 50,
  skip: 50, // skip first 50
  order: 'asc',
});
```

---

### `streamer.queryStream(opts)` — Live streaming (KSQL-style EMIT CHANGES)

Returns a Node.js `Readable` stream. Sends history first then stays open forever — every new record streams through as it arrives. Only closes when you call `stream.destroy()`.

```typescript
const stream = streamer.queryStream({
  topic: 'user-location',
  key: '1',
  historySize: 1000, // send last 1000 records on connect
  order: 'asc',
});

stream.on('data', (chunk) => {
  const record = JSON.parse(chunk.toString());
  console.log(record.data);
});

// stop the stream
stream.destroy();
```

**Pipe to Express SSE endpoint:**

```typescript
app.get('/users/:id/location/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const stream = streamer.queryStream({
    topic: 'user-location',
    key: req.params.id,
    historySize: 1000,
  });

  stream.on('data', (chunk) => {
    res.write(`data: ${chunk.toString()}\n\n`);
  });

  // clean up when client disconnects
  res.on('close', () => stream.destroy());
});
```

**WebSocket:**

```typescript
wss.on('connection', (ws, req) => {
  const userId = req.url.split('/').pop();

  const stream = streamer.queryStream({
    topic: 'user-location',
    key: userId,
  });

  stream.on('data', (chunk) => ws.send(chunk.toString()));
  ws.on('close', () => stream.destroy());
});
```

**Async iterator:**

```typescript
for await (const chunk of streamer.queryStream({
  topic: 'user-location',
  key: '1',
})) {
  const record = JSON.parse(chunk.toString());
  console.log(record.data);
}
```

---

### `streamer.cleanup(topic?, policy?)` — Manual retention cleanup

```typescript
// cleanup all topics using configured policy
streamer.cleanup();

// cleanup one topic
streamer.cleanup('user-location');

// one-off override
streamer.cleanup('user-location', { maxSizeGB: 1 });
```

---

### `streamer.stats(topic)` — Topic stats

```typescript
const stats = streamer.stats('user-location');
console.log(stats);
// {
//   topic:         'user-location',
//   totalRecords:  150000,
//   latestOffset:  149999,
//   consumers:     ['cache-updater', 'db-writer'],
//   indexedKeys:   5000,
//   memoryRecords: 50000,
//   writesPerSec:  1200,
//   diskSegments:  3,
//   diskSizeBytes: 12400000,
// }
```

---

### `streamer.close()` — Graceful shutdown

Always call on process exit to flush pending writes and close file descriptors.

```typescript
process.on('SIGTERM', () => {
  streamer.close();
  process.exit(0);
});

process.on('SIGINT', () => {
  streamer.close();
  process.exit(0);
});
```

---

## Full Example — Real-time Location Tracking

```typescript
import express from 'express';
import { EventStreaming } from 'node-event-streaming';

const app = express();
const streamer = new EventStreaming('./data', {
  retention: { maxAgeDays: 7, maxSizeGB: 20 },
  cleanupIntervalMinutes: 60,
});

app.use(express.json());

// start consumer on app startup — updates Redis cache on every new location
const unsubscribe = streamer.subscribe(
  { topic: 'user-location', groupId: 'cache-updater' },
  async (record) => {
    await redis.set(`user_${record.key}`, JSON.stringify(record.data));
  },
);

// POST /location — publish a new location
app.post('/location', (req, res) => {
  const { user_id, lat, lon } = req.body;

  streamer.produce({
    topic: 'user-location',
    key: String(user_id),
    data: { user_id, lat, lon },
  });

  res.json({ success: true });
});

// GET /users/:id/locations — get location history
app.get('/users/:id/locations', (req, res) => {
  const records = streamer.query({
    topic: 'user-location',
    key: req.params.id,
    limit: 100,
    order: 'desc',
  });

  res.json(records.map((r) => r.data));
});

// GET /users/:id/locations/stream — live location stream (SSE)
app.get('/users/:id/locations/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const stream = streamer.queryStream({
    topic: 'user-location',
    key: req.params.id,
    historySize: 1000,
  });

  stream.on('data', (chunk) => res.write(`data: ${chunk.toString()}\n\n`));
  res.on('close', () => stream.destroy());
});

// graceful shutdown
const shutdown = () => {
  unsubscribe();
  streamer.close();
  process.exit(0);
};
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

app.listen(3000, () => console.log('Server running on port 3000'));
```

---

## Record Structure

Every record stored by the system has this shape:

```typescript
{
  offset:    42,                  // auto-incrementing position
  topic:     'user-location',     // topic name
  key:       '1',                 // entity identifier
  timestamp: 1720000000000,       // epoch ms, set automatically
  data: {                         // your payload
    lat: '23.78',
    lon: '90.41',
  }
}
```

---

## Performance

| Metric                    | Value                                   |
| ------------------------- | --------------------------------------- |
| Write throughput (memory) | 300k – 480k msg/sec                     |
| Write throughput (disk)   | 10k – 20k msg/sec                       |
| Hot records in memory     | 50,000 per topic                        |
| Memory usage              | ~25 MB ring + ~60 bytes per index entry |
| Disk segment size         | 256 MB per segment                      |
| Flush interval            | Every 50ms or 500 records               |
| Max data loss on crash    | ~50ms of writes                         |

---

## Use Cases

- Real-time location tracking
- Event-driven microservices
- Activity and audit logging
- Live dashboards and monitoring
- Notification pipelines
- Lightweight streaming inside a single Node.js service

---

## License

MIT
