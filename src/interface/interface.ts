export interface LogRecord<T = Record<string, unknown>> {
  offset: number;
  topic: string;
  key: string;
  timestamp: number;
  data: T;
}

export interface ProduceOptions {
  topic: string;
  key: string;
  data: Record<string, unknown>;
}

export type MessageHandler<T = Record<string, unknown>> = (
  record: LogRecord<T>,
) => void | Promise<void>;

export interface ConsumerOptions {
  topic: string;
  groupId: string;
  fromBeginning?: boolean;
}

export interface QueryOptions {
  topic: string;
  key?: string;
  fromTime?: number;
  toTime?: number;
  limit?: number;
  skip?: number;
  order?: 'asc' | 'desc';
}

export interface RetentionPolicy {
  maxAgeDays?: number;
  maxAgeHours?: number;
  maxSizeGB?: number;
  maxSizeMB?: number;
  maxRecords?: number;
}

export interface BusOptions {
  retention?: RetentionPolicy;
  cleanupIntervalMinutes?: number;
}

export interface TopicStats {
  topic: string;
  totalRecords: number;
  latestOffset: number;
  consumers: string[];
  indexedKeys: number;
  memoryRecords: number;
  writesPerSec: number;
  diskSegments: number;
  diskSizeBytes: number;
}

export interface CleanupReport {
  topic: string;
  segmentsDeleted: number;
  recordsDropped: number;
  bytesFreed: number;
  reasons: string[];
  skippedConsumers: { groupId: string; skippedToOffset: number }[];
}

export interface QueryStreamOptions {
  topic: string;
  key?: string;
  historySize?: number;
  order?: 'asc' | 'desc';
  fromTime?: number;
  toTime?: number;
}

export interface SegmentMeta {
  filePath: string;
  baseOffset: number;
  lastOffset: number;
  sizeBytes: number;
  oldestTimestamp: number;
  newestTimestamp: number;
  recordCount: number;
  isActive: boolean;
}
