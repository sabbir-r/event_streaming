// ─────────────────────────────────────────────────────────────────────────────
// RETENTION ENGINE
// ─────────────────────────────────────────────────────────────────────────────

import { RetentionPolicy, SegmentMeta } from '../interface/interface';

export default class RetentionEngine {
  constructor(private readonly policy: RetentionPolicy) {}

  evaluate(metas: SegmentMeta[]): {
    toDelete: SegmentMeta[];
    reasons: string[];
  } {
    const candidates = metas.filter((m) => !m.isActive);
    const deleteSet = new Set<number>();
    const reasons: string[] = [];

    // ── Rule 1: Time-based ────────────────────────────────────────────────
    const maxAgeMs =
      this.policy.maxAgeDays != null
        ? this.policy.maxAgeDays * 86_400_000
        : this.policy.maxAgeHours != null
          ? this.policy.maxAgeHours * 3_600_000
          : null;

    if (maxAgeMs !== null) {
      const cutoff = Date.now() - maxAgeMs;
      for (const seg of candidates) {
        if (seg.newestTimestamp > 0 && seg.newestTimestamp < cutoff) {
          deleteSet.add(seg.baseOffset);
          const ageDays = (
            (Date.now() - seg.newestTimestamp) /
            86_400_000
          ).toFixed(1);
          reasons.push(
            `[time] segment ${seg.baseOffset}: newest record is ${ageDays} days old (limit: ${
              this.policy.maxAgeDays ??
              (this.policy.maxAgeHours! / 24).toFixed(1)
            } days)`,
          );
        }
      }
    }

    // ── Rule 2: Size-based ────────────────────────────────────────────────
    const maxBytes =
      this.policy.maxSizeGB != null
        ? this.policy.maxSizeGB * 1_073_741_824
        : this.policy.maxSizeMB != null
          ? this.policy.maxSizeMB * 1_048_576
          : null;

    if (maxBytes !== null) {
      let totalBytes = metas.reduce((s, m) => s + m.sizeBytes, 0);
      const sorted = [...candidates].sort(
        (a, b) => a.baseOffset - b.baseOffset,
      );
      for (const seg of sorted) {
        if (totalBytes <= maxBytes) break;
        if (!deleteSet.has(seg.baseOffset)) {
          deleteSet.add(seg.baseOffset);
          reasons.push(
            `[size] segment ${seg.baseOffset}: total ${(totalBytes / 1e9).toFixed(2)} GB ` +
              `> limit ${(maxBytes / 1e9).toFixed(2)} GB — freeing ${(seg.sizeBytes / 1e6).toFixed(1)} MB`,
          );
        }
        totalBytes -= seg.sizeBytes;
      }
    }

    // ── Rule 3: Record count ──────────────────────────────────────────────
    if (this.policy.maxRecords != null) {
      const total = metas.reduce((s, m) => s + m.recordCount, 0);
      if (total > this.policy.maxRecords) {
        let excess = total - this.policy.maxRecords;
        const sorted = [...candidates].sort(
          (a, b) => a.baseOffset - b.baseOffset,
        );
        for (const seg of sorted) {
          if (excess <= 0) break;
          if (!deleteSet.has(seg.baseOffset)) {
            deleteSet.add(seg.baseOffset);
            reasons.push(
              `[records] segment ${seg.baseOffset}: ${total.toLocaleString()} records ` +
                `> limit ${this.policy.maxRecords.toLocaleString()} — dropping ${seg.recordCount} records`,
            );
          }
          excess -= seg.recordCount;
        }
      }
    }

    return {
      toDelete: candidates
        .filter((m) => deleteSet.has(m.baseOffset))
        .sort((a, b) => a.baseOffset - b.baseOffset),
      reasons,
    };
  }
}
