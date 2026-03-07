"use strict";
// ─────────────────────────────────────────────────────────────────────────────
// RETENTION ENGINE
// ─────────────────────────────────────────────────────────────────────────────
Object.defineProperty(exports, "__esModule", { value: true });
class RetentionEngine {
    constructor(policy) {
        this.policy = policy;
    }
    evaluate(metas) {
        var _a;
        const candidates = metas.filter((m) => !m.isActive);
        const deleteSet = new Set();
        const reasons = [];
        // ── Rule 1: Time-based ────────────────────────────────────────────────
        const maxAgeMs = this.policy.maxAgeDays != null
            ? this.policy.maxAgeDays * 86400000
            : this.policy.maxAgeHours != null
                ? this.policy.maxAgeHours * 3600000
                : null;
        if (maxAgeMs !== null) {
            const cutoff = Date.now() - maxAgeMs;
            for (const seg of candidates) {
                if (seg.newestTimestamp > 0 && seg.newestTimestamp < cutoff) {
                    deleteSet.add(seg.baseOffset);
                    const ageDays = ((Date.now() - seg.newestTimestamp) /
                        86400000).toFixed(1);
                    reasons.push(`[time] segment ${seg.baseOffset}: newest record is ${ageDays} days old (limit: ${(_a = this.policy.maxAgeDays) !== null && _a !== void 0 ? _a : (this.policy.maxAgeHours / 24).toFixed(1)} days)`);
                }
            }
        }
        // ── Rule 2: Size-based ────────────────────────────────────────────────
        const maxBytes = this.policy.maxSizeGB != null
            ? this.policy.maxSizeGB * 1073741824
            : this.policy.maxSizeMB != null
                ? this.policy.maxSizeMB * 1048576
                : null;
        if (maxBytes !== null) {
            let totalBytes = metas.reduce((s, m) => s + m.sizeBytes, 0);
            const sorted = [...candidates].sort((a, b) => a.baseOffset - b.baseOffset);
            for (const seg of sorted) {
                if (totalBytes <= maxBytes)
                    break;
                if (!deleteSet.has(seg.baseOffset)) {
                    deleteSet.add(seg.baseOffset);
                    reasons.push(`[size] segment ${seg.baseOffset}: total ${(totalBytes / 1e9).toFixed(2)} GB ` +
                        `> limit ${(maxBytes / 1e9).toFixed(2)} GB — freeing ${(seg.sizeBytes / 1e6).toFixed(1)} MB`);
                }
                totalBytes -= seg.sizeBytes;
            }
        }
        // ── Rule 3: Record count ──────────────────────────────────────────────
        if (this.policy.maxRecords != null) {
            const total = metas.reduce((s, m) => s + m.recordCount, 0);
            if (total > this.policy.maxRecords) {
                let excess = total - this.policy.maxRecords;
                const sorted = [...candidates].sort((a, b) => a.baseOffset - b.baseOffset);
                for (const seg of sorted) {
                    if (excess <= 0)
                        break;
                    if (!deleteSet.has(seg.baseOffset)) {
                        deleteSet.add(seg.baseOffset);
                        reasons.push(`[records] segment ${seg.baseOffset}: ${total.toLocaleString()} records ` +
                            `> limit ${this.policy.maxRecords.toLocaleString()} — dropping ${seg.recordCount} records`);
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
exports.default = RetentionEngine;
