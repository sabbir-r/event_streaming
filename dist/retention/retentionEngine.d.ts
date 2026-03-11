import { RetentionPolicy, SegmentMeta } from '../interface/interface';
export default class RetentionEngine {
    private readonly policy;
    constructor(policy: RetentionPolicy);
    evaluate(metas: SegmentMeta[]): {
        toDelete: SegmentMeta[];
        reasons: string[];
    };
}
