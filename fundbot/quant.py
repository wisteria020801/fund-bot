from __future__ import annotations
from typing import Dict, List, Optional, Tuple
import math
import statistics


def _percentile_rank(values: List[float], v: float) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    count = sum(1 for x in sorted_vals if x <= v)
    return count / len(sorted_vals) * 100.0


def _num(v: object) -> float | None:
    try:
        x = float(v)  # type: ignore[arg-type]
        if x != x or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def score_pool(
    pool: List[Dict],
) -> List[Dict]:
    r30_values = [v for v in (_num(x.get("change_30d")) for x in pool) if v is not None]
    r90_values = [v for v in (_num(x.get("change_90d")) for x in pool) if v is not None]
    out: List[Dict] = []
    for x in pool:
        # 若缺少收益数据，排名记为中性0，不做加分
        v30 = _num(x.get("change_30d")) or 0.0
        v90 = _num(x.get("change_90d")) or 0.0
        rank30 = _percentile_rank(r30_values, v30) if r30_values else 0.0
        rank90 = _percentile_rank(r90_values, v90) if r90_values else 0.0
        w_rank = 0.4 * ((rank30 + rank90) / 2.0)
        mdd = _num(x.get("max_drawdown"))
        # 缺失最大回撤时不惩罚，避免数据不全导致系统性负分
        penalty_drawdown = 0.3 * (mdd if mdd is not None else 0.0)
        aum = _num(x.get("aum"))
        score_aum = 0.0
        if aum is not None:
            if 2e8 <= aum <= 5e9:
                score_aum = 0.2 * 100.0
            else:
                score_aum = 0.2 * max(0.0, 100.0 - abs((aum - 1.0e9) / 1.0e9) * 100.0)
        fee = _num(x.get("fee_rate"))
        # 缺失费率时不惩罚
        penalty_fee = 0.1 * (fee if fee is not None else 0.0)
        total = w_rank + score_aum - penalty_drawdown - penalty_fee
        y = dict(x)
        y.update(
            {
                "score_total": round(total, 2),
                "score_rank30": round(rank30, 2),
                "score_rank90": round(rank90, 2),
                "penalty_drawdown": round(penalty_drawdown, 2),
                "score_aum": round(score_aum, 2),
                "penalty_fee": round(penalty_fee, 2),
            }
        )
        out.append(y)
    out.sort(key=lambda z: z["score_total"], reverse=True)
    return out
