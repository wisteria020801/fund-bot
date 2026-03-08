from __future__ import annotations
from datetime import datetime
from typing import Dict, List
from fundbot.config import AppConfig
from fundbot import db
from fundbot.fetch import fetch_fund_nav_series, calc_returns, max_drawdown, fetch_fund_meta, fetch_top_holdings_codes
from fundbot.quant import score_pool
from fundbot.notify import send_telegram_message
from fundbot.ai import summarize_with_llm, fallback_summary
from fundbot.config import to_json


def main() -> int:
    db.init_db()
    cfg = AppConfig.load()
    today = datetime.utcnow().date().isoformat()
    pool: List[Dict] = []
    for f in cfg.funds:
        code = f.code.strip()
        if not code:
            continue
        nav_df = fetch_fund_nav_series(code, 365)
        rets = calc_returns(nav_df) if nav_df is not None else {"r1": None, "r7": None, "r30": None, "r90": None}
        mdd = max_drawdown(nav_df) if nav_df is not None else None
        fee, aum = fetch_fund_meta(code)
        holdings = fetch_top_holdings_codes(code)
        latest_nav = None
        if nav_df is not None and not nav_df.empty:
            latest_nav = float(nav_df.iloc[-1]["nav"])
        data = {
            "code": code,
            "name": f.name or code,
            "latest_nav": latest_nav,
            "change_1d": rets.get("r1"),
            "change_7d": rets.get("r7"),
            "change_30d": rets.get("r30"),
            "change_90d": rets.get("r90"),
            "top_holdings": to_json(holdings),
            "max_drawdown": mdd,
            "fee_rate": f.fee_rate if f.fee_rate is not None else fee,
            "aum": f.aum if f.aum is not None else aum,
            "updated_at": datetime.utcnow().isoformat(),
        }
        db.upsert_fund(data)
        pool.append(data)
    ranked = score_pool(pool) if pool else []
    for x in ranked:
        db.upsert_score(
            {
                "code": x["code"],
                "date": today,
                "total": x["score_total"],
                "rank30": x["score_rank30"],
                "rank90": x["score_rank90"],
                "penalty_drawdown": x["penalty_drawdown"],
                "score_aum": x["score_aum"],
                "penalty_fee": x["penalty_fee"],
            }
        )
    top = ranked[:3]
    bottom = ranked[-3:] if ranked else []
    payload = {
        "scores": ranked,
        "top": top,
        "bottom": bottom,
        "type": "nav_update",
        "ts": datetime.utcnow().isoformat(),
    }
    llm = summarize_with_llm(payload) or fallback_summary(payload)
    lines = []
    lines.append("📊 【Wisteria Fund Bot - 净值复盘】")
    lines.append(f"🤖 AI 核心判断：{llm}")
    if top:
        lines.append("🏆 今日高分标的：")
        for x in top:
            lines.append(f"• {x.get('name', x['code'])} (综合分 {x['score_total']})")
    if bottom:
        lines.append("⚠️ 风险警示：")
        for x in bottom:
            lines.append(f"• {x.get('name', x['code'])} (综合分 {x['score_total']})")
    text = "\n".join(lines)
    send_telegram_message(text)
    db.log_message("nav_update", text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
