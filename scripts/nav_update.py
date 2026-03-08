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
    alerts: List[str] = []
    historical = db.all_funds_snapshot()
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
        if (rets.get("r30") is None or rets.get("r90") is None) and historical.get(code):
            snap = historical[code]
            rets = {
                "r1": rets.get("r1") if rets.get("r1") is not None else snap.get("change_1d"),
                "r7": rets.get("r7") if rets.get("r7") is not None else snap.get("change_7d"),
                "r30": rets.get("r30") if rets.get("r30") is not None else snap.get("change_30d"),
                "r90": rets.get("r90") if rets.get("r90") is not None else None,
            }
            if mdd is None:
                mdd = snap.get("max_drawdown")
            if fee is None:
                fee = snap.get("fee_rate")
            if aum is None:
                aum = snap.get("aum")
        data = {
            "code": code,
            "name": f.name or code,
            "role": f.role,
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
        # watch: 日波动阈值告警（配置为小数，0.015=1.5%）
        try:
            threshold = f.watch.daily_change_alert if (f.watch and f.watch.daily_change_alert is not None) else None
        except Exception:
            threshold = None
        chg1 = rets.get("r1")
        if threshold is not None and chg1 is not None:
            if abs(chg1) / 100.0 >= float(threshold):
                role_tag = f"[{f.role}]" if f.role else ""
                alerts.append(f"• {f.name or code}{role_tag} 日变动 {chg1:.2f}% ≥ 阈值 {threshold*100:.2f}%")
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
    fallback_note = None
    if not ranked or all(abs(z.get("score_total", 0.0)) < 1e-9 for z in ranked):
        last_date = db.latest_scores_date()
        if last_date and last_date != today:
            prev = db.scores_by_date(last_date)
            if prev:
                ranked = prev
                top = ranked[:3]
                bottom = ranked[-3:]
                fallback_note = f"数据不足，沿用上一交易日评分（{last_date}）。"
        if (not ranked) and not fallback_note:
            fallback_note = "数据不足，今日不发布榜单。"
    payload = {
        "scores": ranked,
        "top": top,
        "bottom": bottom,
        "type": "nav_update",
        "ts": datetime.utcnow().isoformat(),
        "note": fallback_note,
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
    if alerts:
        lines.append("🚨 阈值告警：")
        lines.extend(alerts)
    if fallback_note:
        lines.append(f"ℹ️ {fallback_note}")
    text = "\n".join(lines)
    send_telegram_message(text)
    db.log_message("nav_update", text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
