from __future__ import annotations
from datetime import datetime, timedelta
import time
from typing import Dict, List
from fundbot.config import AppConfig
from fundbot import db
from fundbot.fetch import (
    fetch_fund_nav_series,
    calc_returns,
    max_drawdown,
    fetch_fund_meta,
    fetch_top_holdings_codes,
    ndx_ma_bias,
    yf_pct_change,
    rsi,
)
from fundbot.quant import score_pool
from fundbot.notify import send_telegram_message
from fundbot.ai import summarize_with_llm, fallback_summary
from fundbot.config import to_json
import yfinance as yf
from fundbot.fetch import fred_dgs10_latest


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
        nav_df = None
        for _ in range(3):
            nav_df = fetch_fund_nav_series(code, 365)
            if nav_df is not None and not nav_df.empty:
                break
            time.sleep(2)
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
        if (not ranked) and historical:
            targets = []
            d = datetime.utcnow().date()
            for i in range(1, 8):
                t = d - timedelta(days=i)
                if t.weekday() < 5:
                    targets.append(t)
                if len(targets) >= 3:
                    break
            for t in targets:
                pool2: List[Dict] = []
                for f in cfg.funds:
                    code = f.code.strip()
                    df = fetch_fund_nav_series(code, 365)
                    if df is None or df.empty:
                        continue
                    rets2 = fetch.calc_returns_asof(df, t)
                    mdd2 = max_drawdown(df[df["date"] <= t])
                    fee2, aum2 = f.fee_rate, f.aum
                    if fee2 is None or aum2 is None:
                        fee3, aum3 = fetch_fund_meta(code)
                        fee2 = fee2 if fee2 is not None else fee3
                        aum2 = aum2 if aum2 is not None else aum3
                    data2 = {
                        "code": code,
                        "name": f.name or code,
                        "latest_nav": None,
                        "change_1d": rets2.get("r1"),
                        "change_7d": rets2.get("r7"),
                        "change_30d": rets2.get("r30"),
                        "change_90d": rets2.get("r90"),
                        "top_holdings": "[]",
                        "max_drawdown": mdd2,
                        "fee_rate": fee2,
                        "aum": aum2,
                    }
                    pool2.append(data2)
                ranked2 = score_pool(pool2) if pool2 else []
                for x in ranked2:
                    db.upsert_score(
                        {
                            "code": x["code"],
                            "date": t.isoformat(),
                            "total": x.get("score_total"),
                            "rank30": x.get("score_rank30"),
                            "rank90": x.get("score_rank90"),
                            "penalty_drawdown": x.get("penalty_drawdown"),
                            "score_aum": x.get("score_aum"),
                            "penalty_fee": x.get("penalty_fee"),
                        }
                    )
    payload = {
        "scores": ranked,
        "top": top,
        "bottom": bottom,
        "type": "nav_update",
        "ts": datetime.utcnow().isoformat(),
        "note": fallback_note,
    }
    llm = summarize_with_llm(payload) or fallback_summary(payload)
    # 决策路径拆解
    pct = yf_pct_change("NQ=F") or yf_pct_change("^NDX") or 0.0
    th = cfg.dca.thresholds
    # 基础乘数
    if pct <= th.crash_hard:
        base_mult = 2.0
    elif pct <= th.crash:
        base_mult = 1.5
    elif pct >= th.bubble:
        base_mult = 0.5
    else:
        base_mult = 1.0
    # 位置修正
    bias = ndx_ma_bias(250)
    if bias is None:
        pos_mult = 1.0
    elif bias < -5.0:
        pos_mult = 1.25
    elif bias > 5.0:
        pos_mult = min(1.0, 1.0)  # 明确限制到不高于1.0
    else:
        pos_mult = 1.0
    dca_mult = base_mult
    if bias is not None:
        if bias < -5.0:
            dca_mult = min(2.0, dca_mult * pos_mult)
        elif bias > 5.0:
            dca_mult = max(0.5, min(dca_mult, 1.0))
    if dca_mult == 1.0 and top:
        avg_score = sum(x.get("score_total", 0.0) for x in top) / max(1, len(top))
        if avg_score <= -10:
            dca_mult = 1.5
        elif avg_score >= 30:
            dca_mult = 0.5
    # 宏观刹车
    dgs10 = fred_dgs10_latest()
    if dgs10 is not None and dgs10 > cfg.dca.macro_brake_threshold:
        macro_mult = cfg.dca.macro_brake_factor
        dca_mult = max(0.5, round(dca_mult * macro_mult, 2))
        macro_note = f"10Y={dgs10:.2f}%>阈值{cfg.dca.macro_brake_threshold:.2f}%，乘数×{cfg.dca.macro_brake_factor}"
    else:
        macro_mult = 1.0
    dca_amount = round(cfg.dca.base_amount * dca_mult, 2)
    # 选拔理由（简单启发式）
    reasons = {}
    if ranked:
        fees = [z.get("fee_rate") for z in ranked if z.get("fee_rate") is not None]
        fee_med = sorted(fees)[len(fees)//2] if fees else None
        mdds = [z.get("max_drawdown") for z in ranked if z.get("max_drawdown") is not None]
        mdd_med = sorted(mdds)[len(mdds)//2] if mdds else None
        for z in top:
            rs = []
            if fee_med is not None and z.get("fee_rate") is not None and z["fee_rate"] <= fee_med:
                rs.append("费率较低")
            if mdd_med is not None and z.get("max_drawdown") is not None and z["max_drawdown"] <= mdd_med:
                rs.append("回撤控制较好")
            if z.get("change_30d") and z.get("change_30d") > 0:
                rs.append("近30日表现偏强")
            if not rs and z.get("change_90d") and z["change_90d"] > 0:
                rs.append("近90日回升")
            reasons[z["code"]] = "；".join(rs[:2]) if rs else "综合因子均衡"
    # 决策看板消息
    lines = []
    lines.append("📊 【Wisteria Fund Bot - 决策看板】")
    lines.append("1. 市场环境监控")
    pct_str = f"{pct:.2f}%" if pct is not None else "—"
    bias_str = f"{bias:.2f}%" if bias is not None else "—"
    dgs10_str = f"{dgs10:.2f}%" if dgs10 is not None else "—"
    lines.append(f"• 纳指期货 NQ=F：{pct_str}")
    lines.append(f"• 年线乖离率 Bias：{bias_str}")
    lines.append(f"• 10年期美债 DGS10：{dgs10_str}")
    lines.append("2. 决策计算路径")
    lines.append(f"• 基础乘数：{base_mult:.2f}x（源于日变动）")
    lines.append(f"• 位置修正：×{pos_mult:.2f}（源于 Bias）")
    lines.append(f"• 宏观修正：×{macro_mult:.2f}（源于 DGS10）")
    lines.append(f"3. 最终指令：{dca_mult:.2f}x → 建议 {dca_amount:.2f} 元")
    if macro_note:
        lines.append(f"🛑 宏观刹车：{macro_note}")
    # 加仓确认：RSI<30 且连跌3天
    suggest_lump = False
    if dca_mult >= 2.0 and top:
        try:
            top_code = top[0].get("code")
            nav_df = fetch_fund_nav_series(top_code, 60)
            if nav_df is not None and not nav_df.empty and len(nav_df) >= 20:
                closes = nav_df["nav"].astype(float).tolist()
                rsi14 = rsi(closes, period=14)
                downs = 0
                for i in range(1, 4):
                    if closes[-i] < closes[-i - 1]:
                        downs += 1
                if (rsi14 is not None and rsi14 < 30) and downs >= 3:
                    suggest_lump = True
        except Exception:
            suggest_lump = False
    if suggest_lump:
        cand = ", ".join([x.get('name', x['code']) for x in top[:2]])
        lines.append(f"🧭 一次性加仓候选（冷静期满足）：{cand}（RSI<30 & 三连跌）")
    if top:
        lines.append("4. 标的选拔")
        for x in top:
            r = reasons.get(x["code"]) if reasons else None
            reason = f"— 理由：{r}" if r else ""
            lines.append(f"• {x.get('name', x['code'])}（综合分 {x['score_total']}）{reason}")
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
    rsi14_logged = None
    try:
        if suggest_lump and 'rsi14' in locals():
            rsi14_logged = rsi14
    except Exception:
        rsi14_logged = None
    try:
        db.upsert_dca_log(
            {
                "date": today,
                "bias": bias,
                "dgs10": dgs10,
                "rsi14": rsi14_logged,
                "dca_mult": dca_mult,
                "dca_amount": dca_amount,
                "pct": pct,
                "avg_score": sum(x.get("score_total", 0.0) for x in top) / max(1, len(top)) if top else None,
                "suggest_lump": 1 if suggest_lump else 0,
                "note": fallback_note,
                "ts": datetime.utcnow().isoformat(),
            }
        )
        db.export_dca_csv("dca_history_snapshot.csv")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
