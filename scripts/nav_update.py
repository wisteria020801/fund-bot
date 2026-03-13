from __future__ import annotations
from datetime import datetime, timedelta
import time
from typing import Dict, List
import os
from fundbot.config import AppConfig
from fundbot import db
from fundbot.fetch import (
    fetch_fund_nav_series,
    calc_returns,
    calc_returns_asof,
    max_drawdown,
    fetch_fund_meta,
    fetch_top_holdings_codes,
    ndx_ma_bias,
    yf_pct_change,
    yf_live_pct_change,
    rsi,
)
from fundbot.quant import score_pool
from fundbot.notify import send_telegram_message, format_score_total
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
    cache_used: Dict[str, bool] = {}
    role_map = {f.code.strip(): f.role for f in cfg.funds if f.code and f.code.strip()}
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
            try:
                latest_nav = float(nav_df.iloc[-1]["nav"])
            except Exception:
                latest_nav = None
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
        used_cache = False
        snap = historical.get(code) if historical else None
        if snap:
            if latest_nav is None or latest_nav == 0:
                if snap.get("latest_nav") not in (None, 0):
                    latest_nav = snap.get("latest_nav")
                    used_cache = True
            if rets.get("r1") is None and snap.get("change_1d") is not None:
                rets["r1"] = snap.get("change_1d")
                used_cache = True
            if rets.get("r7") is None and snap.get("change_7d") is not None:
                rets["r7"] = snap.get("change_7d")
                used_cache = True
            if rets.get("r30") is None and snap.get("change_30d") is not None:
                rets["r30"] = snap.get("change_30d")
                used_cache = True
            if mdd is None and snap.get("max_drawdown") is not None:
                mdd = snap.get("max_drawdown")
                used_cache = True
            if fee is None and snap.get("fee_rate") is not None:
                fee = snap.get("fee_rate")
                used_cache = True
            if aum is None and snap.get("aum") is not None:
                aum = snap.get("aum")
                used_cache = True
        data = {
            "code": code,
            "name": f.name or code,
            "role": f.role,
            "latest_nav": latest_nav,
            "change_1d": rets.get("r1"),
            "change_7d": rets.get("r7"),
            "change_30d": rets.get("r30"),
            "change_90d": rets.get("r90"),
            "top_holdings": to_json(holdings) if holdings else (snap.get("top_holdings") if snap and snap.get("top_holdings") else "[]"),
            "max_drawdown": mdd,
            "fee_rate": f.fee_rate if f.fee_rate is not None else fee,
            "aum": f.aum if f.aum is not None else aum,
            "updated_at": datetime.utcnow().isoformat(),
            "used_cache": used_cache,
        }
        db.upsert_fund(data)
        pool.append(data)
        cache_used[code] = used_cache
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
    ranked_today = score_pool(pool) if pool else []
    for x in ranked_today:
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
    ranked = ranked_today
    if not ranked or all(abs(z.get("score_total", 0.0)) < 1e-9 for z in ranked):
        last_date = db.latest_scores_date()
        if last_date and last_date != today:
            prev = db.scores_by_date(last_date)
            if prev:
                ranked = []
                for r in prev:
                    code = str(r.get("code") or "").strip()
                    snap = historical.get(code) if historical and code else None
                    ranked.append(
                        {
                            "code": code,
                            "name": r.get("name") or (snap.get("name") if snap else None) or code,
                            "role": role_map.get(code),
                            "score_total": float(r.get("total") or 0.0),
                            "score_rank30": float(r.get("rank30") or 0.0),
                            "score_rank90": float(r.get("rank90") or 0.0),
                            "penalty_drawdown": float(r.get("penalty_drawdown") or 0.0),
                            "score_aum": float(r.get("score_aum") or 0.0),
                            "penalty_fee": float(r.get("penalty_fee") or 0.0),
                            "change_30d": snap.get("change_30d") if snap else None,
                            "change_90d": None,
                            "max_drawdown": snap.get("max_drawdown") if snap else None,
                            "fee_rate": snap.get("fee_rate") if snap else None,
                            "aum": snap.get("aum") if snap else None,
                            "used_cache": True,
                        }
                    )
                ranked.sort(key=lambda z: z.get("score_total", 0.0), reverse=True)
    if not ranked:
        ranked = []
        for f in cfg.funds:
            code = f.code.strip()
            snap = historical.get(code) if historical and code else None
            ranked.append(
                {
                    "code": code,
                    "name": f.name or (snap.get("name") if snap else None) or code,
                    "role": f.role,
                    "score_total": 0.0,
                    "score_rank30": 0.0,
                    "score_rank90": 0.0,
                    "penalty_drawdown": 0.0,
                    "score_aum": 0.0,
                    "penalty_fee": 0.0,
                    "change_30d": snap.get("change_30d") if snap else None,
                    "change_90d": None,
                    "max_drawdown": snap.get("max_drawdown") if snap else None,
                    "fee_rate": snap.get("fee_rate") if snap else None,
                    "aum": snap.get("aum") if snap else None,
                    "used_cache": True,
                }
            )
    top_for_msg = ranked[:3]
    bottom_for_msg = ranked[-3:] if ranked else []
    payload = {
        "scores": ranked,
        "top": top_for_msg,
        "bottom": bottom_for_msg,
        "type": "nav_update",
        "ts": datetime.utcnow().isoformat(),
    }
    llm = summarize_with_llm(payload) or fallback_summary(payload)
    cn_now = datetime.utcnow() + timedelta(hours=8)
    mode = (os.getenv("NAV_UPDATE_MODE") or "").strip().lower()
    if not mode:
        if cn_now.hour == 9:
            mode = "morning"
        elif cn_now.hour == 14:
            mode = "close"
        else:
            mode = "sync"

    pct_raw: float | None
    if mode == "close":
        pct_raw = yf_live_pct_change("NQ=F") or yf_pct_change("NQ=F")
    elif mode == "morning":
        pct_raw = yf_pct_change("^NDX") or yf_pct_change("NQ=F")
    else:
        pct_raw = yf_pct_change("NQ=F") or yf_pct_change("^NDX")
    pct = pct_raw if pct_raw is not None else 0.0
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
    if dca_mult == 1.0 and top_for_msg:
        avg_score = sum(x.get("score_total", 0.0) for x in top_for_msg) / max(1, len(top_for_msg))
        if avg_score <= -10:
            dca_mult = 1.5
        elif avg_score >= 30:
            dca_mult = 0.5
    # 宏观刹车
    macro_note = None
    dgs10 = fred_dgs10_latest()
    if dgs10 is not None and dgs10 > cfg.dca.macro_brake_threshold:
        macro_mult = cfg.dca.macro_brake_factor
        dca_mult = max(0.5, round(dca_mult * macro_mult, 2))
        macro_note = f"10Y={dgs10:.2f}%>阈值{cfg.dca.macro_brake_threshold:.2f}%，乘数×{cfg.dca.macro_brake_factor}"
    else:
        macro_mult = 1.0
    base_amount = float(cfg.dca.base_amount or 10.0)
    dca_amount = round(base_amount * dca_mult, 2)
    if dca_amount <= 0:
        dca_amount = base_amount
    # 选拔理由（简单启发式）
    reasons = {}
    if top_for_msg:
        fees = [z.get("fee_rate") for z in ranked if z.get("fee_rate") is not None]
        fee_med = sorted(fees)[len(fees)//2] if fees else None
        mdds = [z.get("max_drawdown") for z in ranked if z.get("max_drawdown") is not None]
        mdd_med = sorted(mdds)[len(mdds)//2] if mdds else None
        for z in top_for_msg:
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
    if mode == "morning":
        lines.append("📊 【Wisteria Fund Bot - 今日执行指令（09:30）】")
    elif mode == "close":
        lines.append("📊 【Wisteria Fund Bot - 收盘提醒（14:30）】")
    else:
        lines.append("📊 【Wisteria Fund Bot - 数据同步报告】")
    lines.append(f"🤖 AI 核心判断：{llm}")
    lines.append("数据来源：实时=AKShare；缓存=本地DB最近有效交易日 (Cache)")
    lines.append("1. 市场环境监控")
    pct_str = f"{pct_raw:.2f}%" if pct_raw is not None else "数据同步中"
    bias_str = f"{bias:.2f}%" if bias is not None else "数据同步中"
    dgs10_str = f"{dgs10:.2f}%" if dgs10 is not None else "数据同步中"
    lines.append(f"• 纳指期货 NQ=F：{pct_str}")
    lines.append(f"• 年线乖离率 Bias：{bias_str}")
    lines.append(f"• 10年期美债 DGS10：{dgs10_str}")
    if mode == "sync":
        lines.append("提示：当前为数据同步阶段，以下指令按可得数据计算，仅供参考")
    lines.append("2. 决策计算路径")
    lines.append(f"• 基础乘数：{base_mult:.2f}x（源于 NQ=F/^NDX）")
    lines.append(f"• 位置修正：×{pos_mult:.2f}（源于 Bias）")
    lines.append(f"• 宏观修正：×{macro_mult:.2f}（源于 DGS10）")
    lines.append(f"3. 最终指令：{dca_mult:.2f}x → 建议 {dca_amount:.2f} 元")
    if macro_note:
        lines.append(f"🛑 宏观刹车：{macro_note}")
    suggest_lump = False
    if dca_mult >= 2.0 and top_for_msg:
        try:
            top_code = top_for_msg[0].get("code")
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
        cand = ", ".join([x.get("name", x["code"]) for x in top_for_msg[:2]])
        lines.append(f"🧭 一次性加仓候选（冷静期满足）：{cand}（RSI<30 & 三连跌）")
    if top_for_msg:
        lines.append("4. 标的选拔")
        for x in top_for_msg:
            r = reasons.get(x["code"]) if reasons else None
            reason = f"— 理由：{r}" if r else ""
            score = format_score_total(x.get("score_total"), bool(x.get("used_cache")))
            r30 = x.get("score_rank30")
            r90 = x.get("score_rank90")
            r30s = f"{float(r30):.2f}" if r30 is not None else "数据同步中"
            r90s = f"{float(r90):.2f}" if r90 is not None else "数据同步中"
            lines.append(f"• {x.get('name', x['code'])}（综合分 {score}，Rank30 {r30s}，Rank90 {r90s}）{reason}")
    if bottom_for_msg:
        lines.append("⚠️ 风险警示：")
        for x in bottom_for_msg:
            score = format_score_total(x.get("score_total"), bool(x.get("used_cache")))
            r30 = x.get("score_rank30")
            r90 = x.get("score_rank90")
            r30s = f"{float(r30):.2f}" if r30 is not None else "数据同步中"
            r90s = f"{float(r90):.2f}" if r90 is not None else "数据同步中"
            lines.append(f"• {x.get('name', x['code'])} (综合分 {score}，Rank30 {r30s}，Rank90 {r90s})")
    if alerts:
        lines.append("🚨 阈值告警：")
        lines.extend(alerts)
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
                "avg_score": sum(x.get("score_total", 0.0) for x in top_for_msg) / max(1, len(top_for_msg)) if top_for_msg else None,
                "suggest_lump": 1 if suggest_lump else 0,
                "note": None,
                "ts": datetime.utcnow().isoformat(),
            }
        )
        db.export_dca_csv("dca_history_snapshot.csv")
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
