"""
股票分析评分模块。

评分模型（总分100）：
  技术面 30%：RSI 超买超卖 + MACD 金叉死叉 + 布林带位置
  基本面 40%：PE 合理性 + PB 合理性 + 近期涨跌趋势
  估值   20%：PE/PB 与历史阈值对比
  情绪   10%：短期动量（5日涨幅方向）

用法：
  from fundbot.stocks import analyze_pool, format_report
  results = analyze_pool(cfg)  # 分析全部观察池
  text = format_report(results)  # 生成推送文本
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from fundbot.config import AppConfig
from fundbot.fetch import (
    bollinger,
    fetch_cn_stock_hist,
    fetch_cn_stock_price,
    fetch_cn_stock_valuation,
    fetch_us_stock_hist,
    fetch_us_stock_price,
    fetch_us_stock_valuation,
    macd,
    rsi,
)


def _safe(v) -> Optional[float]:
    try:
        x = float(v)
        if x != x or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _pct_change(closes: List[float], window: int) -> Optional[float]:
    if not closes or len(closes) < window + 1:
        return None
    return (closes[-1] - closes[-1 - window]) / closes[-1 - window] * 100.0


# ==================== 单只股票分析 ====================

def analyze_stock(
    code: str,
    market: str,
    name: str = "",
    sector: str = "",
    cfg: Optional[AppConfig] = None,
) -> Dict[str, Any]:
    """
    分析单只股票，返回完整指标 + 评分。

    market: 'cn_stock' | 'us_stock'
    """
    if cfg is None:
        cfg = AppConfig.load()
    sa = cfg.stock_analysis
    w = sa.weights
    vt = sa.valuation
    tt = sa.technical

    # 1. 获取数据
    if market == "cn_stock":
        hist = fetch_cn_stock_hist(code, 120)
        pe, pb = fetch_cn_stock_valuation(code)
        current_price = fetch_cn_stock_price(code)
    else:
        hist = fetch_us_stock_hist(code, "6mo")
        pe, pb = fetch_us_stock_valuation(code)
        current_price = fetch_us_stock_price(code)

    # 实时价获取失败时，用 K线最新收盘价兜底
    if current_price is None and hist is not None and not hist.empty:
        current_price = float(hist.iloc[-1]["close"])

    result: Dict[str, Any] = {
        "code": code,
        "market": market,
        "name": name or code,
        "sector": sector,
        "price": current_price,
        "pe": pe,
        "pb": pb,
        "rsi14": None,
        "macd_dif": None,
        "macd_dea": None,
        "macd_hist": None,
        "boll_upper": None,
        "boll_mid": None,
        "boll_lower": None,
        "pct_5d": None,
        "pct_20d": None,
        "pct_60d": None,
        "score_technical": 0.0,
        "score_fundamental": 0.0,
        "score_valuation": 0.0,
        "score_sentiment": 0.0,
        "score_total": 0.0,
        "signal": "数据不足",
        "reasons": [],
    }

    closes: List[float] = []
    if hist is not None and not hist.empty:
        closes = hist["close"].astype(float).tolist()
        result["rsi14"] = rsi(closes, 14)
        dif, dea, hist_val = macd(closes)
        result["macd_dif"] = dif
        result["macd_dea"] = dea
        result["macd_hist"] = hist_val
        upper, mid, lower = bollinger(closes, 20, 2.0)
        result["boll_upper"] = upper
        result["boll_mid"] = mid
        result["boll_lower"] = lower
        result["pct_5d"] = _pct_change(closes, 5)
        result["pct_20d"] = _pct_change(closes, 20)
        result["pct_60d"] = _pct_change(closes, 60)

    # 2. 技术面评分 (满分100，最终 ×权重)
    tech_score = 50.0  # 中性起点
    tech_reasons: List[str] = []

    rsi_val = result["rsi14"]
    if rsi_val is not None:
        if rsi_val < tt.rsi_oversold:
            tech_score += 20
            tech_reasons.append(f"RSI{rsi_val:.0f}超卖")
        elif rsi_val > tt.rsi_overbought:
            tech_score -= 20
            tech_reasons.append(f"RSI{rsi_val:.0f}超买")
        else:
            tech_reasons.append(f"RSI{rsi_val:.0f}中性")

    dif = result["macd_dif"]
    dea = result["macd_dea"]
    hist_val = result["macd_hist"]
    if dif is not None and dea is not None:
        if dif > dea:
            tech_score += 15
            tech_reasons.append("MACD金叉")
        else:
            tech_score -= 15
            tech_reasons.append("MACD死叉")

    if hist_val is not None:
        if hist_val > 0:
            tech_score += 5
        else:
            tech_score -= 5

    # 布林带位置
    if current_price is not None and result["boll_upper"] is not None and result["boll_lower"] is not None:
        boll_range = result["boll_upper"] - result["boll_lower"]
        if boll_range > 0:
            pos = (current_price - result["boll_lower"]) / boll_range  # 0~1
            if pos < 0.2:
                tech_score += 10
                tech_reasons.append("接近布林下轨")
            elif pos > 0.8:
                tech_score -= 10
                tech_reasons.append("接近布林上轨")

    tech_score = max(0, min(100, tech_score))
    result["score_technical"] = round(tech_score, 1)

    # 3. 基本面评分 (满分100)
    fund_score = 50.0
    fund_reasons: List[str] = []

    if pe is not None:
        if pe > 0:
            if pe < vt.pe_undervalued:
                fund_score += 25
                fund_reasons.append(f"PE{pe:.1f}偏低")
            elif pe < vt.pe_overvalued:
                fund_score += 10
                fund_reasons.append(f"PE{pe:.1f}合理")
            else:
                fund_score -= 15
                fund_reasons.append(f"PE{pe:.1f}偏高")
        elif pe < 0:
            fund_reasons.append(f"PE{pe:.1f}亏损")

    if pb is not None:
        if pb > 0:
            if pb < vt.pb_undervalued:
                fund_score += 15
                fund_reasons.append(f"PB{pb:.1f}偏低")
            elif pb < vt.pb_overvalued:
                fund_score += 5
            else:
                fund_score -= 10
                fund_reasons.append(f"PB{pb:.1f}偏高")

    # 趋势加分
    pct_60d = result["pct_60d"]
    if pct_60d is not None:
        if pct_60d > 10:
            fund_score += 10
            fund_reasons.append("60日强势")
        elif pct_60d < -10:
            fund_score -= 5
            fund_reasons.append("60日弱势")

    fund_score = max(0, min(100, fund_score))
    result["score_fundamental"] = round(fund_score, 1)

    # 4. 估值评分 (满分100) — PE/PB 相对阈值
    val_score = 50.0
    val_reasons: List[str] = []

    if pe is not None and pe > 0:
        if pe < vt.pe_undervalued:
            val_score = 80
            val_reasons.append("估值低估")
        elif pe > vt.pe_overvalued:
            val_score = 20
            val_reasons.append("估值高估")
        else:
            # 在合理区间内，越低越高
            ratio = (pe - vt.pe_undervalued) / (vt.pe_overvalued - vt.pe_undervalued)
            val_score = 80 - ratio * 40  # 80→40 线性递减

    if pb is not None and pb > 0:
        if pb < vt.pb_undervalued:
            val_score = min(90, val_score + 15)
        elif pb > vt.pb_overvalued:
            val_score = max(10, val_score - 15)

    val_score = max(0, min(100, val_score))
    result["score_valuation"] = round(val_score, 1)

    # 5. 情绪评分 (满分100) — 短期动量
    sent_score = 50.0
    sent_reasons: List[str] = []
    pct_5d = result["pct_5d"]
    if pct_5d is not None:
        if pct_5d > 3:
            sent_score += 25
            sent_reasons.append("5日强势")
        elif pct_5d > 0:
            sent_score += 10
        elif pct_5d < -3:
            sent_score -= 25
            sent_reasons.append("5日弱势")
        elif pct_5d < 0:
            sent_score -= 10

    sent_score = max(0, min(100, sent_score))
    result["score_sentiment"] = round(sent_score, 1)

    # 6. 综合评分
    total = (
        tech_score * w.technical / 100
        + fund_score * w.fundamental / 100
        + val_score * w.valuation / 100
        + sent_score * w.sentiment / 100
    )
    result["score_total"] = round(total, 1)

    # 7. 信号判断
    all_reasons = tech_reasons + fund_reasons + val_reasons + sent_reasons
    result["reasons"] = all_reasons
    if total >= 70:
        result["signal"] = "关注买入"
    elif total >= 55:
        result["signal"] = "可关注"
    elif total >= 40:
        result["signal"] = "观望"
    else:
        result["signal"] = "回避"

    return result


# ==================== 批量分析观察池 ====================

def analyze_pool(cfg: Optional[AppConfig] = None) -> List[Dict[str, Any]]:
    """分析 config.yaml 中的全部 A股 + 美股观察池。"""
    if cfg is None:
        cfg = AppConfig.load()
    results: List[Dict[str, Any]] = []
    for s in cfg.cn_stocks:
        results.append(analyze_stock(s.code, "cn_stock", s.name, s.sector or "", cfg))
    for s in cfg.us_stocks:
        results.append(analyze_stock(s.code, "us_stock", s.name, s.sector or "", cfg))
    results.sort(key=lambda x: x["score_total"], reverse=True)
    return results


# ==================== 报告格式化 ====================

def format_report(results: List[Dict[str, Any]]) -> str:
    """生成 Telegram 推送格式的股票分析报告。"""
    if not results:
        return "暂无股票分析数据"

    lines = []
    lines.append("📊 【Wisteria 股票分析报告】")
    lines.append(f"分析标的：{len(results)} 只 | 评分模型：技术30%+基本面40%+估值20%+情绪10%")
    lines.append("")

    for i, r in enumerate(results, 1):
        market_tag = "A股" if r["market"] == "cn_stock" else "美股"
        cur_sym = "$" if r["market"] == "us_stock" else "¥"
        price_str = f"{cur_sym}{r['price']:.2f}" if r["price"] is not None else "数据同步中"

        signal_emoji = {"关注买入": "🟢", "可关注": "🟡", "观望": "🟠", "回避": "🔴"}.get(r["signal"], "⚪")

        lines.append(f"{i}. {signal_emoji} {r['name']}({r['code']}) [{market_tag}]")
        lines.append(f"   现价 {price_str} | 综合评分 {r['score_total']:.1f}/100 → {r['signal']}")

        # 指标摘要
        indicators = []
        if r["pe"] is not None:
            indicators.append(f"PE {r['pe']:.1f}")
        if r["pb"] is not None:
            indicators.append(f"PB {r['pb']:.1f}")
        if r["rsi14"] is not None:
            indicators.append(f"RSI {r['rsi14']:.0f}")
        if r["macd_hist"] is not None:
            macd_signal = "金叉" if r["macd_hist"] > 0 else "死叉"
            indicators.append(f"MACD {macd_signal}")
        if r["pct_5d"] is not None:
            indicators.append(f"5D {r['pct_5d']:+.1f}%")
        if r["pct_60d"] is not None:
            indicators.append(f"60D {r['pct_60d']:+.1f}%")
        if indicators:
            lines.append(f"   {' | '.join(indicators)}")

        # 分项评分
        lines.append(
            f"   技术{r['score_technical']:.0f} 基本面{r['score_fundamental']:.0f} "
            f"估值{r['score_valuation']:.0f} 情绪{r['score_sentiment']:.0f}"
        )

        # 关键理由（最多3条）
        if r["reasons"]:
            lines.append(f"   理由：{'、'.join(r['reasons'][:3])}")
        lines.append("")

    # 汇总建议
    buy_candidates = [r for r in results if r["score_total"] >= 70]
    avoid_list = [r for r in results if r["score_total"] < 40]
    lines.append("━━━ 汇总 ━━━")
    if buy_candidates:
        names = "、".join([f"{r['name']}({r['score_total']:.0f})" for r in buy_candidates[:3]])
        lines.append(f"🟢 关注买入：{names}")
    if avoid_list:
        names = "、".join([f"{r['name']}({r['score_total']:.0f})" for r in avoid_list[:3]])
        lines.append(f"🔴 建议回避：{names}")
    if not buy_candidates and not avoid_list:
        lines.append("⚪ 当前无极端信号，维持观望")

    return "\n".join(lines)
