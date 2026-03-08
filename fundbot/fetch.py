from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math

import pandas as pd


def _safe_float(v) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return None
        return float(v)
    except Exception:
        return None


def fetch_fund_nav_series(code: str, days: int = 365) -> Optional[pd.DataFrame]:
    try:
        import akshare as ak
    except Exception:
        return None
    try:
        end = datetime.today().date()
        start = end - timedelta(days=days + 5)
        df = ak.fund_em_open_fund_info(fund=code, indicator="单位净值走势")
        df = df.rename(columns={"净值日期": "date", "单位净值": "nav"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] >= start]
        df = df.sort_values("date")
        return df[["date", "nav"]]
    except Exception:
        return None


def calc_returns(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    if df is None or df.empty:
        return {"r1": None, "r7": None, "r30": None, "r90": None}
    df = df.sort_values("date")
    latest = df.iloc[-1]["nav"]
    out: Dict[str, Optional[float]] = {}
    for k, window in [("r1", 1), ("r7", 7), ("r30", 30), ("r90", 90)]:
        past_df = df[df["date"] <= df.iloc[-1]["date"] - timedelta(days=window)]
        if past_df.empty:
            out[k] = None
        else:
            past = past_df.iloc[-1]["nav"]
            out[k] = (latest - past) / past * 100.0 if past else None
    return out


def max_drawdown(df: pd.DataFrame) -> Optional[float]:
    if df is None or df.empty:
        return None
    series = df["nav"].astype(float).values
    peak = series[0]
    mdd = 0.0
    for x in series:
        if x > peak:
            peak = x
        dd = (peak - x) / peak if peak else 0.0
        if dd > mdd:
            mdd = dd
    return mdd * 100.0


def fetch_fund_meta(code: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        import akshare as ak
    except Exception:
        return None, None
    fee = None
    aum = None
    try:
        info = ak.fund_em_open_fund_info(fund=code, indicator="基金档案")
        if isinstance(info, pd.DataFrame) and not info.empty:
            for _, r in info.iterrows():
                k = str(r.get("项目", ""))
                v = str(r.get("内容", ""))
                if "管理费率" in k:
                    try:
                        fee = float(v.strip("%")) if v.endswith("%") else float(v)
                    except Exception:
                        pass
                if "资产规模" in k or "基金规模" in k:
                    try:
                        aum = float(v.replace(",", "").replace("亿", "")) * 1e8 if "亿" in v else float(v)
                    except Exception:
                        pass
    except Exception:
        pass
    return fee, aum


def fetch_top_holdings_codes(code: str) -> List[str]:
    try:
        import akshare as ak
    except Exception:
        return []
    try:
        df = ak.fund_em_portfolio_holdings(fund=code)
        if isinstance(df, pd.DataFrame) and not df.empty:
            col = "持仓股票代码" if "持仓股票代码" in df.columns else "股票代码" if "股票代码" in df.columns else None
            if col:
                return [str(x) for x in df[col].head(10).tolist()]
    except Exception:
        return []
    return []


def fetch_premarket_change(symbols: List[str]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    try:
        import yfinance as yf
    except Exception:
        return {s: None for s in symbols}
    for s in symbols:
        try:
            t = yf.Ticker(s)
            info = {}
            try:
                info = t.fast_info or {}
            except Exception:
                pass
            val = None
            if info and "preMarketChangePercent" in info:
                val = _safe_float(info.get("preMarketChangePercent"))
            if val is None:
                data = t.history(period="1d", prepost=True)
                if "Close" in data.columns and "Open" in data.columns and len(data) > 0:
                    close = float(data["Close"].iloc[-1])
                    pre = float(data["Open"].iloc[-1])
                    if close:
                        val = (pre - close) / close * 100.0
            out[s] = val
        except Exception:
            out[s] = None
    return out
