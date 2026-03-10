from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math

import pandas as pd
import requests


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

def calc_returns_asof(df: pd.DataFrame, asof: datetime.date) -> Dict[str, Optional[float]]:
    if df is None or df.empty:
        return {"r1": None, "r7": None, "r30": None, "r90": None}
    df = df.sort_values("date")
    df2 = df[df["date"] <= asof]
    if df2.empty:
        return {"r1": None, "r7": None, "r30": None, "r90": None}
    latest_nav = df2.iloc[-1]["nav"]
    latest_date = df2.iloc[-1]["date"]
    out: Dict[str, Optional[float]] = {}
    for k, window in [("r1", 1), ("r7", 7), ("r30", 30), ("r90", 90)]:
        past_df = df2[df2["date"] <= latest_date - timedelta(days=window)]
        if past_df.empty:
            out[k] = None
        else:
            past = past_df.iloc[-1]["nav"]
            out[k] = (latest_nav - past) / past * 100.0 if past else None
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


def _yf_history(symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
    try:
        import yfinance as yf
    except Exception:
        return None
    try:
        df = yf.Ticker(symbol).history(period=period)
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.reset_index()
            if "Date" in df.columns:
                df.rename(columns={"Date": "date"}, inplace=True)
            if "Close" in df.columns:
                df.rename(columns={"Close": "close"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            return df[["date", "close"]]
    except Exception:
        return None
    return None


def _stooq_history_ndx(period_days: int = 400) -> Optional[pd.DataFrame]:
    # Stooq NDX daily CSV: https://stooq.com/q/d/l/?s=^ndx&i=d
    try:
        url = "https://stooq.com/q/d/l/?s=^ndx&i=d"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        from io import StringIO

        df = pd.read_csv(StringIO(r.text))
        if not df.empty:
            df.rename(columns={"Date": "date", "Close": "close"}, inplace=True)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df = df.sort_values("date").tail(period_days)
            return df[["date", "close"]]
    except Exception:
        return None
    return None


def ndx_ma_bias(window: int = 250) -> Optional[float]:
    # Try yfinance '^NDX', fallback to Stooq '^ndx'
    df = _yf_history("^NDX", period="2y")
    if df is None or df.empty:
        df = _stooq_history_ndx(400)
    if df is None or df.empty or len(df) < window + 1:
        return None
    closes = df["close"].astype(float)
    ma = closes.rolling(window).mean().iloc[-1]
    if not ma or math.isnan(ma):
        return None
    bias = (closes.iloc[-1] - ma) / ma * 100.0
    return float(bias)


def yf_pct_change(symbol: str) -> Optional[float]:
    # Return latest close vs previous close percentage change
    df = _yf_history(symbol, period="5d")
    if df is None or len(df) < 2:
        return None
    a = float(df["close"].iloc[-2])
    b = float(df["close"].iloc[-1])
    if a:
        return (b - a) / a * 100.0
    return None


def fred_dgs10_latest() -> Optional[float]:
    # 10Y Treasury Yield, daily percent
    try:
        url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        from io import StringIO

        df = pd.read_csv(StringIO(r.text))
        if "DGS10" in df.columns and not df.empty:
            # get last non-NaN
            for v in reversed(df["DGS10"].tolist()):
                try:
                    val = float(v)
                    return val
                except Exception:
                    continue
    except Exception:
        return None
    return None


def rsi(values: List[float], period: int = 14) -> Optional[float]:
    if not values or len(values) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, period + 1):
        delta = values[-i] - values[-i - 1]
        if delta >= 0:
            gains.append(delta)
        else:
            losses.append(-delta)
    avg_gain = sum(gains) / period if gains else 0.0
    avg_loss = sum(losses) / period if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

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
