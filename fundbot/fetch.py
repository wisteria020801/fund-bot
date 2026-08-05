from __future__ import annotations
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import math
import time

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

        df = None
        for i in range(3):
            try:
                df = ak.fund_em_open_fund_info(fund=code, indicator="单位净值走势")
                if isinstance(df, pd.DataFrame) and not df.empty:
                    break
            except Exception:
                df = None
            time.sleep(1.2 * (i + 1))
        if df is None or df.empty:
            return None
        df = df.rename(columns={"净值日期": "date", "单位净值": "nav"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["nav"] = pd.to_numeric(df["nav"], errors="coerce")
        df = df.dropna(subset=["nav"])
        df = df[df["nav"] > 0]
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
    for i in range(3):
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
            pass
        time.sleep(1.0 * (i + 1))
    return None


def _stooq_history_ndx(period_days: int = 400) -> Optional[pd.DataFrame]:
    # Stooq NDX daily CSV: https://stooq.com/q/d/l/?s=^ndx&i=d
    url = "https://stooq.com/q/d/l/?s=^ndx&i=d"
    for i in range(3):
        try:
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
            pass
        time.sleep(1.2 * (i + 1))
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
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
    for i in range(3):
        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            from io import StringIO

            df = pd.read_csv(StringIO(r.text))
            if "DGS10" in df.columns and not df.empty:
                for v in reversed(df["DGS10"].tolist()):
                    try:
                        val = float(v)
                        return val
                    except Exception:
                        continue
        except Exception:
            pass
        time.sleep(1.2 * (i + 1))
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
    info = None
    for i in range(3):
        try:
            info = ak.fund_em_open_fund_info(fund=code, indicator="基金档案")
            if isinstance(info, pd.DataFrame) and not info.empty:
                break
        except Exception:
            info = None
        time.sleep(1.2 * (i + 1))
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
    return fee, aum


def fetch_top_holdings_codes(code: str) -> List[str]:
    try:
        import akshare as ak
    except Exception:
        return []
    df = None
    for i in range(3):
        try:
            df = ak.fund_em_portfolio_holdings(fund=code)
            if isinstance(df, pd.DataFrame) and not df.empty:
                break
        except Exception:
            df = None
        time.sleep(1.2 * (i + 1))
    if isinstance(df, pd.DataFrame) and not df.empty:
        col = "持仓股票代码" if "持仓股票代码" in df.columns else "股票代码" if "股票代码" in df.columns else None
        if col:
            return [str(x) for x in df[col].head(10).tolist()]
    return []


def yf_live_pct_change(symbol: str) -> Optional[float]:
    try:
        import yfinance as yf
    except Exception:
        return None
    for i in range(3):
        try:
            t = yf.Ticker(symbol)
            info = {}
            try:
                info = t.fast_info or {}
            except Exception:
                info = {}
            last = _safe_float(info.get("lastPrice") if isinstance(info, dict) else None)
            prev = _safe_float(info.get("previousClose") if isinstance(info, dict) else None)
            if last is not None and prev:
                return (last - prev) / prev * 100.0
        except Exception:
            pass
        time.sleep(0.8 * (i + 1))
    return None


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


def fetch_cn_stock_price(code: str) -> Optional[float]:
    """获取A股个股实时价格（akshare）。code 如 '600519' / '000001'。"""
    try:
        import akshare as ak
    except Exception:
        return None
    try:
        df = ak.stock_zh_a_spot_em()
        if isinstance(df, pd.DataFrame) and not df.empty:
            col_code = "代码" if "代码" in df.columns else None
            col_price = "最新价" if "最新价" in df.columns else None
            if col_code and col_price:
                row = df[df[col_code].astype(str).str.strip() == code.strip()]
                if not row.empty:
                    return _safe_float(row.iloc[0][col_price])
    except Exception:
        pass
    return None


def fetch_us_stock_price(symbol: str) -> Optional[float]:
    """获取美股个股实时价格（yfinance fast_info.lastPrice）。"""
    try:
        import yfinance as yf
    except Exception:
        return None
    for i in range(3):
        try:
            t = yf.Ticker(symbol)
            info = t.fast_info or {}
            last = _safe_float(info.get("lastPrice") if isinstance(info, dict) else None)
            if last is not None and last > 0:
                return last
            # 兜底：从 history 取最新收盘
            df = t.history(period="2d")
            if isinstance(df, pd.DataFrame) and not df.empty and "Close" in df.columns:
                return _safe_float(df["Close"].iloc[-1])
        except Exception:
            pass
        time.sleep(0.8 * (i + 1))
    return None


def fetch_usd_cny() -> Optional[float]:
    """获取 USD/CNY 实时汇率。"""
    try:
        import yfinance as yf
    except Exception:
        return None
    for i in range(3):
        try:
            df = yf.Ticker("USDCNY=X").history(period="2d")
            if isinstance(df, pd.DataFrame) and not df.empty and "Close" in df.columns:
                return _safe_float(df["Close"].iloc[-1])
        except Exception:
            pass
        time.sleep(0.8 * (i + 1))
    return None


def fetch_fund_latest_nav(code: str) -> Optional[float]:
    """获取基金最新净值（优先 akshare 单位净值，兜底本地 DB 缓存）。"""
    try:
        df = fetch_fund_nav_series(code, 5)
        if df is not None and not df.empty:
            return _safe_float(df.iloc[-1]["nav"])
    except Exception:
        pass
    # 兜底：从本地数据库读取
    try:
        from fundbot import db
        conn = db.connect()
        cur = conn.cursor()
        cur.execute("select latest_nav from funds where code=?", (code,))
        row = cur.fetchone()
        conn.close()
        if row and row["latest_nav"]:
            return _safe_float(row["latest_nav"])
    except Exception:
        pass
    return None


# ==================== 股票技术指标数据 ====================

def fetch_cn_stock_hist(code: str, days: int = 120) -> Optional[pd.DataFrame]:
    """获取A股日K线（akshare），返回 date/close/high/low/volume。"""
    try:
        import akshare as ak
    except Exception:
        return None
    end = datetime.today().date().strftime("%Y%m%d")
    start = (datetime.today() - timedelta(days=days + 10)).strftime("%Y%m%d")
    for i in range(2):
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq")
            if isinstance(df, pd.DataFrame) and not df.empty:
                col_map = {"日期": "date", "收盘": "close", "最高": "high", "最低": "low", "成交量": "volume"}
                df = df.rename(columns=col_map)
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df.sort_values("date").tail(days)
                return df[["date", "close", "high", "low", "volume"]]
        except Exception:
            pass
        time.sleep(1.0 * (i + 1))
    return None


def fetch_us_stock_hist(symbol: str, period: str = "6mo") -> Optional[pd.DataFrame]:
    """获取美股日K线（yfinance），返回 date/close/high/low/volume。"""
    try:
        import yfinance as yf
    except Exception:
        return None
    for i in range(3):
        try:
            df = yf.Ticker(symbol).history(period=period)
            if isinstance(df, pd.DataFrame) and not df.empty:
                df = df.reset_index()
                col_map = {"Date": "date", "Close": "close", "High": "high", "Low": "low", "Volume": "volume"}
                df = df.rename(columns=col_map)
                df["date"] = pd.to_datetime(df["date"]).dt.date
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                df = df.dropna(subset=["close"])
                df = df.sort_values("date")
                return df[["date", "close", "high", "low", "volume"]]
        except Exception:
            pass
        time.sleep(0.8 * (i + 1))
    return None


def fetch_cn_stock_valuation(code: str) -> Tuple[Optional[float], Optional[float]]:
    """获取A股 PE(TTM) 和 PB。优先单只接口，兜底全量行情。返回 (pe, pb)。"""
    try:
        import akshare as ak
    except Exception:
        return None, None

    # 方式1：单只股票基本信息（轻量、快）
    try:
        df = ak.stock_individual_info_em(symbol=code)
        if isinstance(df, pd.DataFrame) and not df.empty:
            pe = None
            pb = None
            for _, row in df.iterrows():
                k = str(row.iloc[0]) if len(row) > 0 else ""
                v = str(row.iloc[1]) if len(row) > 1 else ""
                if "市盈率" in k:
                    pe = _safe_float(v)
                if "市净率" in k:
                    pb = _safe_float(v)
            if pe is not None or pb is not None:
                return pe, pb
    except Exception:
        pass

    # 方式2：全量行情兜底（重，但数据全）
    try:
        df = ak.stock_zh_a_spot_em()
        if isinstance(df, pd.DataFrame) and not df.empty:
            col_code = "代码" if "代码" in df.columns else None
            if col_code:
                row = df[df[col_code].astype(str).str.strip() == code.strip()]
                if not row.empty:
                    pe = _safe_float(row.iloc[0].get("市盈率-动态"))
                    pb = _safe_float(row.iloc[0].get("市净率"))
                    return pe, pb
    except Exception:
        pass
    return None, None


def fetch_us_stock_valuation(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """获取美股 PE(trailing) 和 PB（yfinance）。返回 (pe, pb)。"""
    try:
        import yfinance as yf
    except Exception:
        return None, None
    for i in range(3):
        try:
            info = yf.Ticker(symbol).info
            if isinstance(info, dict):
                pe = _safe_float(info.get("trailingPE"))
                pb = _safe_float(info.get("priceToBook"))
                if pe is not None or pb is not None:
                    return pe, pb
        except Exception:
            pass
        time.sleep(0.8 * (i + 1))
    return None, None


def macd(closes: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    计算 MACD 指标。
    返回 (dif, dea, hist)：快线、慢线、柱状图。
    """
    if not closes or len(closes) < slow + signal:
        return None, None, None

    s = pd.Series(closes, dtype=float)
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = (dif - dea) * 2.0  # A股惯例 ×2

    return (
        _safe_float(dif.iloc[-1]),
        _safe_float(dea.iloc[-1]),
        _safe_float(hist.iloc[-1]),
    )


def bollinger(closes: List[float], window: int = 20, num_std: float = 2.0) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    计算布林带。返回 (upper, middle, lower)。
    """
    if not closes or len(closes) < window:
        return None, None, None
    s = pd.Series(closes[-window:], dtype=float)
    mid = s.mean()
    std = s.std()
    if math.isnan(mid) or math.isnan(std):
        return None, None, None
    upper = mid + num_std * std
    lower = mid - num_std * std
    return _safe_float(upper), _safe_float(mid), _safe_float(lower)
