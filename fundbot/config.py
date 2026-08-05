from __future__ import annotations
import os
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
import yaml
from pydantic import BaseModel


class Watch(BaseModel):
    daily_change_alert: Optional[float] = None


class DCAThresholds(BaseModel):
    crash: float = -2.0
    crash_hard: float = -4.0
    bubble: float = 3.0


class DCAConfig(BaseModel):
    base_amount: float = 10.0
    thresholds: DCAThresholds = DCAThresholds()
    macro_brake_threshold: float = 4.5
    macro_brake_factor: float = 0.8


class Fund(BaseModel):
    code: str
    name: Optional[str] = None
    fee_rate: Optional[float] = None
    aum: Optional[float] = None
    role: Optional[str] = None
    watch: Optional[Watch] = None


class Stock(BaseModel):
    code: str
    name: Optional[str] = None
    sector: Optional[str] = None


class PaperAccountConfig(BaseModel):
    initial_cash: float = 10000.0
    currency: str = "CNY"


class StockAnalysisWeights(BaseModel):
    technical: float = 30
    fundamental: float = 40
    valuation: float = 20
    sentiment: float = 10


class ValuationThresholds(BaseModel):
    pe_undervalued: float = 15
    pe_overvalued: float = 40
    pb_undervalued: float = 1.5
    pb_overvalued: float = 6.0


class TechnicalThresholds(BaseModel):
    rsi_oversold: float = 30
    rsi_overbought: float = 70


class StockAnalysisConfig(BaseModel):
    weights: StockAnalysisWeights = StockAnalysisWeights()
    valuation: ValuationThresholds = ValuationThresholds()
    technical: TechnicalThresholds = TechnicalThresholds()


class AppConfig(BaseModel):
    funds: List[Fund] = []
    us_tickers: List[str] = [
        "AAPL",
        "MSFT",
        "NVDA",
        "AMZN",
        "META",
        "GOOGL",
        "AVGO",
        "TSLA",
        "COST",
        "GOOG",
    ]
    cn_stocks: List[Stock] = []
    us_stocks: List[Stock] = []
    pool_name: str = "纳指/科技基金池"
    timezone: str = "Asia/Shanghai"
    dca: DCAConfig = DCAConfig()
    paper_account: PaperAccountConfig = PaperAccountConfig()
    stock_analysis: StockAnalysisConfig = StockAnalysisConfig()

    @staticmethod
    def load(path: Optional[Path] = None) -> "AppConfig":
        if path is None:
            path = Path("config.yaml")
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            funds = [Fund(**x) for x in data.get("funds", [])]
            us_tickers = data.get("us_tickers", None)
            cn_stocks = [Stock(**x) for x in data.get("cn_stocks", [])]
            us_stocks = [Stock(**x) for x in data.get("us_stocks", [])]
            pool_name = data.get("pool_name", None)
            timezone = data.get("timezone", None)
            dca_dict = data.get("dca", None)
            paper_dict = data.get("paper_account", None)
            sa_dict = data.get("stock_analysis", None)
            return AppConfig(
                funds=funds,
                us_tickers=us_tickers if us_tickers else AppConfig().us_tickers,
                cn_stocks=cn_stocks,
                us_stocks=us_stocks,
                pool_name=pool_name if pool_name else AppConfig().pool_name,
                timezone=timezone if timezone else AppConfig().timezone,
                dca=DCAConfig(**dca_dict) if isinstance(dca_dict, dict) else AppConfig().dca,
                paper_account=PaperAccountConfig(**paper_dict) if isinstance(paper_dict, dict) else AppConfig().paper_account,
                stock_analysis=StockAnalysisConfig(**sa_dict) if isinstance(sa_dict, dict) else AppConfig().stock_analysis,
            )
        return AppConfig()


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
