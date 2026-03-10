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


class Fund(BaseModel):
    code: str
    name: Optional[str] = None
    fee_rate: Optional[float] = None
    aum: Optional[float] = None
    role: Optional[str] = None
    watch: Optional[Watch] = None


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
    pool_name: str = "纳指/科技基金池"
    timezone: str = "Asia/Shanghai"
    dca: DCAConfig = DCAConfig()

    @staticmethod
    def load(path: Optional[Path] = None) -> "AppConfig":
        if path is None:
            path = Path("config.yaml")
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            funds = [Fund(**x) for x in data.get("funds", [])]
            us_tickers = data.get("us_tickers", None)
            pool_name = data.get("pool_name", None)
            timezone = data.get("timezone", None)
            dca_dict = data.get("dca", None)
            return AppConfig(
                funds=funds,
                us_tickers=us_tickers if us_tickers else AppConfig().us_tickers,
                pool_name=pool_name if pool_name else AppConfig().pool_name,
                timezone=timezone if timezone else AppConfig().timezone,
                dca=DCAConfig(**dca_dict) if isinstance(dca_dict, dict) else AppConfig().dca,
            )
        return AppConfig()


def env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None and v != "" else default


def to_json(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
