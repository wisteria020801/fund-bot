from __future__ import annotations
import os
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


def load_logs(db_path: str) -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query("select * from dca_logs order by date", conn)
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def build_heatmap(df: pd.DataFrame, out_path: str) -> None:
    if df.empty:
        return
    end = df["date"].max()
    start = end - timedelta(days=55)
    df = df[(df["date"] >= start) & (df["date"] <= end)].copy()
    if df.empty:
        return
    df["weekday"] = pd.to_datetime(df["date"]).dt.weekday
    df["week"] = pd.to_datetime(df["date"]) - pd.to_timedelta(pd.to_datetime(df["date"]).dt.weekday, unit="D")
    pivot = df.pivot_table(index="week", columns="weekday", values="dca_mult", aggfunc="mean")
    weeks = sorted({(start + timedelta(days=7*i)) for i in range(8)})
    weeks = [w for w in weeks if w <= end]
    idx = pd.to_datetime(weeks)
    pivot = pivot.reindex(idx)
    pivot = pivot.sort_index()
    plt.figure(figsize=(10, 3))
    ax = sns.heatmap(pivot, cmap="YlGnBu", vmin=0.5, vmax=2.0, linewidths=0.5, linecolor="white", cbar=True)
    ax.set_xlabel("")
    ax.set_ylabel("")
    ax.set_yticklabels([d.strftime("%m-%d") for d in pivot.index.date], rotation=0)
    ax.set_xticklabels(["一", "二", "三", "四", "五", "六", "日"], rotation=0)
    plt.title(f"DCA Heatmap {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def main() -> int:
    db_path = os.getenv("DB_PATH", "fund_data.db")
    out_path = os.getenv("HEATMAP_PATH", "dca_heatmap.png")
    df = load_logs(db_path)
    build_heatmap(df, out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

