from __future__ import annotations
import os
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from fundbot.notify import send_telegram_message, send_telegram_photo, send_telegram_document


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
    # Ensure all 7 weekdays present to match 7 labels
    pivot = pivot.reindex(columns=list(range(7)))
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
    # Align xtick labels with 7 columns
    ax.set_xticks(range(7))
    ax.set_xticklabels(["一", "二", "三", "四", "五", "六", "日"], rotation=0)
    plt.title(f"DCA Heatmap {start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}")
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()

def weekly_pnl(df_logs: pd.DataFrame, db_path: str, out_csv: str) -> None:
    if df_logs.empty:
        return
    end = df_logs["date"].max()
    start = end - timedelta(days=6)
    week_logs = df_logs[(df_logs["date"] >= start) & (df_logs["date"] <= end)].copy()
    if week_logs.empty:
        return
    invest_sum = float(week_logs["dca_amount"].fillna(0).sum())
    # 读取基金的近7日变动（均值）作为回溯收益率
    conn = sqlite3.connect(db_path)
    try:
        funds = pd.read_sql_query("select code, name, change_7d from funds", conn)
    finally:
        conn.close()
    if funds.empty:
        avg_7d = 0.0
    else:
        avg_7d = float(pd.to_numeric(funds["change_7d"], errors="coerce").dropna().mean() or 0.0)
    est_pnl = round(invest_sum * (avg_7d / 100.0), 2)
    est_value = round(invest_sum + est_pnl, 2)
    out = pd.DataFrame(
        [
            {
                "week_start": start.strftime("%Y-%m-%d"),
                "week_end": end.strftime("%Y-%m-%d"),
                "invest_sum": round(invest_sum, 2),
                "avg_change_7d_percent": round(avg_7d, 2),
                "est_pnl": est_pnl,
                "est_value": est_value,
            }
        ]
    )
    out.to_csv(out_csv, index=False, encoding="utf-8")

def main() -> int:
    db_path = os.getenv("DB_PATH", "fund_data.db")
    out_path = os.getenv("HEATMAP_PATH", "dca_heatmap.png")
    df = load_logs(db_path)
    build_heatmap(df, out_path)
    pnl_csv = os.getenv("PNL_CSV_PATH", "weekly_pnl.csv")
    weekly_pnl(df, db_path, pnl_csv)
    if os.path.exists(pnl_csv):
        try:
            pnl_df = pd.read_csv(pnl_csv)
            if not pnl_df.empty:
                r = pnl_df.iloc[0]
                text = (
                    f"📅 【Wisteria Fund Bot - 周复盘】\n"
                    f"周期：{r['week_start']} ~ {r['week_end']}\n"
                    f"投入合计：{r['invest_sum']:.2f} 元\n"
                    f"估算浮盈：{r['est_pnl']:.2f} 元（均值7日涨跌 {r['avg_change_7d_percent']:.2f}%）\n"
                    f"估算总资产：{r['est_value']:.2f} 元"
                )
                send_telegram_message(text)
        except Exception:
            pass
    if os.path.exists(out_path):
        try:
            send_telegram_photo(out_path, caption="DCA乘数日历热力图")
        except Exception:
            pass
    if os.path.exists(pnl_csv):
        try:
            send_telegram_document(pnl_csv, caption="周盈亏回溯CSV")
        except Exception:
            pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
