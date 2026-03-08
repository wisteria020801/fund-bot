from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple
from datetime import datetime

DB_PATH = Path("fund_data.db")


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        """
        create table if not exists funds(
            code text primary key,
            name text,
            latest_nav real,
            change_1d real,
            change_7d real,
            change_30d real,
            top_holdings text,
            max_drawdown real,
            fee_rate real,
            aum real,
            updated_at text
        )
        """
    )
    cur.execute(
        """
        create table if not exists scores(
            code text,
            date text,
            total real,
            rank30 real,
            rank90 real,
            penalty_drawdown real,
            score_aum real,
            penalty_fee real,
            primary key(code, date)
        )
        """
    )
    cur.execute(
        """
        create table if not exists premarket(
            symbol text,
            date text,
            prechange real,
            fetched_at text,
            primary key(symbol, date)
        )
        """
    )
    cur.execute(
        """
        create table if not exists messages(
            id integer primary key autoincrement,
            run_type text,
            sent_at text,
            content text
        )
        """
    )
    conn.commit()
    conn.close()


def upsert_fund(data: Dict[str, Any]) -> None:
    conn = connect()
    cur = conn.cursor()
    cols = [
        "code",
        "name",
        "latest_nav",
        "change_1d",
        "change_7d",
        "change_30d",
        "top_holdings",
        "max_drawdown",
        "fee_rate",
        "aum",
        "updated_at",
    ]
    placeholders = ",".join(["?"] * len(cols))
    update_cols = ",".join([f"{c}=excluded.{c}" for c in cols[1:]])
    sql = f"""
        insert into funds({",".join(cols)})
        values({placeholders})
        on conflict(code) do update set {update_cols}
    """
    values = [data.get(c) for c in cols]
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def bulk_upsert_premarket(rows: Iterable[Tuple[str, str, float, str]]) -> None:
    conn = connect()
    cur = conn.cursor()
    cur.executemany(
        """
        insert into premarket(symbol, date, prechange, fetched_at)
        values(?,?,?,?)
        on conflict(symbol, date) do update set prechange=excluded.prechange, fetched_at=excluded.fetched_at
        """,
        list(rows),
    )
    conn.commit()
    conn.close()


def upsert_score(row: Dict[str, Any]) -> None:
    conn = connect()
    cur = conn.cursor()
    cols = [
        "code",
        "date",
        "total",
        "rank30",
        "rank90",
        "penalty_drawdown",
        "score_aum",
        "penalty_fee",
    ]
    placeholders = ",".join(["?"] * len(cols))
    update_cols = ",".join([f"{c}=excluded.{c}" for c in cols[2:]])
    sql = f"""
        insert into scores({",".join(cols)})
        values({placeholders})
        on conflict(code, date) do update set {update_cols}
    """
    values = [row.get(c) for c in cols]
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def log_message(run_type: str, content: str) -> None:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "insert into messages(run_type, sent_at, content) values(?,?,?)",
        (run_type, datetime.utcnow().isoformat(), content),
    )
    conn.commit()
    conn.close()
