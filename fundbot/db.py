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
    cur.execute(
        """
        create table if not exists dca_logs(
            date text primary key,
            bias real,
            dgs10 real,
            rsi14 real,
            dca_mult real,
            dca_amount real,
            pct real,
            avg_score real,
            suggest_lump integer,
            note text,
            ts text
        )
        """
    )
    cur.execute(
        """
        create table if not exists accounts(
            id integer primary key autoincrement,
            name text not null,
            mode text not null,
            currency text default 'CNY',
            initial_cash real not null,
            cash real not null,
            created_at text,
            updated_at text
        )
        """
    )
    cur.execute(
        """
        create table if not exists trades(
            id integer primary key autoincrement,
            account_id integer not null,
            date text not null,
            code text not null,
            market text not null,
            name text,
            action text not null,
            price real not null,
            quantity real not null,
            amount real not null,
            fee real default 0,
            currency text default 'CNY',
            reason text,
            realized_pnl real,
            created_at text
        )
        """
    )
    cur.execute(
        """
        create table if not exists positions(
            account_id integer not null,
            code text not null,
            market text not null,
            name text,
            quantity real not null,
            avg_cost real not null,
            currency text default 'CNY',
            updated_at text,
            primary key(account_id, code)
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


def latest_scores_date() -> str | None:
    conn = connect()
    cur = conn.cursor()
    cur.execute("select date from scores order by date desc limit 1")
    row = cur.fetchone()
    conn.close()
    return row["date"] if row else None


def scores_by_date(date: str) -> list[dict]:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        """
        select s.code, s.date, s.total, s.rank30, s.rank90, s.penalty_drawdown, s.score_aum, s.penalty_fee, f.name
        from scores s
        left join funds f on f.code = s.code
        where s.date = ?
        order by s.total desc
        """,
        (date,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def all_funds_snapshot() -> dict[str, dict]:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        """
        select code, name, latest_nav, change_1d, change_7d, change_30d, max_drawdown, fee_rate, aum, updated_at
        from funds
        """
    )
    rows = cur.fetchall()
    conn.close()
    out: dict[str, dict] = {}
    for r in rows:
        out[r["code"]] = dict(r)
    return out


def upsert_dca_log(row: Dict[str, Any]) -> None:
    conn = connect()
    cur = conn.cursor()
    cols = ["date", "bias", "dgs10", "rsi14", "dca_mult", "dca_amount", "pct", "avg_score", "suggest_lump", "note", "ts"]
    placeholders = ",".join(["?"] * len(cols))
    update_cols = ",".join([f"{c}=excluded.{c}" for c in cols[1:]])
    sql = f"""
        insert into dca_logs({",".join(cols)})
        values({placeholders})
        on conflict(date) do update set {update_cols}
    """
    values = [row.get(c) for c in cols]
    cur.execute(sql, values)
    conn.commit()
    conn.close()


def export_dca_csv(path: str) -> None:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "select date,bias,dgs10,rsi14,dca_mult,dca_amount,pct,avg_score,suggest_lump,note,ts from dca_logs order by date"
    )
    rows = cur.fetchall()
    conn.close()
    import csv

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "bias", "dgs10", "rsi14", "dca_mult", "dca_amount", "pct", "avg_score", "suggest_lump", "note", "ts"])
        for r in rows:
            w.writerow([r["date"], r["bias"], r["dgs10"], r["rsi14"], r["dca_mult"], r["dca_amount"], r["pct"], r["avg_score"], r["suggest_lump"], r["note"], r["ts"]])


def latest_dca_logs(limit: int = 2) -> list[dict]:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "select date,bias,dgs10,rsi14,dca_mult,dca_amount,pct,avg_score,suggest_lump,note,ts from dca_logs order by date desc limit ?",
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==================== 账户管理 ====================

def create_account(name: str, mode: str, initial_cash: float, currency: str = "CNY") -> int:
    """创建账户，mode: 'paper'(模拟) / 'real'(实盘)。返回 account_id。"""
    conn = connect()
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute(
        "insert into accounts(name, mode, currency, initial_cash, cash, created_at, updated_at) values(?,?,?,?,?,?,?)",
        (name, mode, currency, initial_cash, initial_cash, now, now),
    )
    conn.commit()
    aid = cur.lastrowid
    conn.close()
    return aid


def get_account(account_id: int) -> dict | None:
    conn = connect()
    cur = conn.cursor()
    cur.execute("select * from accounts where id=?", (account_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def list_accounts() -> list[dict]:
    conn = connect()
    cur = conn.cursor()
    cur.execute("select * from accounts order by id")
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_account_cash(account_id: int, cash: float) -> None:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "update accounts set cash=?, updated_at=? where id=?",
        (cash, datetime.utcnow().isoformat(), account_id),
    )
    conn.commit()
    conn.close()


# ==================== 交易记录 ====================

def insert_trade(row: Dict[str, Any]) -> int:
    """插入一条交易记录，返回 trade id。"""
    conn = connect()
    cur = conn.cursor()
    cols = [
        "account_id", "date", "code", "market", "name", "action",
        "price", "quantity", "amount", "fee", "currency", "reason",
        "realized_pnl", "created_at",
    ]
    vals = [row.get(c) for c in cols]
    placeholders = ",".join(["?"] * len(cols))
    cur.execute(f"insert into trades({','.join(cols)}) values({placeholders})", vals)
    conn.commit()
    tid = cur.lastrowid
    conn.close()
    return tid


def get_trades(account_id: int, limit: int = 100) -> list[dict]:
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "select * from trades where account_id=? order by date desc, id desc limit ?",
        (account_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_trades_by_code(account_id: int, code: str) -> list[dict]:
    """获取某只标的的全部交易记录（按时间正序），用于重建持仓。"""
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "select * from trades where account_id=? and code=? order by date asc, id asc",
        (account_id, code),
    )
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ==================== 持仓管理 ====================

def upsert_position(row: Dict[str, Any]) -> None:
    conn = connect()
    cur = conn.cursor()
    cols = ["account_id", "code", "market", "name", "quantity", "avg_cost", "currency", "updated_at"]
    vals = [row.get(c) for c in cols]
    update_cols = ",".join([f"{c}=excluded.{c}" for c in cols[3:]])
    placeholders = ",".join(["?"] * len(cols))
    cur.execute(
        f"insert into positions({','.join(cols)}) values({placeholders}) on conflict(account_id, code) do update set {update_cols}",
        vals,
    )
    conn.commit()
    conn.close()


def get_position(account_id: int, code: str) -> dict | None:
    conn = connect()
    cur = conn.cursor()
    cur.execute("select * from positions where account_id=? and code=?", (account_id, code))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_positions(account_id: int) -> list[dict]:
    conn = connect()
    cur = conn.cursor()
    cur.execute("select * from positions where account_id=? and quantity > 0 order by code", (account_id,))
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_position(account_id: int, code: str) -> None:
    conn = connect()
    cur = conn.cursor()
    cur.execute("delete from positions where account_id=? and code=?", (account_id, code))
    conn.commit()
    conn.close()


def get_realized_pnl(account_id: int) -> float:
    """账户已实现盈亏合计（从卖出交易记录汇总）。"""
    conn = connect()
    cur = conn.cursor()
    cur.execute(
        "select coalesce(sum(realized_pnl), 0) as total from trades where account_id=? and action='sell'",
        (account_id,),
    )
    row = cur.fetchone()
    conn.close()
    return float(row["total"]) if row else 0.0
