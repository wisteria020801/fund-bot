from __future__ import annotations
import sys
from datetime import datetime
from pathlib import Path
from fundbot.config import AppConfig
from fundbot import db
from fundbot.fetch import fetch_premarket_change
from fundbot.notify import send_telegram_message
from fundbot.ai import summarize_with_llm, fallback_summary
from fundbot.config import to_json


def main() -> int:
    db.init_db()
    cfg = AppConfig.load()
    symbols = cfg.us_tickers
    changes = fetch_premarket_change(symbols)
    now = datetime.utcnow().isoformat()
    rows = []
    for s in symbols:
        v = changes.get(s)
        rows.append((s, datetime.utcnow().date().isoformat(), None if v is None else float(v), now))
    db.bulk_upsert_premarket(rows)
    ranked = sorted([(k, v if v is not None else -9999) for k, v in changes.items()], key=lambda x: x[1], reverse=True)
    top = [f"{s}: {v:.2f}%" for s, v in ranked if v != -9999][:3]
    worst = [f"{s}: {v:.2f}%" for s, v in ranked if v != -9999][-3:]
    payload = {
        "premarket": {k: v for k, v in changes.items()},
        "top": [],
        "bottom": [],
        "ts": now,
        "type": "premarket",
    }
    llm = summarize_with_llm(payload) or fallback_summary(payload)
    lines = []
    lines.append("📊 【Wisteria Fund Bot - 盘前播报】")
    lines.append(f"🤖 AI 核心判断：{llm}")
    if top:
        lines.append("📈 盘前较强：")
        lines.extend([f"• {x}" for x in top])
    if worst:
        lines.append("📉 盘前较弱：")
        lines.extend([f"• {x}" for x in worst])
    text = "\n".join(lines)
    send_telegram_message(text)
    db.log_message("premarket", text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
