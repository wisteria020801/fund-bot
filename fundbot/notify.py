from __future__ import annotations
import os
import requests
from typing import Optional


def format_score_total(score: Optional[float], used_cache: bool) -> str:
    if score is None:
        s = "—"
    else:
        s = str(score)
    return f"{s}(Cache)" if used_cache else s


def send_telegram_message(text: str) -> Optional[str]:
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return None
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        r = requests.post(url, json=payload, timeout=15)
        r.raise_for_status()
        return "ok"
    except Exception:
        return None


def send_telegram_photo(photo_path: str, caption: Optional[str] = None) -> Optional[str]:
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id or not os.path.exists(photo_path):
        return None
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    files = {"photo": open(photo_path, "rb")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    try:
        r = requests.post(url, data=data, files=files, timeout=30)
        r.raise_for_status()
        return "ok"
    except Exception:
        return None


def send_telegram_document(file_path: str, caption: Optional[str] = None) -> Optional[str]:
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id or not os.path.exists(file_path):
        return None
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    files = {"document": open(file_path, "rb")}
    data = {"chat_id": chat_id}
    if caption:
        data["caption"] = caption
        data["parse_mode"] = "HTML"
    try:
        r = requests.post(url, data=data, files=files, timeout=30)
        r.raise_for_status()
        return "ok"
    except Exception:
        return None
