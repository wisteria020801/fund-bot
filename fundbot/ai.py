from __future__ import annotations
import json
import os
from typing import Dict, List, Optional
import requests


def _env(name: str) -> Optional[str]:
    v = os.getenv(name, "")
    return v if v else None


def summarize_with_llm(payload: Dict) -> Optional[str]:
    provider = (_env("LLM_PROVIDER") or "").lower()
    api_key = _env("LLM_API_KEY")
    if not provider or not api_key:
        return None
    text = (
        "你是一个冷静的量化基金分析师。基于以下传入的数据和宏观新闻，不要夸大，不要臆测，用150字内极简总结今日市场情绪。判断纳指定投策略今日是否需要暂停或加码，并指出如有哪只基金的重仓股出现致命利空。只输出结论。"
    )
    content = json.dumps(payload, ensure_ascii=False)
    try:
        if provider == "deepseek":
            url = "https://api.deepseek.com/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            body = {
                "model": os.getenv("LLM_MODEL", "deepseek-chat"),
                "messages": [{"role": "user", "content": text + "\n" + content}],
                "temperature": 0.2,
                "max_tokens": 256,
            }
            r = requests.post(url, headers=headers, json=body, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data["choices"][0]["message"]["content"].strip()
        if provider == "gemini":
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{os.getenv('LLM_MODEL','gemini-2.5-flash')}:generateContent?key={api_key}"
            headers = {"Content-Type": "application/json"}
            body = {"contents": [{"parts": [{"text": text + "\n" + content}]}]}
            r = requests.post(url, headers=headers, json=body, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception:
        return None
    return None


def fallback_summary(payload: Dict) -> str:
    tops = payload.get("top", [])
    pre = payload.get("premarket", {})
    hint = "情绪平稳"
    if any((v or 0) < -1 for v in pre.values() if v is not None):
        hint = "谨慎偏弱"
    if any((v or 0) > 1 for v in pre.values() if v is not None):
        hint = "偏乐观"
    if tops:
        return f"{hint}。维持纪律化定投，关注榜首：{tops[0].get('name','') or tops[0].get('code','')}。"
    return f"{hint}。按计划执行，暂无明显加减仓信号。"
