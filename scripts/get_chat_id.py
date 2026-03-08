from __future__ import annotations
import os
import sys
import requests


def main() -> int:
    token = os.getenv("TELEGRAM_TOKEN", "")
    if not token:
        print("ERROR: 请先设置环境变量 TELEGRAM_TOKEN", file=sys.stderr)
        return 2
    url = f"https://api.telegram.org/bot{token}/getUpdates"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"ERROR: 请求 Telegram 失败: {e}", file=sys.stderr)
        return 3
    result = data.get("result", [])
    if not result:
        print("未获取到任何更新。请确保：\n1) 将 Bot 加入目标群；\n2) 在群内@Bot发送一条消息；\n3) 关闭隐私模式或@时直接对Bot说话；\n然后重试本脚本。")
        return 1
    seen = set()
    print("以下是最近更新中发现的 chat 列表：")
    for upd in result:
        msg = upd.get("message") or upd.get("channel_post") or upd.get("edited_message")
        if not msg:
            continue
        chat = msg.get("chat", {})
        cid = chat.get("id")
        title = chat.get("title") or chat.get("username") or chat.get("first_name") or ""
        if cid in seen:
            continue
        seen.add(cid)
        print(f"- chat_id={cid}  title={title}")
    if not seen:
        print("未解析到 chat_id。请按上述提示在群内与 Bot 产生消息交互后再试。")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
