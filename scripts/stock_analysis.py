"""
股票分析 CLI 工具。

用法示例：
  # 分析全部观察池（A股 + 美股）
  python scripts/stock_analysis.py

  # 分析单只 A股
  python scripts/stock_analysis.py --code 600519 --market cn_stock --name "贵州茅台"

  # 分析单只美股
  python scripts/stock_analysis.py --code NVDA --market us_stock --name "英伟达"

  # 分析并推送到 Telegram
  python scripts/stock_analysis.py --push
"""
from __future__ import annotations

import argparse
import sys
import os

from fundbot.config import AppConfig
from fundbot.stocks import analyze_pool, analyze_stock, format_report
from fundbot.notify import send_telegram_message


def main():
    parser = argparse.ArgumentParser(description="Wisteria Fund Bot - 股票分析")
    parser.add_argument("--code", default=None, help="单只股票代码（不填则分析全部观察池）")
    parser.add_argument("--market", default=None, choices=["cn_stock", "us_stock"], help="市场类型")
    parser.add_argument("--name", default="", help="股票名称")
    parser.add_argument("--sector", default="", help="所属板块")
    parser.add_argument("--push", action="store_true", help="推送到 Telegram")
    args = parser.parse_args()

    cfg = AppConfig.load()

    if args.code:
        if not args.market:
            print("ERROR: --code 需要同时指定 --market", file=sys.stderr)
            sys.exit(1)
        result = analyze_stock(args.code, args.market, args.name, args.sector, cfg)
        results = [result]
    else:
        print("正在分析观察池，请稍候...")
        results = analyze_pool(cfg)

    text = format_report(results)
    print(text)

    if args.push:
        ok = send_telegram_message(text)
        if ok:
            print("\n✅ 已推送到 Telegram")
        else:
            print("\n⚠️ Telegram 推送失败（请检查 TELEGRAM_TOKEN / TELEGRAM_CHAT_ID）", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
