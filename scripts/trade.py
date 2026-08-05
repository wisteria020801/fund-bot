"""
投资组合 CLI 工具。

用法示例：
  # 初始化模拟盘（10000元虚拟资金）
  python scripts/trade.py init-paper

  # 买入基金
  python scripts/trade.py buy --account 1 --code 006479 --market fund --name "广发纳指100C" --price 1.0234 --qty 1000 --reason "定投1.5x"

  # 买入美股（价格按美元，自动折算）
  python scripts/trade.py buy --account 1 --code NVDA --market us_stock --name "英伟达" --price 120.50 --qty 2 --reason "AI核心"

  # 买入A股
  python scripts/trade.py buy --account 1 --code 600519 --market cn_stock --name "贵州茅台" --price 1680.00 --qty 0.1 --reason "消费龙头"

  # 卖出
  python scripts/trade.py sell --account 1 --code 006479 --price 1.1000 --qty 500 --reason "止盈"

  # 查看持仓报告
  python scripts/trade.py summary --account 1

  # 查看交易记录
  python scripts/trade.py trades --account 1

  # 查看所有账户
  python scripts/trade.py accounts
"""
from __future__ import annotations

import argparse
import sys

from fundbot import db, portfolio


def cmd_init_paper(args):
    db.init_db()
    aid = portfolio.init_paper_account(args.name)
    acc = db.get_account(aid)
    print(f"✅ 模拟盘创建成功: #{aid} {acc['name']} | 初始资金 {acc['initial_cash']:.2f} {acc['currency']}")


def cmd_init_real(args):
    db.init_db()
    aid = portfolio.init_real_account(args.name, args.cash)
    acc = db.get_account(aid)
    print(f"✅ 实盘账户创建成功: #{aid} {acc['name']} | 初始资金 {acc['initial_cash']:.2f} {acc['currency']}")


def cmd_buy(args):
    db.init_db()
    trade_id, msg = portfolio.record_buy(
        account_id=args.account,
        code=args.code,
        market=args.market,
        name=args.name or args.code,
        price=args.price,
        quantity=args.qty,
        fee=args.fee,
        reason=args.reason or "",
    )
    if trade_id:
        print(f"✅ {msg}")
    else:
        print(f"❌ {msg}", file=sys.stderr)
        sys.exit(1)


def cmd_sell(args):
    db.init_db()
    trade_id, msg = portfolio.record_sell(
        account_id=args.account,
        code=args.code,
        price=args.price,
        quantity=args.qty,
        fee=args.fee,
        reason=args.reason or "",
    )
    if trade_id:
        print(f"✅ {msg}")
    else:
        print(f"❌ {msg}", file=sys.stderr)
        sys.exit(1)


def cmd_summary(args):
    db.init_db()
    print(portfolio.format_summary_text(args.account))


def cmd_positions(args):
    db.init_db()
    positions = db.get_positions(args.account)
    if not positions:
        print("（当前无持仓）")
        return
    print(f"{'标的':<20} {'市场':<8} {'数量':>12} {'均价':>12} {'币种':<6}")
    print("-" * 62)
    for p in positions:
        market_tag = {"fund": "基金", "cn_stock": "A股", "us_stock": "美股"}.get(p["market"], "")
        print(f"{p['name']}({p['code']})".ljust(20) + f" {market_tag:<8} {p['quantity']:>12.2f} {p['avg_cost']:>12.4f} {p['currency']:<6}")


def cmd_trades(args):
    db.init_db()
    trades = db.get_trades(args.account, limit=args.limit)
    if not trades:
        print("（暂无交易记录）")
        return
    print(f"{'日期':<12} {'操作':<6} {'标的':<20} {'价格':>10} {'数量':>10} {'金额':>12} {'盈亏':>10}")
    print("-" * 84)
    for t in trades:
        name = f"{t['name']}({t['code']})"
        pnl = f"{t['realized_pnl']:+.2f}" if t['realized_pnl'] is not None else "-"
        print(f"{t['date']:<12} {t['action']:<6} {name:<20} {t['price']:>10.4f} {t['quantity']:>10.2f} {t['amount']:>12.2f} {pnl:>10}")


def cmd_accounts(args):
    db.init_db()
    accounts = db.list_accounts()
    if not accounts:
        print("（暂无账户，请先运行 init-paper 创建模拟盘）")
        return
    print(f"{'ID':<5} {'名称':<12} {'模式':<8} {'初始资金':>12} {'当前现金':>12} {'币种':<6}")
    print("-" * 58)
    for a in accounts:
        mode = "模拟盘" if a["mode"] == "paper" else "实盘"
        print(f"{a['id']:<5} {a['name']:<12} {mode:<8} {a['initial_cash']:>12.2f} {a['cash']:>12.2f} {a['currency']:<6}")


def main():
    parser = argparse.ArgumentParser(description="Wisteria Fund Bot - 投资组合 CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # init-paper
    p = sub.add_parser("init-paper", help="创建模拟盘账户")
    p.add_argument("--name", default="模拟盘", help="账户名称")
    p.set_defaults(func=cmd_init_paper)

    # init-real
    p = sub.add_parser("init-real", help="创建实盘账户")
    p.add_argument("--name", default="实盘", help="账户名称")
    p.add_argument("--cash", type=float, default=0.0, help="初始现金")
    p.set_defaults(func=cmd_init_real)

    # buy
    p = sub.add_parser("buy", help="记录买入")
    p.add_argument("--account", type=int, required=True, help="账户ID")
    p.add_argument("--code", required=True, help="标的代码")
    p.add_argument("--market", required=True, choices=["fund", "cn_stock", "us_stock"], help="市场类型")
    p.add_argument("--name", default=None, help="标的名称")
    p.add_argument("--price", type=float, required=True, help="买入价格")
    p.add_argument("--qty", type=float, required=True, help="买入数量")
    p.add_argument("--fee", type=float, default=0.0, help="手续费")
    p.add_argument("--reason", default=None, help="买入理由")
    p.set_defaults(func=cmd_buy)

    # sell
    p = sub.add_parser("sell", help="记录卖出")
    p.add_argument("--account", type=int, required=True, help="账户ID")
    p.add_argument("--code", required=True, help="标的代码")
    p.add_argument("--price", type=float, required=True, help="卖出价格")
    p.add_argument("--qty", type=float, required=True, help="卖出数量")
    p.add_argument("--fee", type=float, default=0.0, help="手续费")
    p.add_argument("--reason", default=None, help="卖出理由")
    p.set_defaults(func=cmd_sell)

    # summary
    p = sub.add_parser("summary", help="查看持仓报告")
    p.add_argument("--account", type=int, required=True, help="账户ID")
    p.set_defaults(func=cmd_summary)

    # positions
    p = sub.add_parser("positions", help="查看持仓列表")
    p.add_argument("--account", type=int, required=True, help="账户ID")
    p.set_defaults(func=cmd_positions)

    # trades
    p = sub.add_parser("trades", help="查看交易记录")
    p.add_argument("--account", type=int, required=True, help="账户ID")
    p.add_argument("--limit", type=int, default=50, help="显示条数")
    p.set_defaults(func=cmd_trades)

    # accounts
    p = sub.add_parser("accounts", help="查看所有账户")
    p.set_defaults(func=cmd_accounts)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
