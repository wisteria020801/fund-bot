"""
持仓管理 + 盈亏计算核心模块。

职责：
  - 记录买入/卖出交易，自动更新持仓和现金
  - 实时计算浮动盈亏、已实现盈亏、总资产
  - 支持模拟盘(paper)和实盘(real)两种模式

核心闭环：
  机器人建议 → 你执行买入 → record_buy 记账 → portfolio_summary 算盈亏 → Telegram 推送
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fundbot import db
from fundbot.config import AppConfig
from fundbot.fetch import (
    fetch_cn_stock_price,
    fetch_fund_latest_nav,
    fetch_us_stock_price,
    fetch_usd_cny,
)


# ==================== 模拟盘初始化 ====================

def init_paper_account(name: str = "模拟盘") -> int:
    """创建模拟盘账户，初始资金从 config.yaml 读取。返回 account_id。"""
    cfg = AppConfig.load()
    return db.create_account(
        name=name,
        mode="paper",
        initial_cash=cfg.paper_account.initial_cash,
        currency=cfg.paper_account.currency,
    )


def init_real_account(name: str = "实盘", initial_cash: float = 0.0) -> int:
    """创建实盘账户。"""
    return db.create_account(
        name=name,
        mode="real",
        initial_cash=initial_cash,
        currency="CNY",
    )


def get_or_create_paper_account() -> int:
    """获取已有的模拟盘账户，不存在则创建。"""
    accounts = db.list_accounts()
    for a in accounts:
        if a["mode"] == "paper":
            return a["id"]
    return init_paper_account()


# ==================== 交易记录 ====================

def record_buy(
    account_id: int,
    code: str,
    market: str,
    name: str,
    price: float,
    quantity: float,
    fee: float = 0.0,
    reason: str = "",
    date: Optional[str] = None,
) -> Tuple[int, str]:
    """
    记录买入交易。
    market: 'fund' | 'cn_stock' | 'us_stock'
    返回 (trade_id, 摘要信息)
    """
    account = db.get_account(account_id)
    if not account:
        return 0, "账户不存在"

    amount = price * quantity
    total_cost = amount + fee
    if account["cash"] < total_cost:
        return 0, f"现金不足: 可用 {account['cash']:.2f}，需要 {total_cost:.2f}"

    # 更新持仓
    pos = db.get_position(account_id, code)
    if pos:
        old_qty = pos["quantity"]
        old_avg = pos["avg_cost"]
        new_qty = old_qty + quantity
        new_avg = (old_qty * old_avg + amount + fee) / new_qty if new_qty else old_avg
    else:
        new_qty = quantity
        new_avg = (amount + fee) / quantity if quantity else 0.0

    currency = "USD" if market == "us_stock" else "CNY"
    db.upsert_position({
        "account_id": account_id,
        "code": code,
        "market": market,
        "name": name,
        "quantity": new_qty,
        "avg_cost": new_avg,
        "currency": currency,
        "updated_at": datetime.utcnow().isoformat(),
    })

    # 记录交易
    trade_id = db.insert_trade({
        "account_id": account_id,
        "date": date or datetime.utcnow().date().isoformat(),
        "code": code,
        "market": market,
        "name": name,
        "action": "buy",
        "price": price,
        "quantity": quantity,
        "amount": amount,
        "fee": fee,
        "currency": currency,
        "reason": reason,
        "realized_pnl": None,
        "created_at": datetime.utcnow().isoformat(),
    })

    # 扣减现金
    db.update_account_cash(account_id, account["cash"] - total_cost)

    summary = f"买入 {name}({code}) {quantity}@{price} = {amount:.2f}{currency} 费{fee:.2f} | 持仓{new_qty}@{new_avg:.4f}"
    return trade_id, summary


def record_sell(
    account_id: int,
    code: str,
    price: float,
    quantity: float,
    fee: float = 0.0,
    reason: str = "",
    date: Optional[str] = None,
) -> Tuple[int, str]:
    """
    记录卖出交易，自动计算已实现盈亏。
    返回 (trade_id, 摘要信息)
    """
    account = db.get_account(account_id)
    if not account:
        return 0, "账户不存在"

    pos = db.get_position(account_id, code)
    if not pos or pos["quantity"] < quantity:
        hold = pos["quantity"] if pos else 0
        return 0, f"持仓不足: 持有 {hold}，卖出 {quantity}"

    amount = price * quantity
    net_proceeds = amount - fee
    avg_cost = pos["avg_cost"]
    realized_native = (price - avg_cost) * quantity - fee  # 原币种盈亏

    # 美股卖出折算为 CNY 记账
    fx = 1.0
    if pos["currency"] == "USD":
        fx = fetch_usd_cny() or 7.25
    realized_cny = realized_native * fx  # 统一以 CNY 存储 realized_pnl

    # 更新持仓
    new_qty = pos["quantity"] - quantity
    if new_qty <= 1e-9:
        db.delete_position(account_id, code)
        new_qty = 0
    else:
        db.upsert_position({
            "account_id": account_id,
            "code": code,
            "market": pos["market"],
            "name": pos["name"],
            "quantity": new_qty,
            "avg_cost": avg_cost,
            "currency": pos["currency"],
            "updated_at": datetime.utcnow().isoformat(),
        })

    trade_id = db.insert_trade({
        "account_id": account_id,
        "date": date or datetime.utcnow().date().isoformat(),
        "code": code,
        "market": pos["market"],
        "name": pos["name"],
        "action": "sell",
        "price": price,
        "quantity": quantity,
        "amount": amount,
        "fee": fee,
        "currency": pos["currency"],
        "reason": reason,
        "realized_pnl": realized_cny,  # 以 CNY 存储，方便汇总
        "created_at": datetime.utcnow().isoformat(),
    })

    # 增加现金（统一折算为 CNY）
    cash_delta = net_proceeds * fx
    db.update_account_cash(account_id, account["cash"] + cash_delta)

    pnl_str = f"+{realized_cny:.2f}" if realized_cny >= 0 else f"{realized_cny:.2f}"
    summary = f"卖出 {pos['name']}({code}) {quantity}@{price} = {amount:.2f}{pos['currency']} | 已实现盈亏 {pnl_str}元 | 剩余持仓 {new_qty}"
    return trade_id, summary


# ==================== 实时价格获取 ====================

def get_current_price(code: str, market: str) -> Optional[float]:
    """根据市场类型获取最新价格。"""
    if market == "fund":
        return fetch_fund_latest_nav(code)
    elif market == "cn_stock":
        return fetch_cn_stock_price(code)
    elif market == "us_stock":
        return fetch_us_stock_price(code)
    return None


# ==================== 盈亏计算 ====================

def portfolio_summary(account_id: int) -> Dict[str, Any]:
    """
    计算账户完整盈亏快照。

    返回:
    {
        account, positions: [{..., current_price, market_value_cny, cost_cny, unrealized_pnl, roi}],
        cash, total_positions_value, total_cost,
        unrealized_pnl, realized_pnl, total_value, total_pnl, roi, fx_rate
    }
    """
    account = db.get_account(account_id)
    if not account:
        return {"error": "账户不存在"}

    positions = db.get_positions(account_id)
    fx_rate = fetch_usd_cny() or 7.25

    enriched = []
    total_positions_value = 0.0
    total_cost = 0.0
    total_unrealized = 0.0

    for pos in positions:
        current_price = get_current_price(pos["code"], pos["market"])
        qty = pos["quantity"]
        avg_cost = pos["avg_cost"]

        if current_price is None:
            current_price = avg_cost  # 获取失败时用成本价兜底

        # 原币种市值和成本
        market_value_native = current_price * qty
        cost_native = avg_cost * qty

        # 折算人民币
        if pos["currency"] == "USD":
            market_value_cny = market_value_native * fx_rate
            cost_cny = cost_native * fx_rate
        else:
            market_value_cny = market_value_native
            cost_cny = cost_native

        unrealized = market_value_cny - cost_cny
        roi = (unrealized / cost_cny * 100.0) if cost_cny else 0.0

        enriched.append({
            **pos,
            "current_price": current_price,
            "market_value_cny": round(market_value_cny, 2),
            "cost_cny": round(cost_cny, 2),
            "unrealized_pnl": round(unrealized, 2),
            "roi": round(roi, 2),
            "fx_rate": fx_rate if pos["currency"] == "USD" else None,
        })
        total_positions_value += market_value_cny
        total_cost += cost_cny
        total_unrealized += unrealized

    # realized_pnl 在 record_sell 时已折算为 CNY 存储，可直接汇总
    realized = db.get_realized_pnl(account_id)

    cash = account["cash"]
    total_value = cash + total_positions_value
    initial = account["initial_cash"]
    total_pnl = total_value - initial
    total_roi = (total_pnl / initial * 100.0) if initial else 0.0
    unrealized_roi = (total_unrealized / total_cost * 100.0) if total_cost else 0.0

    return {
        "account": account,
        "positions": enriched,
        "cash": round(cash, 2),
        "total_positions_value": round(total_positions_value, 2),
        "total_cost": round(total_cost, 2),
        "unrealized_pnl": round(total_unrealized, 2),
        "unrealized_roi": round(unrealized_roi, 2),
        "realized_pnl": round(realized, 2),
        "total_value": round(total_value, 2),
        "total_pnl": round(total_pnl, 2),
        "total_roi": round(total_roi, 2),
        "fx_rate": fx_rate,
        "initial_cash": initial,
    }


def format_summary_text(account_id: int) -> str:
    """生成 Telegram 推送格式的持仓报告。"""
    s = portfolio_summary(account_id)
    if "error" in s:
        return s["error"]

    acc = s["account"]
    lines = []
    mode_tag = "模拟盘" if acc["mode"] == "paper" else "实盘"
    lines.append(f"💼 【Wisteria 投资组合 - {mode_tag} #{acc['id']}】")
    lines.append(f"📊 总资产：{s['total_value']:.2f} 元（初始 {s['initial_cash']:.2f}）")
    lines.append(f"💰 现金：{s['cash']:.2f} 元")
    lines.append(f"📈 持仓市值：{s['total_positions_value']:.2f} 元")

    if s["positions"]:
        lines.append("")
        lines.append("📋 持仓明细：")
        for p in s["positions"]:
            market_tag = {"fund": "基金", "cn_stock": "A股", "us_stock": "美股"}.get(p["market"], "")
            cur_sym = "$" if p["currency"] == "USD" else ""
            pnl_str = f"+{p['unrealized_pnl']:.2f}" if p["unrealized_pnl"] >= 0 else f"{p['unrealized_pnl']:.2f}"
            roi_str = f"+{p['roi']:.2f}%" if p["roi"] >= 0 else f"{p['roi']:.2f}%"
            lines.append(f"• {p['name']}({p['code']}) [{market_tag}]")
            lines.append(f"  数量:{p['quantity']:.2f} 成本:{cur_sym}{p['avg_cost']:.4f} 现价:{cur_sym}{p['current_price']:.4f}")
            if p.get("fx_rate"):
                lines.append(f"  市值:¥{p['market_value_cny']:.2f}(汇率{p['fx_rate']:.2f}) 浮盈:{pnl_str}元({roi_str})")
            else:
                lines.append(f"  市值:¥{p['market_value_cny']:.2f} 浮盈:{pnl_str}元({roi_str})")
    else:
        lines.append("（当前无持仓）")

    lines.append("")
    lines.append("📉 盈亏总览：")
    lines.append(f"  持仓成本: {s['total_cost']:.2f} | 浮盈: {s['unrealized_pnl']:+.2f} ({s['unrealized_roi']:+.2f}%)")
    lines.append(f"  已实现盈亏: {s['realized_pnl']:+.2f}")
    pnl_color = "📈" if s["total_pnl"] >= 0 else "📉"
    lines.append(f"  {pnl_color} 总盈亏: {s['total_pnl']:+.2f} 元 ({s['total_roi']:+.2f}%)")

    return "\n".join(lines)


# ==================== 持仓重建（安全网） ====================

def rebuild_position(account_id: int, code: str) -> Dict[str, Any]:
    """从全部交易记录重建某标的的持仓（数据修复用）。"""
    trades = db.get_trades_by_code(account_id, code)
    qty = 0.0
    cost_basis = 0.0  # 总成本（含手续费）
    market = "fund"
    name = code
    currency = "CNY"

    for t in trades:
        if t["action"] == "buy":
            qty += t["quantity"]
            cost_basis += t["amount"] + (t["fee"] or 0)
            market = t["market"]
            name = t["name"] or name
            currency = t["currency"]
        elif t["action"] == "sell":
            qty -= t["quantity"]
            cost_basis -= (t["amount"] - (t["fee"] or 0))  # 简化：按卖出金额扣减成本

    avg_cost = (cost_basis / qty) if qty > 0 else 0.0

    if qty > 1e-9:
        db.upsert_position({
            "account_id": account_id,
            "code": code,
            "market": market,
            "name": name,
            "quantity": qty,
            "avg_cost": avg_cost,
            "currency": currency,
            "updated_at": datetime.utcnow().isoformat(),
        })
        return {"code": code, "quantity": qty, "avg_cost": avg_cost, "status": "rebuilt"}
    else:
        db.delete_position(account_id, code)
        return {"code": code, "quantity": 0, "avg_cost": 0, "status": "cleared"}
