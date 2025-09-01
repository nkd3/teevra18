# C:\teevra18\core\policies.py
def enforce_core_limits(pol: dict) -> list:
    """Return list of violations (empty = all good)."""
    issues = []
    if pol["max_trades_per_day"] > 5:
        issues.append("Max trades/day must be ≤ 5.")
    if pol["sl_max_per_lot"] > 1000.0:
        issues.append("Stop-loss per lot must be ≤ ₹1000.")
    if pol["rr_min"] < 2.0:
        issues.append("Risk:Reward must be ≥ 1:2.")
    return issues

def position_sizing(policies: dict, entry_price: float, sl_distance: float, lot_size: int):
    """
    Simple position sizing based on risk % and SL distance.
    Returns (qty_lots, est_loss_per_lot, total_risk).
    """
    cap = policies["fixed_capital"] if policies["capital_mode"] == "Fixed" else policies.get("dynamic_capital", 150000)
    risk_cash = cap * (policies["risk_per_trade_pct"] / 100.0)
    est_loss_per_lot = sl_distance * lot_size
    if est_loss_per_lot <= 0:
        return 0, 0.0, 0.0
    qty_lots = int(risk_cash // est_loss_per_lot)
    total_risk = qty_lots * est_loss_per_lot
    return max(qty_lots, 0), est_loss_per_lot, total_risk
