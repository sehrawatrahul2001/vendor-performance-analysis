from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_access import PROCESSED_DIR


def load_vendor_summary(path: Path | None = None) -> pd.DataFrame:
    summary_path = path or PROCESSED_DIR / "vendor_performance_summary.csv"
    return pd.read_csv(summary_path)


def build_portfolio_kpis(summary_df: pd.DataFrame) -> dict[str, float | int]:
    total_revenue = float(summary_df["Revenue"].sum())
    total_cost = float(summary_df["PurchaseCost"].sum())
    total_adjusted_profit = float(summary_df["AdjustedProfit"].sum())
    adjusted_margin = (total_adjusted_profit / total_revenue * 100) if total_revenue else 0
    return {
        "vendor_count": int(summary_df["VendorNumber"].nunique()),
        "total_revenue": round(total_revenue, 2),
        "total_cost": round(total_cost, 2),
        "total_adjusted_profit": round(total_adjusted_profit, 2),
        "adjusted_margin_pct": round(adjusted_margin, 2),
        "inventory_at_risk": round(float(summary_df["InventoryAtRisk"].sum()), 2),
    }
