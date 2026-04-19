from __future__ import annotations

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ASSETS_DIR = PROJECT_ROOT / "assets"
SUMMARY_PATH = PROJECT_ROOT / "data" / "processed" / "vendor_performance_summary.csv"


def main() -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    summary_df = pd.read_csv(SUMMARY_PATH)
    top_profit = summary_df.nlargest(10, "AdjustedProfit").sort_values("AdjustedProfit")
    top_risk = summary_df.nlargest(10, "InventoryAtRisk").sort_values("InventoryAtRisk")

    fig, axes = plt.subplots(1, 2, figsize=(16, 8))

    axes[0].barh(top_profit["VendorName"], top_profit["AdjustedProfit"] / 1_000_000, color="#155e75")
    axes[0].set_title("Top Vendors by Adjusted Profit")
    axes[0].set_xlabel("Adjusted Profit (USD Millions)")
    axes[0].set_ylabel("")

    axes[1].barh(top_risk["VendorName"], top_risk["InventoryAtRisk"] / 1_000_000, color="#b45309")
    axes[1].set_title("Highest Inventory-at-Risk Vendors")
    axes[1].set_xlabel("Inventory at Risk (USD Millions)")
    axes[1].set_ylabel("")

    fig.suptitle("Vendor Portfolio Overview", fontsize=16, fontweight="bold")
    fig.tight_layout()
    output_path = ASSETS_DIR / "vendor_portfolio_overview.png"
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved dashboard asset: {output_path}")


if __name__ == "__main__":
    main()
