from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

from data_access import DATABASE_PATH, PROCESSED_DIR, resolve_input_path

PURCHASE_CHUNK_SIZE = 250_000
SALES_CHUNK_SIZE = 500_000


def clean_text(series: pd.Series, unknown: str = "Unknown") -> pd.Series:
    """Standardize text fields used as grouping keys."""
    return series.fillna(unknown).astype(str).str.strip().replace("", unknown)


def aggregate_purchases() -> pd.DataFrame:
    """Aggregate raw purchase transactions to vendor-brand-category level."""
    usecols = [
        "VendorNumber",
        "VendorName",
        "Brand",
        "Description",
        "Classification",
        "PONumber",
        "PODate",
        "ReceivingDate",
        "Quantity",
        "Dollars",
    ]
    grouped_frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(
        resolve_input_path("purchases.csv"),
        usecols=usecols,
        chunksize=PURCHASE_CHUNK_SIZE,
        low_memory=False,
    ):
        chunk["VendorName"] = clean_text(chunk["VendorName"])
        chunk["Description"] = clean_text(chunk["Description"])
        chunk["Classification"] = clean_text(chunk["Classification"], unknown="Unclassified")
        chunk["Quantity"] = pd.to_numeric(chunk["Quantity"], errors="coerce").fillna(0)
        chunk["Dollars"] = pd.to_numeric(chunk["Dollars"], errors="coerce").fillna(0)
        chunk["PODate"] = pd.to_datetime(chunk["PODate"], errors="coerce")
        chunk["ReceivingDate"] = pd.to_datetime(chunk["ReceivingDate"], errors="coerce")
        chunk["LeadDays"] = (chunk["ReceivingDate"] - chunk["PODate"]).dt.days
        chunk.loc[chunk["LeadDays"] < 0, "LeadDays"] = np.nan
        chunk["LeadDayCount"] = chunk["LeadDays"].notna().astype(int)

        grouped = (
            chunk.groupby(["VendorNumber", "Brand", "Classification"], dropna=False)
            .agg(
                VendorName=("VendorName", "first"),
                Description=("Description", "first"),
                PurchaseQuantity=("Quantity", "sum"),
                PurchaseCost=("Dollars", "sum"),
                PurchaseLineCount=("PONumber", "count"),
                LeadDaySum=("LeadDays", "sum"),
                LeadDayCount=("LeadDayCount", "sum"),
            )
            .reset_index()
        )
        grouped_frames.append(grouped)

    purchases = (
        pd.concat(grouped_frames, ignore_index=True)
        .groupby(["VendorNumber", "Brand", "Classification"], dropna=False)
        .agg(
            VendorName=("VendorName", "first"),
            Description=("Description", "first"),
            PurchaseQuantity=("PurchaseQuantity", "sum"),
            PurchaseCost=("PurchaseCost", "sum"),
            PurchaseLineCount=("PurchaseLineCount", "sum"),
            LeadDaySum=("LeadDaySum", "sum"),
            LeadDayCount=("LeadDayCount", "sum"),
        )
        .reset_index()
    )
    return purchases


def aggregate_sales() -> pd.DataFrame:
    """Aggregate raw sales transactions to vendor-brand-category level."""
    usecols = [
        "VendorNo",
        "VendorName",
        "Brand",
        "Description",
        "Classification",
        "SalesQuantity",
        "SalesDollars",
        "ExciseTax",
    ]
    grouped_frames: list[pd.DataFrame] = []

    for chunk in pd.read_csv(
        resolve_input_path("sales.csv"),
        usecols=usecols,
        chunksize=SALES_CHUNK_SIZE,
        low_memory=False,
    ):
        chunk["VendorName"] = clean_text(chunk["VendorName"])
        chunk["Description"] = clean_text(chunk["Description"])
        chunk["Classification"] = clean_text(chunk["Classification"], unknown="Unclassified")
        chunk["SalesQuantity"] = pd.to_numeric(
            chunk["SalesQuantity"], errors="coerce"
        ).fillna(0)
        chunk["SalesDollars"] = pd.to_numeric(
            chunk["SalesDollars"], errors="coerce"
        ).fillna(0)
        chunk["ExciseTax"] = pd.to_numeric(chunk["ExciseTax"], errors="coerce").fillna(0)

        grouped = (
            chunk.groupby(["VendorNo", "Brand", "Classification"], dropna=False)
            .agg(
                VendorName=("VendorName", "first"),
                Description=("Description", "first"),
                SalesQuantity=("SalesQuantity", "sum"),
                Revenue=("SalesDollars", "sum"),
                ExciseTax=("ExciseTax", "sum"),
            )
            .reset_index()
            .rename(columns={"VendorNo": "VendorNumber"})
        )
        grouped_frames.append(grouped)

    sales = (
        pd.concat(grouped_frames, ignore_index=True)
        .groupby(["VendorNumber", "Brand", "Classification"], dropna=False)
        .agg(
            VendorName=("VendorName", "first"),
            Description=("Description", "first"),
            SalesQuantity=("SalesQuantity", "sum"),
            Revenue=("Revenue", "sum"),
            ExciseTax=("ExciseTax", "sum"),
        )
        .reset_index()
    )
    return sales


def aggregate_price_reference() -> pd.DataFrame:
    """Create a reference table for average retail and purchase pricing."""
    price_reference = pd.read_csv(resolve_input_path("purchase_prices.csv"), low_memory=False)
    price_reference["VendorName"] = clean_text(price_reference["VendorName"])
    price_reference["Description"] = clean_text(price_reference["Description"])
    price_reference["Classification"] = clean_text(
        price_reference["Classification"], unknown="Unclassified"
    )
    price_reference["Price"] = pd.to_numeric(price_reference["Price"], errors="coerce").fillna(0)
    price_reference["PurchasePrice"] = pd.to_numeric(
        price_reference["PurchasePrice"], errors="coerce"
    ).fillna(0)
    price_reference["Volume"] = pd.to_numeric(
        price_reference["Volume"], errors="coerce"
    ).fillna(0)

    price_reference = (
        price_reference.groupby(["VendorNumber", "Brand", "Classification"], dropna=False)
        .agg(
            VendorName=("VendorName", "first"),
            Description=("Description", "first"),
            ReferenceRetailPrice=("Price", "mean"),
            ReferencePurchasePrice=("PurchasePrice", "mean"),
            VolumeML=("Volume", "mean"),
        )
        .reset_index()
    )
    return price_reference


def aggregate_freight() -> pd.DataFrame:
    """Summarize invoice and freight information at vendor level."""
    invoices = pd.read_csv(resolve_input_path("vendor_invoice.csv"), low_memory=False)
    invoices["VendorName"] = clean_text(invoices["VendorName"])
    invoices["Freight"] = pd.to_numeric(invoices["Freight"], errors="coerce").fillna(0)
    invoices["Dollars"] = pd.to_numeric(invoices["Dollars"], errors="coerce").fillna(0)
    freight = (
        invoices.groupby("VendorNumber", dropna=False)
        .agg(
            VendorName=("VendorName", "first"),
            FreightCost=("Freight", "sum"),
            InvoiceValue=("Dollars", "sum"),
            InvoiceCount=("PONumber", "nunique"),
        )
        .reset_index()
    )
    return freight


def aggregate_inventory_snapshots() -> pd.DataFrame:
    """Aggregate beginning and ending inventory snapshots to brand-description level."""
    begin_inventory = pd.read_csv(resolve_input_path("begin_inventory.csv"), low_memory=False)
    end_inventory = pd.read_csv(resolve_input_path("end_inventory.csv"), low_memory=False)

    def prepare_inventory_snapshot(
        dataframe: pd.DataFrame,
        units_column: str,
        retail_value_column: str,
    ) -> pd.DataFrame:
        prepared = dataframe.copy()
        prepared["Description"] = clean_text(prepared["Description"])
        prepared["onHand"] = pd.to_numeric(prepared["onHand"], errors="coerce").fillna(0)
        prepared["Price"] = pd.to_numeric(prepared["Price"], errors="coerce").fillna(0)
        prepared[retail_value_column] = prepared["onHand"] * prepared["Price"]
        prepared = (
            prepared.groupby(["Brand", "Description"], dropna=False)
            .agg(
                **{
                    units_column: ("onHand", "sum"),
                    retail_value_column: (retail_value_column, "sum"),
                }
            )
            .reset_index()
        )
        return prepared

    beginning = prepare_inventory_snapshot(
        begin_inventory,
        units_column="BeginningInventoryUnits",
        retail_value_column="BeginningInventoryRetailValue",
    )
    ending = prepare_inventory_snapshot(
        end_inventory,
        units_column="EndingInventoryUnits",
        retail_value_column="EndingInventoryRetailValue",
    )

    return beginning.merge(ending, on=["Brand", "Description"], how="outer").fillna(0)


def merge_vendor_brand_tables(
    purchases: pd.DataFrame,
    sales: pd.DataFrame,
    price_reference: pd.DataFrame,
    freight: pd.DataFrame,
    inventory: pd.DataFrame,
) -> pd.DataFrame:
    """Combine transaction summaries and derive portfolio performance metrics."""
    merge_keys = ["VendorNumber", "Brand", "Classification"]
    vendor_brand = purchases.merge(
        sales,
        on=merge_keys,
        how="outer",
        suffixes=("_Purchase", "_Sales"),
    )
    vendor_brand = vendor_brand.merge(
        price_reference,
        on=merge_keys,
        how="left",
        suffixes=("", "_Price"),
    )
    vendor_brand = vendor_brand.merge(
        freight,
        on="VendorNumber",
        how="left",
        suffixes=("", "_Freight"),
    )
    vendor_brand = vendor_brand.merge(
        inventory,
        on=["Brand", "Description"],
        how="left",
    )

    vendor_brand["VendorName"] = (
        vendor_brand["VendorName_Purchase"]
        .fillna(vendor_brand["VendorName_Sales"])
        .fillna(vendor_brand["VendorName"])
        .fillna(vendor_brand["VendorName_Freight"])
    )
    vendor_brand["Description"] = (
        vendor_brand["Description_Purchase"]
        .fillna(vendor_brand["Description_Sales"])
        .fillna(vendor_brand["Description"])
    )

    numeric_columns = [
        "PurchaseQuantity",
        "PurchaseCost",
        "PurchaseLineCount",
        "LeadDaySum",
        "LeadDayCount",
        "SalesQuantity",
        "Revenue",
        "ExciseTax",
        "ReferenceRetailPrice",
        "ReferencePurchasePrice",
        "VolumeML",
        "FreightCost",
        "InvoiceValue",
        "InvoiceCount",
        "BeginningInventoryUnits",
        "BeginningInventoryRetailValue",
        "EndingInventoryUnits",
        "EndingInventoryRetailValue",
    ]
    for column in numeric_columns:
        vendor_brand[column] = vendor_brand[column].fillna(0)

    vendor_brand["AvgUnitCost"] = np.where(
        vendor_brand["PurchaseQuantity"] > 0,
        vendor_brand["PurchaseCost"] / vendor_brand["PurchaseQuantity"],
        0,
    )
    vendor_brand["AvgUnitRevenue"] = np.where(
        vendor_brand["SalesQuantity"] > 0,
        vendor_brand["Revenue"] / vendor_brand["SalesQuantity"],
        0,
    )
    vendor_brand["GrossProfit"] = vendor_brand["Revenue"] - vendor_brand["PurchaseCost"]
    vendor_brand["GrossMarginPct"] = np.where(
        vendor_brand["Revenue"] > 0,
        (vendor_brand["GrossProfit"] / vendor_brand["Revenue"]) * 100,
        0,
    )
    vendor_brand["AvgLeadDays"] = np.where(
        vendor_brand["LeadDayCount"] > 0,
        vendor_brand["LeadDaySum"] / vendor_brand["LeadDayCount"],
        0,
    )
    vendor_brand["StockTurnover"] = np.where(
        vendor_brand["PurchaseQuantity"] > 0,
        vendor_brand["SalesQuantity"] / vendor_brand["PurchaseQuantity"],
        0,
    )
    vendor_brand["SalesToPurchaseRatio"] = np.where(
        vendor_brand["PurchaseCost"] > 0,
        vendor_brand["Revenue"] / vendor_brand["PurchaseCost"],
        0,
    )
    derived_unsold = (vendor_brand["PurchaseQuantity"] - vendor_brand["SalesQuantity"]).clip(lower=0)
    vendor_brand["UnsoldQuantity"] = np.where(
        vendor_brand["EndingInventoryUnits"] > 0,
        vendor_brand["EndingInventoryUnits"],
        derived_unsold,
    )
    vendor_brand["InventoryChangeUnits"] = (
        vendor_brand["EndingInventoryUnits"] - vendor_brand["BeginningInventoryUnits"]
    )
    available_units = vendor_brand["BeginningInventoryUnits"] + vendor_brand["PurchaseQuantity"]
    vendor_brand["SellThroughRate"] = np.where(
        available_units > 0,
        vendor_brand["SalesQuantity"] / available_units,
        0,
    )
    vendor_brand["InventoryAtRisk"] = vendor_brand["UnsoldQuantity"] * np.where(
        vendor_brand["AvgUnitCost"] > 0,
        vendor_brand["AvgUnitCost"],
        vendor_brand["ReferencePurchasePrice"],
    )

    # Allocate total freight to brand lines based on procurement spend share.
    vendor_purchase_totals = vendor_brand.groupby("VendorNumber")["PurchaseCost"].transform("sum")
    vendor_brand["AllocatedFreight"] = np.where(
        vendor_purchase_totals > 0,
        vendor_brand["FreightCost"] * vendor_brand["PurchaseCost"] / vendor_purchase_totals,
        0,
    )
    vendor_brand["AdjustedProfit"] = (
        vendor_brand["Revenue"] - vendor_brand["PurchaseCost"] - vendor_brand["AllocatedFreight"]
    )
    vendor_brand["AdjustedMarginPct"] = np.where(
        vendor_brand["Revenue"] > 0,
        (vendor_brand["AdjustedProfit"] / vendor_brand["Revenue"]) * 100,
        0,
    )

    vendor_brand["VendorName"] = clean_text(
        vendor_brand.pop("VendorName"), unknown="Unknown Vendor"
    )
    vendor_brand["Description"] = clean_text(vendor_brand.pop("Description"))
    vendor_brand.drop(
        columns=[
            column
            for column in [
                "VendorName_Purchase",
                "VendorName_Sales",
                "VendorName_Freight",
                "Description_Purchase",
                "Description_Sales",
            ]
            if column in vendor_brand
        ],
        inplace=True,
    )

    ordered_columns = [
        "VendorNumber",
        "VendorName",
        "Brand",
        "Description",
        "Classification",
        "PurchaseQuantity",
        "SalesQuantity",
        "PurchaseCost",
        "Revenue",
        "GrossProfit",
        "GrossMarginPct",
        "AllocatedFreight",
        "AdjustedProfit",
        "AdjustedMarginPct",
        "ExciseTax",
        "AvgUnitCost",
        "AvgUnitRevenue",
        "ReferencePurchasePrice",
        "ReferenceRetailPrice",
        "VolumeML",
        "AvgLeadDays",
        "StockTurnover",
        "SellThroughRate",
        "SalesToPurchaseRatio",
        "BeginningInventoryUnits",
        "EndingInventoryUnits",
        "InventoryChangeUnits",
        "BeginningInventoryRetailValue",
        "EndingInventoryRetailValue",
        "UnsoldQuantity",
        "InventoryAtRisk",
        "PurchaseLineCount",
        "InvoiceCount",
        "InvoiceValue",
        "LeadDaySum",
        "LeadDayCount",
        "FreightCost",
    ]
    vendor_brand = vendor_brand[ordered_columns].sort_values(
        ["AdjustedProfit", "Revenue"], ascending=[False, False]
    )
    return vendor_brand


def build_vendor_summary(vendor_brand: pd.DataFrame) -> pd.DataFrame:
    """Roll brand-level performance into one scorecard row per vendor."""
    vendor_summary = (
        vendor_brand.sort_values(["VendorNumber", "Revenue"], ascending=[True, False])
        .groupby("VendorNumber", dropna=False)
        .agg(
            VendorName=("VendorName", "first"),
            BrandCount=("Brand", "nunique"),
            CategoryCount=("Classification", "nunique"),
            PurchaseQuantity=("PurchaseQuantity", "sum"),
            SalesQuantity=("SalesQuantity", "sum"),
            PurchaseCost=("PurchaseCost", "sum"),
            Revenue=("Revenue", "sum"),
            GrossProfit=("GrossProfit", "sum"),
            AllocatedFreight=("AllocatedFreight", "sum"),
            AdjustedProfit=("AdjustedProfit", "sum"),
            ExciseTax=("ExciseTax", "sum"),
            InventoryAtRisk=("InventoryAtRisk", "sum"),
            UnsoldQuantity=("UnsoldQuantity", "sum"),
            BeginningInventoryUnits=("BeginningInventoryUnits", "sum"),
            EndingInventoryUnits=("EndingInventoryUnits", "sum"),
            InventoryChangeUnits=("InventoryChangeUnits", "sum"),
            EndingInventoryRetailValue=("EndingInventoryRetailValue", "sum"),
            PurchaseLineCount=("PurchaseLineCount", "sum"),
            InvoiceCount=("InvoiceCount", "max"),
            AvgLeadDaysWeightedSum=("LeadDaySum", "sum"),
            AvgLeadDaysWeight=("LeadDayCount", "sum"),
        )
        .reset_index()
    )

    vendor_summary["GrossMarginPct"] = np.where(
        vendor_summary["Revenue"] > 0,
        (vendor_summary["GrossProfit"] / vendor_summary["Revenue"]) * 100,
        0,
    )
    vendor_summary["AdjustedMarginPct"] = np.where(
        vendor_summary["Revenue"] > 0,
        (vendor_summary["AdjustedProfit"] / vendor_summary["Revenue"]) * 100,
        0,
    )
    vendor_summary["StockTurnover"] = np.where(
        vendor_summary["PurchaseQuantity"] > 0,
        vendor_summary["SalesQuantity"] / vendor_summary["PurchaseQuantity"],
        0,
    )
    available_units = vendor_summary["BeginningInventoryUnits"] + vendor_summary["PurchaseQuantity"]
    vendor_summary["SellThroughRate"] = np.where(
        available_units > 0,
        vendor_summary["SalesQuantity"] / available_units,
        0,
    )
    vendor_summary["SalesToPurchaseRatio"] = np.where(
        vendor_summary["PurchaseCost"] > 0,
        vendor_summary["Revenue"] / vendor_summary["PurchaseCost"],
        0,
    )
    vendor_summary["AvgLeadDays"] = np.where(
        vendor_summary["AvgLeadDaysWeight"] > 0,
        vendor_summary["AvgLeadDaysWeightedSum"] / vendor_summary["AvgLeadDaysWeight"],
        0,
    )

    total_revenue = vendor_summary["Revenue"].sum()
    total_purchase_cost = vendor_summary["PurchaseCost"].sum()
    vendor_summary["RevenueSharePct"] = np.where(
        total_revenue > 0, vendor_summary["Revenue"] / total_revenue * 100, 0
    )
    vendor_summary["CostSharePct"] = np.where(
        total_purchase_cost > 0,
        vendor_summary["PurchaseCost"] / total_purchase_cost * 100,
        0,
    )

    # Blend commercial scale, profitability, turnover, and service speed into one score.
    adjusted_profit_rank = vendor_summary["AdjustedProfit"].rank(pct=True, method="average")
    margin_rank = vendor_summary["AdjustedMarginPct"].rank(pct=True, method="average")
    turnover_rank = vendor_summary["StockTurnover"].rank(pct=True, method="average")
    revenue_rank = vendor_summary["Revenue"].rank(pct=True, method="average")
    lead_rank = vendor_summary["AvgLeadDays"].rank(
        pct=True, ascending=False, method="average"
    )
    vendor_summary["PerformanceScore"] = (
        adjusted_profit_rank * 0.35
        + margin_rank * 0.25
        + turnover_rank * 0.15
        + revenue_rank * 0.10
        + lead_rank * 0.10
        + vendor_summary["SellThroughRate"].rank(pct=True, method="average") * 0.05
    ) * 100
    vendor_summary["PerformanceRank"] = vendor_summary["PerformanceScore"].rank(
        ascending=False, method="dense"
    ).astype(int)
    vendor_summary["PerformanceTier"] = pd.cut(
        vendor_summary["PerformanceScore"],
        bins=[-0.1, 35, 60, 80, 100],
        labels=["Critical", "Watchlist", "Stable", "High Impact"],
    )

    vendor_summary = vendor_summary.sort_values(
        ["PerformanceScore", "AdjustedProfit"], ascending=[False, False]
    )
    return vendor_summary


def build_vendor_category_summary(vendor_brand: pd.DataFrame) -> pd.DataFrame:
    """Summarize vendor performance within each product classification."""
    vendor_category = (
        vendor_brand.sort_values(["VendorNumber", "Revenue"], ascending=[True, False])
        .groupby(["VendorNumber", "Classification"], dropna=False)
        .agg(
            VendorName=("VendorName", "first"),
            BrandCount=("Brand", "nunique"),
            PurchaseCost=("PurchaseCost", "sum"),
            Revenue=("Revenue", "sum"),
            GrossProfit=("GrossProfit", "sum"),
            AdjustedProfit=("AdjustedProfit", "sum"),
            InventoryAtRisk=("InventoryAtRisk", "sum"),
            EndingInventoryUnits=("EndingInventoryUnits", "sum"),
            AvgLeadDaysWeightedSum=("LeadDaySum", "sum"),
            AvgLeadDaysWeight=("LeadDayCount", "sum"),
        )
        .reset_index()
    )
    vendor_category["GrossMarginPct"] = np.where(
        vendor_category["Revenue"] > 0,
        (vendor_category["GrossProfit"] / vendor_category["Revenue"]) * 100,
        0,
    )
    vendor_category["AdjustedMarginPct"] = np.where(
        vendor_category["Revenue"] > 0,
        (vendor_category["AdjustedProfit"] / vendor_category["Revenue"]) * 100,
        0,
    )
    vendor_category["AvgLeadDays"] = np.where(
        vendor_category["AvgLeadDaysWeight"] > 0,
        vendor_category["AvgLeadDaysWeightedSum"] / vendor_category["AvgLeadDaysWeight"],
        0,
    )
    return vendor_category.sort_values(["AdjustedProfit", "Revenue"], ascending=[False, False])


def build_vendor_watchlist(vendor_summary: pd.DataFrame) -> pd.DataFrame:
    """Create an action-oriented watchlist for weak or inventory-heavy vendors."""
    watchlist = vendor_summary.copy()
    watchlist["RiskScore"] = (
        (watchlist["AdjustedProfit"] < 0).astype(int) * 3
        + (watchlist["PerformanceTier"] == "Critical").astype(int) * 2
        + (watchlist["InventoryAtRisk"] >= watchlist["InventoryAtRisk"].quantile(0.90)).astype(int) * 2
        + (watchlist["SellThroughRate"] < 0.9).astype(int)
    )
    watchlist["WatchlistReason"] = np.select(
        [
            watchlist["AdjustedProfit"] < 0,
            watchlist["InventoryAtRisk"] >= watchlist["InventoryAtRisk"].quantile(0.75),
            watchlist["PerformanceTier"].isin(["Critical", "Watchlist"]),
        ],
        [
            "Negative adjusted profit",
            "High inventory at risk",
            "Low performance tier",
        ],
        default="Monitor",
    )
    watchlist = watchlist.loc[watchlist["RiskScore"] > 0]
    return watchlist.sort_values(
        ["RiskScore", "InventoryAtRisk", "AdjustedProfit"],
        ascending=[False, False, True],
    ).head(25)


def build_vendor_tier_summary(vendor_summary: pd.DataFrame) -> pd.DataFrame:
    """Summarize commercial outcomes by vendor tier for dashboard storytelling."""
    tier_summary = (
        vendor_summary.groupby("PerformanceTier", dropna=False)
        .agg(
            VendorCount=("VendorNumber", "nunique"),
            Revenue=("Revenue", "sum"),
            AdjustedProfit=("AdjustedProfit", "sum"),
            InventoryAtRisk=("InventoryAtRisk", "sum"),
            EndingInventoryUnits=("EndingInventoryUnits", "sum"),
            AveragePerformanceScore=("PerformanceScore", "mean"),
        )
        .reset_index()
        .sort_values("AveragePerformanceScore", ascending=False)
    )
    return tier_summary


def build_category_summary(vendor_brand: pd.DataFrame) -> pd.DataFrame:
    """Create a category-level profitability view for dashboard reporting."""
    category_summary = (
        vendor_brand.groupby("Classification", dropna=False)
        .agg(
            VendorCount=("VendorNumber", "nunique"),
            BrandCount=("Brand", "nunique"),
            PurchaseCost=("PurchaseCost", "sum"),
            Revenue=("Revenue", "sum"),
            GrossProfit=("GrossProfit", "sum"),
            AdjustedProfit=("AdjustedProfit", "sum"),
            InventoryAtRisk=("InventoryAtRisk", "sum"),
        )
        .reset_index()
    )
    category_summary["GrossMarginPct"] = np.where(
        category_summary["Revenue"] > 0,
        (category_summary["GrossProfit"] / category_summary["Revenue"]) * 100,
        0,
    )
    return category_summary.sort_values("AdjustedProfit", ascending=False)


def write_outputs(outputs: dict[str, pd.DataFrame]) -> dict[str, Path]:
    """Persist curated outputs as CSV files and SQLite tables."""
    saved_paths: dict[str, Path] = {}
    for table_name, dataframe in outputs.items():
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = PROCESSED_DIR / f"{table_name}.csv"
        dataframe.to_csv(csv_path, index=False)
        saved_paths[table_name] = csv_path

    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()
    with sqlite3.connect(DATABASE_PATH) as connection:
        for table_name, dataframe in outputs.items():
            dataframe.to_sql(table_name, connection, if_exists="replace", index=False)

    return saved_paths


def build_outputs() -> dict[str, Path]:
    """Build all curated project assets from the raw source files."""
    purchases = aggregate_purchases()
    sales = aggregate_sales()
    price_reference = aggregate_price_reference()
    freight = aggregate_freight()
    inventory = aggregate_inventory_snapshots()

    vendor_brand = merge_vendor_brand_tables(purchases, sales, price_reference, freight, inventory)
    vendor_summary = build_vendor_summary(vendor_brand)
    vendor_category = build_vendor_category_summary(vendor_brand)
    category_summary = build_category_summary(vendor_brand)
    vendor_watchlist = build_vendor_watchlist(vendor_summary)
    vendor_tier_summary = build_vendor_tier_summary(vendor_summary)

    outputs = {
        "vendor_brand_performance": vendor_brand,
        "vendor_performance_summary": vendor_summary,
        "vendor_category_performance": vendor_category,
        "category_performance_summary": category_summary,
        "vendor_watchlist": vendor_watchlist,
        "vendor_tier_summary": vendor_tier_summary,
    }
    return write_outputs(outputs)


if __name__ == "__main__":
    output_paths = build_outputs()
    for name, path in output_paths.items():
        print(f"{name}: {path}")
