# core.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import numpy as np
import pandas as pd


@dataclass
class AppConfig:
    target_doh: int = 21
    baseline_ma_days: int = 28
    promo_impact_pct_default: float = 80.0
    dcs: tuple[str, ...] = ("DC1", "DC2", "DC4")


def _safe_to_datetime(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")


def _normalize_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def _pick_latest_stock_date(stock: pd.DataFrame) -> pd.Timestamp:
    dt = _safe_to_datetime(stock["Stock_Date"])
    return dt.max()


def _dc_cols(dc: str) -> dict:
    return {
        "store_qty": f"{dc}_StoreStockQty",
        "dc_qty": f"{dc}_DCStockQty",
        "doh_store": f"{dc}_DOHStore",
        "doh_dc": f"{dc}_DOHDC",
        "avg_sale_90d": f"{dc}_AvgSaleQty90D",
        "perc_oos": f"{dc}_PercOOS",
        "oos_assort": f"{dc}_OOSAssort",
        "scm_assort": f"{dc}_ScmAssort",
    }


def _lead_time_col(dc: str) -> str:
    return f"Lead Time to {dc}"


def _moq_sku_col(dc: str) -> str:
    return f"{dc}_MOQ_per_SKU[CTN]"


def compute_open_po_pipeline(salein: pd.DataFrame) -> pd.DataFrame:
    df = salein.copy()
    df["CJ_Item"] = _normalize_str(df["CJ_Item"])

    qty_col = "Actual_Qty" if "Actual_Qty" in df.columns else "Order_Qty"
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    # open criteria
    received_blank = df["Received_Date"].isna() if "Received_Date" in df.columns else pd.Series([True] * len(df))

    if "Delivery_Status" in df.columns:
        status = df["Delivery_Status"].astype(str).str.lower()
        not_closed = ~status.isin(["received", "complete", "completed", "closed"])
    else:
        not_closed = True

    open_po = df[received_blank & not_closed].copy()

    dc_key = "DC_Code" if "DC_Code" in open_po.columns and open_po["DC_Code"].nunique() > 1 else "DC_Name"
    if dc_key not in open_po.columns:
        dc_key = None

    if dc_key:
        out = open_po.groupby(["CJ_Item", dc_key], as_index=False)[qty_col].sum()
        out = out.rename(columns={dc_key: "dc", qty_col: "pipeline_qty_open_po"})
    else:
        out = open_po.groupby(["CJ_Item"], as_index=False)[qty_col].sum()
        out["dc"] = "UNKNOWN"
        out = out.rename(columns={qty_col: "pipeline_qty_open_po"})

    return out


def make_inside_outside_case(doh_store: float, doh_dc: float) -> str:
    if pd.isna(doh_store) or pd.isna(doh_dc):
        return "UNKNOWN"
    store_oos = doh_store <= 0
    dc_oos = doh_dc <= 0
    if store_oos and not dc_oos:
        return "STORE_OOS_DC_OK"
    if (not store_oos) and dc_oos:
        return "DC_OOS_STORE_OK"
    if store_oos and dc_oos:
        return "BOTH_OOS"
    return "OK"


def build_oos_reconcile(stock: pd.DataFrame, salein: pd.DataFrame, pro: pd.DataFrame, moq: pd.DataFrame, cfg: AppConfig):
    latest = _pick_latest_stock_date(stock)
    snap = stock[stock["Stock_Date"] == latest].copy()
    snap["CJ_Item"] = _normalize_str(snap["CJ_Item"])

    open_po = compute_open_po_pipeline(salein)
    open_po["CJ_Item"] = _normalize_str(open_po["CJ_Item"])

    # Total level
    total = snap[[
        "CJ_Item", "Description", "Division", "Category", "Subcate",
        "Total_StoreStockQty", "Total_DCStockQty", "Total_DOHStore", "Total_DOHDC", "Total_AvgSaleQty90D"
    ]].copy()

    total = total.rename(columns={
        "Total_StoreStockQty": "store_stock_qty",
        "Total_DCStockQty": "dc_stock_qty",
        "Total_DOHStore": "doh_store",
        "Total_DOHDC": "doh_dc",
        "Total_AvgSaleQty90D": "avg_sale_qty_90d",
    })
    total["as_of_date"] = latest.date().isoformat()
    total["dc"] = "Total"
    total["oos_flag_store"] = (pd.to_numeric(total["doh_store"], errors="coerce").fillna(0) <= 0).astype(int)
    total["oos_flag_dc"] = (pd.to_numeric(total["doh_dc"], errors="coerce").fillna(0) <= 0).astype(int)
    total["inside_outside_case"] = [
        make_inside_outside_case(ds, dd)
        for ds, dd in zip(pd.to_numeric(total["doh_store"], errors="coerce"), pd.to_numeric(total["doh_dc"], errors="coerce"))
    ]
    total["root_cause_code"] = np.where(total["inside_outside_case"] == "OK", "OK", "NEED_DC_LEVEL_REVIEW")
    total["root_cause_text"] = np.where(total["inside_outside_case"] == "OK", "No OOS detected", "Review at DC-level for root cause")

    for c in [
        "evidence_po_number","evidence_delivery_status","evidence_required_delivery_date","evidence_received_date","evidence_diff_qty",
        "evidence_pro_id","evidence_promo_window","evidence_uplift_indicator","evidence_moq_ctn","evidence_lt_days"
    ]:
        total[c] = ""
    total["notes"] = ""

    detail_rows = [total]

    # DC-level detail (rule-based cause simplified)
    for dc in cfg.dcs:
        cols = _dc_cols(dc)
        d = snap[[
            "CJ_Item", "Description", "Division", "Category", "Subcate",
            cols["store_qty"], cols["dc_qty"], cols["doh_store"], cols["doh_dc"], cols["avg_sale_90d"]
        ]].copy()

        d = d.rename(columns={
            cols["store_qty"]:"store_stock_qty",
            cols["dc_qty"]:"dc_stock_qty",
            cols["doh_store"]:"doh_store",
            cols["doh_dc"]:"doh_dc",
            cols["avg_sale_90d"]:"avg_sale_qty_90d",
        })
        d["as_of_date"] = latest.date().isoformat()
        d["dc"] = dc

        d["oos_flag_store"] = (pd.to_numeric(d["doh_store"], errors="coerce").fillna(0) <= 0).astype(int)
        d["oos_flag_dc"]    = (pd.to_numeric(d["doh_dc"], errors="coerce").fillna(0) <= 0).astype(int)
        d["inside_outside_case"] = [
            make_inside_outside_case(ds, dd)
            for ds, dd in zip(pd.to_numeric(d["doh_store"], errors="coerce"), pd.to_numeric(d["doh_dc"], errors="coerce"))
        ]

        # simplified root cause
        # - STORE_OOS_DC_OK => allocation/store execution
        # - DC/BOTH OOS => pipeline? else no_pipeline
        pipe = open_po[open_po["dc"].astype(str).str.contains(dc[-1], na=False)].groupby("CJ_Item", as_index=False)["pipeline_qty_open_po"].sum()
        d = d.merge(pipe, on="CJ_Item", how="left")
        d["pipeline_qty_open_po"] = d["pipeline_qty_open_po"].fillna(0.0)

        def cause(row):
            case = row["inside_outside_case"]
            if case == "OK":
                return ("OK", "No OOS detected")
            if case == "STORE_OOS_DC_OK":
                return ("ALLOCATION_STORE_EXEC", "Store OOS but DC has stock (distribution/allocation/store execution)")
            if case in ("DC_OOS_STORE_OK","BOTH_OOS"):
                if row["pipeline_qty_open_po"] <= 0:
                    return ("NO_PIPELINE", "DC OOS with no open PO pipeline")
                return ("LATE_OR_INSUFFICIENT_PO", "DC OOS but PO pipeline exists (late delivery or insufficient qty)")
            return ("UNKNOWN", "Unable to classify")

        rc = d.apply(lambda r: cause(r), axis=1, result_type="expand")
        d["root_cause_code"] = rc[0]
        d["root_cause_text"] = rc[1]

        # evidence columns (leave blanks now; you can enrich later)
        for c in [
            "evidence_po_number","evidence_delivery_status","evidence_required_delivery_date","evidence_received_date","evidence_diff_qty",
            "evidence_pro_id","evidence_promo_window","evidence_uplift_indicator","evidence_moq_ctn","evidence_lt_days"
        ]:
            d[c] = ""
        d["notes"] = ""

        detail_rows.append(d.drop(columns=["pipeline_qty_open_po"]))

    detail_df = pd.concat(detail_rows, ignore_index=True)

    # summary
    summary_rows = []
    for dc in ["Total"] + list(cfg.dcs):
        sub = detail_df[detail_df["dc"] == dc]
        summary_rows.append({
            "as_of_date": latest.date().isoformat(),
            "dc": dc,
            "total_scm_assort": len(sub),
            "oos_assort": int(((sub["oos_flag_store"]==1) | (sub["oos_flag_dc"]==1)).sum()),
            "perc_oos": float((((sub["oos_flag_store"]==1) | (sub["oos_flag_dc"]==1)).sum()) / max(len(sub), 1)),
            "count_store_oos_dc_ok": int((sub["inside_outside_case"]=="STORE_OOS_DC_OK").sum()),
            "count_dc_oos_store_ok": int((sub["inside_outside_case"]=="DC_OOS_STORE_OK").sum()),
            "count_both_oos": int((sub["inside_outside_case"]=="BOTH_OOS").sum()),
            "count_ok": int((sub["inside_outside_case"]=="OK").sum()),
        })
    summary_df = pd.DataFrame(summary_rows)

    return detail_df, summary_df


def build_suggested_order_qty(stock: pd.DataFrame, salein: pd.DataFrame, moq: pd.DataFrame, saleout: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    latest = _pick_latest_stock_date(stock)
    snap = stock[stock["Stock_Date"] == latest].copy()
    snap["CJ_Item"] = _normalize_str(snap["CJ_Item"])

    so = saleout[["Sale_Date","CJ_Item","Sale_Qty"]].dropna()
    so["CJ_Item"] = _normalize_str(so["CJ_Item"])
    so["Sale_Date"] = _safe_to_datetime(so["Sale_Date"])
    so["Sale_Qty"] = pd.to_numeric(so["Sale_Qty"], errors="coerce").fillna(0.0)

    last = so["Sale_Date"].max()
    start = last - pd.Timedelta(days=cfg.baseline_ma_days)
    so28 = so[(so["Sale_Date"] > start) & (so["Sale_Date"] <= last)]
    avg_daily = so28.groupby("CJ_Item", as_index=False)["Sale_Qty"].mean().rename(columns={"Sale_Qty":"avg_daily_sales"})

    open_po = compute_open_po_pipeline(salein)
    open_po["CJ_Item"] = _normalize_str(open_po["CJ_Item"])

    out_rows = []
    for dc in cfg.dcs:
        cols = _dc_cols(dc)
        lt_col = _lead_time_col(dc)
        moq_col = _moq_sku_col(dc)

        df = snap[["CJ_Item","Description", cols["dc_qty"]]].copy()
        df = df.rename(columns={cols["dc_qty"]:"current_dc_stock_qty"})
        df["dc"] = dc
        df["as_of_date"] = latest.date().isoformat()

        df = df.merge(avg_daily, on="CJ_Item", how="left")
        df["avg_daily_sales"] = df["avg_daily_sales"].fillna(0.0)

        m = moq.copy()
        m["CJ_Item"] = _normalize_str(m["CJ_Item"])
        keep = ["CJ_Item","Supplier_Name","PC_Cartons", lt_col, moq_col]
        keep = [c for c in keep if c in m.columns]
        m = m[keep].drop_duplicates("CJ_Item")
        df = df.merge(m, on="CJ_Item", how="left")

        df["lead_time_days"] = pd.to_numeric(df.get(lt_col, np.nan), errors="coerce").fillna(0).astype(int)
        df["moq_ctn"] = pd.to_numeric(df.get(moq_col, np.nan), errors="coerce").fillna(0).astype(int)
        df["pc_cartons"] = pd.to_numeric(df.get("PC_Cartons", np.nan), errors="coerce").fillna(0).astype(int)

        pipe = open_po[open_po["dc"].astype(str).str.contains(dc[-1], na=False)].groupby("CJ_Item", as_index=False)["pipeline_qty_open_po"].sum()
        df = df.merge(pipe, on="CJ_Item", how="left")
        df["pipeline_qty_open_po"] = df["pipeline_qty_open_po"].fillna(0.0)

        df["forecast_demand_lt"] = df["avg_daily_sales"] * df["lead_time_days"]
        df["target_doh"] = cfg.target_doh
        df["target_stock_qty"] = df["avg_daily_sales"] * cfg.target_doh

        df["net_requirement_qty"] = df["target_stock_qty"] - (df["current_dc_stock_qty"] + df["pipeline_qty_open_po"])
        df["net_requirement_qty"] = df["net_requirement_qty"].apply(lambda x: max(0.0, x))

        df["suggested_order_qty_ctn"] = np.maximum(df["net_requirement_qty"], df["moq_ctn"]).round().astype(int)

        df["risk_flag"] = np.where(
            (df["current_dc_stock_qty"] + df["pipeline_qty_open_po"]) < df["forecast_demand_lt"],
            "OOS_RISK",
            "OK"
        )

        df["notes"] = ""

        out_rows.append(df[[
            "as_of_date","dc","CJ_Item","Description","Supplier_Name",
            "current_dc_stock_qty","pipeline_qty_open_po","avg_daily_sales","forecast_demand_lt",
            "lead_time_days","target_doh","target_stock_qty",
            "net_requirement_qty","moq_ctn","pc_cartons","suggested_order_qty_ctn",
            "risk_flag","notes"
        ]])

    return pd.concat(out_rows, ignore_index=True)