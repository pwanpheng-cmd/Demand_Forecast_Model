# core.py
from __future__ import annotations
from dataclasses import dataclass
import numpy as np
import pandas as pd


@dataclass
class AppConfig:
    target_doh: int = 21
    baseline_ma_days: int = 28
    promo_impact_pct_default: float = 80.0
    dcs: tuple[str, ...] = ("DC1", "DC2", "DC4")


def normalize_inputs(saleout: pd.DataFrame, stock: pd.DataFrame, salein: pd.DataFrame, moq: pd.DataFrame, pro: pd.DataFrame | None):
    pro = pro if pro is not None else pd.DataFrame()

    for df in [saleout, stock, salein, moq, pro]:
        if df is not None and "CJ_Item" in df.columns:
            df["CJ_Item"] = df["CJ_Item"].astype(str).str.strip()

    if "Sale_Date" in saleout.columns:
        saleout["Sale_Date"] = pd.to_datetime(saleout["Sale_Date"], errors="coerce")
    if "Stock_Date" in stock.columns:
        stock["Stock_Date"] = pd.to_datetime(stock["Stock_Date"], errors="coerce")

    for c in ["PO_Date", "Reqired_Delivery_Date", "Received_Date", "Delivery_Date"]:
        if c in salein.columns:
            salein[c] = pd.to_datetime(salein[c], errors="coerce")

    for c in ["Promotion_Start_date", "Promotion_End_date", "Update_date"]:
        if c in pro.columns:
            pro[c] = pd.to_datetime(pro[c], errors="coerce")

    return saleout, stock, salein, moq, pro


def _pick_latest_stock_date(stock: pd.DataFrame) -> pd.Timestamp:
    if "Stock_Date" not in stock.columns:
        raise ValueError("Combine_Stock_CJ.xlsx ต้องมีคอลัมน์ Stock_Date")
    dt = pd.to_datetime(stock["Stock_Date"], errors="coerce")
    latest = dt.max()
    if pd.isna(latest):
        raise ValueError("Stock_Date แปลงเป็นวันที่ไม่ได้ (ตรวจรูปแบบวันที่ในไฟล์ Stock)")
    return latest


def _dc_cols(dc: str) -> dict:
    return {
        "store_qty": f"{dc}_StoreStockQty",
        "dc_qty": f"{dc}_DCStockQty",
        "doh_store": f"{dc}_DOHStore",
        "doh_dc": f"{dc}_DOHDC",
        "avg_sale_90d": f"{dc}_AvgSaleQty90D",
    }


def compute_open_po_pipeline(salein: pd.DataFrame) -> pd.DataFrame:
    if "CJ_Item" not in salein.columns:
        raise ValueError("Combine_Sale_In.xlsx ต้องมีคอลัมน์ CJ_Item")

    df = salein.copy()
    df["CJ_Item"] = df["CJ_Item"].astype(str).str.strip()

    qty_col = "Actual_Qty" if "Actual_Qty" in df.columns else "Order_Qty"
    if qty_col not in df.columns:
        raise ValueError("Combine_Sale_In.xlsx ต้องมีคอลัมน์ Actual_Qty หรือ Order_Qty")
    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    received_blank = df["Received_Date"].isna() if "Received_Date" in df.columns else pd.Series([True] * len(df))

    if "Delivery_Status" in df.columns:
        status = df["Delivery_Status"].astype(str).str.lower()
        not_closed = ~status.isin(["received", "complete", "completed", "closed"])
    else:
        not_closed = True

    open_po = df[received_blank & not_closed].copy()

    dc_key = None
    if "DC_Code" in open_po.columns and open_po["DC_Code"].nunique() > 1:
        dc_key = "DC_Code"
    elif "DC_Name" in open_po.columns:
        dc_key = "DC_Name"

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


def build_oos_reconcile(stock: pd.DataFrame, salein: pd.DataFrame, cfg: AppConfig):
    latest = _pick_latest_stock_date(stock)
    snap = stock[pd.to_datetime(stock["Stock_Date"], errors="coerce") == latest].copy()
    snap["CJ_Item"] = snap["CJ_Item"].astype(str).str.strip()

    # Total
    required_total = [
        "CJ_Item","Description","Division","Category","Subcate",
        "Total_StoreStockQty","Total_DCStockQty","Total_DOHStore","Total_DOHDC","Total_AvgSaleQty90D"
    ]
    missing = [c for c in required_total if c not in snap.columns]
    if missing:
        raise ValueError(f"Stock file ขาดคอลัมน์ Total_*: {missing}")

    total = snap[required_total].copy()
    total = total.rename(columns={
        "Total_StoreStockQty":"store_stock_qty",
        "Total_DCStockQty":"dc_stock_qty",
        "Total_DOHStore":"doh_store",
        "Total_DOHDC":"doh_dc",
        "Total_AvgSaleQty90D":"avg_sale_qty_90d",
    })
    total["as_of_date"] = latest.date().isoformat()
    total["dc"] = "Total"

    total["oos_flag_store"] = (pd.to_numeric(total["doh_store"], errors="coerce").fillna(0) <= 0).astype(int)
    total["oos_flag_dc"]    = (pd.to_numeric(total["doh_dc"], errors="coerce").fillna(0) <= 0).astype(int)
    total["inside_outside_case"] = [
        make_inside_outside_case(ds, dd)
        for ds, dd in zip(pd.to_numeric(total["doh_store"], errors="coerce"), pd.to_numeric(total["doh_dc"], errors="coerce"))
    ]
    total["root_cause_code"] = np.where(total["inside_outside_case"] == "OK", "OK", "NEED_DC_LEVEL_REVIEW")
    total["root_cause_text"] = np.where(total["inside_outside_case"] == "OK", "No OOS detected", "Review at DC-level for root cause")
    total["notes"] = ""

    detail_rows = [total]

    # DC-level
    open_po = compute_open_po_pipeline(salein)
    open_po["CJ_Item"] = open_po["CJ_Item"].astype(str).str.strip()

    for dc in cfg.dcs:
        cols = _dc_cols(dc)
        required = ["CJ_Item","Description","Division","Category","Subcate", cols["store_qty"], cols["dc_qty"], cols["doh_store"], cols["doh_dc"], cols["avg_sale_90d"]]
        miss = [c for c in required if c not in snap.columns]
        if miss:
            raise ValueError(f"Stock file ขาดคอลัมน์ {dc}_*: {miss}")

        d = snap[required].copy()
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

        # simplified root cause with pipeline hint
        pipe = open_po[open_po["dc"].astype(str).str.contains(dc[-1], na=False)] \
            .groupby("CJ_Item", as_index=False)["pipeline_qty_open_po"].sum()

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
        d["notes"] = ""

        d = d.drop(columns=["pipeline_qty_open_po"])
        detail_rows.append(d)

    detail_df = pd.concat(detail_rows, ignore_index=True)

    # summary
    summary_rows = []
    for dc in ["Total"] + list(cfg.dcs):
        sub = detail_df[detail_df["dc"] == dc]
        oos = int(((sub["oos_flag_store"]==1) | (sub["oos_flag_dc"]==1)).sum())
        summary_rows.append({
            "as_of_date": latest.date().isoformat(),
            "dc": dc,
            "total_scm_assort": int(len(sub)),
            "oos_assort": oos,
            "perc_oos": float(oos / max(len(sub), 1)),
            "count_store_oos_dc_ok": int((sub["inside_outside_case"]=="STORE_OOS_DC_OK").sum()),
            "count_dc_oos_store_ok": int((sub["inside_outside_case"]=="DC_OOS_STORE_OK").sum()),
            "count_both_oos": int((sub["inside_outside_case"]=="BOTH_OOS").sum()),
            "count_ok": int((sub["inside_outside_case"]=="OK").sum()),
        })
    summary_df = pd.DataFrame(summary_rows)

    return detail_df, summary_df


def build_suggested_order_qty(stock: pd.DataFrame, salein: pd.DataFrame, moq: pd.DataFrame, saleout: pd.DataFrame, cfg: AppConfig) -> pd.DataFrame:
    latest = _pick_latest_stock_date(stock)
    snap = stock[pd.to_datetime(stock["Stock_Date"], errors="coerce") == latest].copy()
    snap["CJ_Item"] = snap["CJ_Item"].astype(str).str.strip()

    if "Sale_Date" not in saleout.columns or "Sale_Qty" not in saleout.columns:
        raise ValueError("SaleOut file ต้องมีคอลัมน์ Sale_Date และ Sale_Qty")

    so = saleout[["Sale_Date","CJ_Item","Sale_Qty"]].dropna()
    so["CJ_Item"] = so["CJ_Item"].astype(str).str.strip()
    so["Sale_Date"] = pd.to_datetime(so["Sale_Date"], errors="coerce")
    so["Sale_Qty"] = pd.to_numeric(so["Sale_Qty"], errors="coerce").fillna(0.0)

    last = so["Sale_Date"].max()
    start = last - pd.Timedelta(days=cfg.baseline_ma_days)
    so28 = so[(so["Sale_Date"] > start) & (so["Sale_Date"] <= last)]
    avg_daily = so28.groupby("CJ_Item", as_index=False)["Sale_Qty"].mean().rename(columns={"Sale_Qty":"avg_daily_sales"})

    open_po = compute_open_po_pipeline(salein)
    open_po["CJ_Item"] = open_po["CJ_Item"].astype(str).str.strip()

    out_rows = []
    for dc in cfg.dcs:
        cols = _dc_cols(dc)
        # required columns for dc
        if cols["dc_qty"] not in snap.columns:
            raise ValueError(f"Stock file ขาดคอลัมน์ {cols['dc_qty']}")

        df = snap[["CJ_Item","Description", cols["dc_qty"]]].copy()
        df = df.rename(columns={cols["dc_qty"]:"current_dc_stock_qty"})
        df["dc"] = dc
        df["as_of_date"] = latest.date().isoformat()

        df = df.merge(avg_daily, on="CJ_Item", how="left")
        df["avg_daily_sales"] = df["avg_daily_sales"].fillna(0.0)

        # moq/lt
        moq_use = moq.copy()
        moq_use["CJ_Item"] = moq_use["CJ_Item"].astype(str).str.strip()

        lt_col = f"Lead Time to {dc}"
        moq_col = f"{dc}_MOQ_per_SKU[CTN]"

        keep = ["CJ_Item","Supplier_Name","PC_Cartons", lt_col, moq_col]
        keep = [c for c in keep if c in moq_use.columns]
        moq_use = moq_use[keep].drop_duplicates("CJ_Item")

        df = df.merge(moq_use, on="CJ_Item", how="left")

        df["lead_time_days"] = pd.to_numeric(df.get(lt_col, np.nan), errors="coerce").fillna(0).astype(int)
        df["moq_ctn"] = pd.to_numeric(df.get(moq_col, np.nan), errors="coerce").fillna(0).astype(int)
        df["pc_cartons"] = pd.to_numeric(df.get("PC_Cartons", np.nan), errors="coerce").fillna(0).astype(int)

        # pipeline
        pipe = open_po[open_po["dc"].astype(str).str.contains(dc[-1], na=False)] \
            .groupby("CJ_Item", as_index=False)["pipeline_qty_open_po"].sum()
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
