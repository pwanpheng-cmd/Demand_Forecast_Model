# streamlit_app.py
import io
import streamlit as st
import pandas as pd

from core import AppConfig, build_oos_reconcile, build_suggested_order_qty

st.set_page_config(page_title="Supply Chain Run Out / OOS Reconcile", layout="wide")

st.title("🏭 Supply Chain Run Out / OOS Reconcile (Streamlit)")

st.markdown("อัปโหลดไฟล์ แล้วกด **Run** เพื่อสร้างผลลัพธ์: OOS Reconcile + Suggested Order Qty")

with st.sidebar:
    st.header("⚙️ Settings")
    target_doh = st.number_input("Target DOH", min_value=1, max_value=120, value=21, step=1)
    baseline_ma_days = st.number_input("Baseline window (days)", min_value=7, max_value=90, value=28, step=1)
    st.divider()
    st.caption("รองรับ DC: DC1 / DC2 / DC4")

cfg = AppConfig(target_doh=int(target_doh), baseline_ma_days=int(baseline_ma_days))

st.subheader("📤 Upload Input Files")

col1, col2, col3 = st.columns(3)
with col1:
    f_saleout = st.file_uploader("Combine_SaleOut.xlsx", type=["xlsx"])
    f_stock   = st.file_uploader("Combine_Stock_CJ.xlsx", type=["xlsx"])
with col2:
    f_salein  = st.file_uploader("Combine_Sale_In.xlsx", type=["xlsx"])
    f_moq     = st.file_uploader("MOQ_LeadTime.xlsx", type=["xlsx"])
with col3:
    f_pro     = st.file_uploader("Combine_Pro.xlsx (optional)", type=["xlsx"])

def read_xlsx(uploaded_file, sheet_name=None):
    if uploaded_file is None:
        return None
    return pd.read_excel(uploaded_file, sheet_name=sheet_name)

run_btn = st.button("🚀 Run Run-Out / OOS Reconcile", type="primary")

if run_btn:
    if any(x is None for x in [f_saleout, f_stock, f_salein, f_moq]):
        st.error("กรุณาอัปโหลดไฟล์ให้ครบ: SaleOut, Stock_CJ, Sale_In, MOQ_LeadTime")
        st.stop()

    with st.spinner("Reading files..."):
        saleout = read_xlsx(f_saleout)  # first sheet is OK
        stock   = read_xlsx(f_stock)
        salein  = read_xlsx(f_salein, sheet_name="SaleIn_data")
        moq     = read_xlsx(f_moq, sheet_name="MOQ_LT")
        pro     = read_xlsx(f_pro) if f_pro is not None else pd.DataFrame()

    # Basic cleaning to match core expectations
    for df in [saleout, stock, salein, moq, pro]:
        if df is not None and "CJ_Item" in df.columns:
            df["CJ_Item"] = df["CJ_Item"].astype(str).str.strip()

    # Ensure date columns
    if "Sale_Date" in saleout.columns:
        saleout["Sale_Date"] = pd.to_datetime(saleout["Sale_Date"], errors="coerce")
    if "Stock_Date" in stock.columns:
        stock["Stock_Date"] = pd.to_datetime(stock["Stock_Date"], errors="coerce")
    for c in ["PO_Date","Reqired_Delivery_Date","Received_Date","Delivery_Date"]:
        if c in salein.columns:
            salein[c] = pd.to_datetime(salein[c], errors="coerce")
    for c in ["Promotion_Start_date","Promotion_End_date","Update_date"]:
        if c in pro.columns:
            pro[c] = pd.to_datetime(pro[c], errors="coerce")

    with st.spinner("Computing OOS Reconcile..."):
        oos_detail, oos_summary = build_oos_reconcile(stock, salein, pro, moq, cfg)

    with st.spinner("Computing Suggested Order Qty..."):
        suggested = build_suggested_order_qty(stock, salein, moq, saleout, cfg)

    st.success("Done ✅")

    st.subheader("📌 OOS Summary")
    st.dataframe(oos_summary, use_container_width=True)

    st.subheader("🔎 OOS Reconcile Detail (sample)")
    st.dataframe(oos_detail.head(200), use_container_width=True)

    st.subheader("🧾 Suggested Order Qty (sample)")
    st.dataframe(suggested.head(200), use_container_width=True)

    # Download buttons
    def to_csv_bytes(df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button(
            "⬇️ Download oos_reconcile_summary.csv",
            data=to_csv_bytes(oos_summary),
            file_name="oos_reconcile_summary.csv",
            mime="text/csv",
        )
    with c2:
        st.download_button(
            "⬇️ Download oos_reconcile_detail.csv",
            data=to_csv_bytes(oos_detail),
            file_name="oos_reconcile_detail.csv",
            mime="text/csv",
        )
    with c3:
        st.download_button(
            "⬇️ Download suggested_order_qty.csv",
            data=to_csv_bytes(suggested),
            file_name="suggested_order_qty.csv",
            mime="text/csv",
        )