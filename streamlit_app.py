# streamlit_app.py
import streamlit as st
import pandas as pd

from core import AppConfig, normalize_inputs, build_oos_reconcile, build_suggested_order_qty

st.set_page_config(page_title="Run Out / OOS Reconcile", layout="wide")
st.title("🏭 Run Out / OOS Reconcile")

with st.sidebar:
    st.header("⚙️ Settings")
    target_doh = st.number_input("Target DOH", 1, 120, 21, 1)
    baseline_ma_days = st.number_input("Baseline window (days)", 7, 90, 28, 1)
    cfg = AppConfig(target_doh=int(target_doh), baseline_ma_days=int(baseline_ma_days))

st.subheader("📤 Upload Input Files (Browse from your computer)")

c1, c2, c3 = st.columns(3)
with c1:
    f_saleout = st.file_uploader("Combine_SaleOut.xlsx", type=["xlsx"])
    f_stock = st.file_uploader("Combine_Stock_CJ.xlsx", type=["xlsx"])
with c2:
    f_salein = st.file_uploader("Combine_Sale_In.xlsx (sheet: SaleIn_data)", type=["xlsx"])
    f_moq = st.file_uploader("MOQ_LeadTime.xlsx (sheet: MOQ_LT)", type=["xlsx"])
with c3:
    f_pro = st.file_uploader("Combine_Pro.xlsx (optional)", type=["xlsx"])

def read_excel(uploaded, sheet_name=None) -> pd.DataFrame:
    # อ่านจากไฟล์ที่ผู้ใช้ browse เข้ามาเท่านั้น
    return pd.read_excel(uploaded, sheet_name=sheet_name)

run_btn = st.button("🚀 Run", type="primary")

if run_btn:
    if any(x is None for x in [f_saleout, f_stock, f_salein, f_moq]):
        st.error("กรุณาอัปโหลดให้ครบ: SaleOut, Stock_CJ, Sale_In, MOQ_LeadTime")
        st.stop()

    with st.spinner("Reading files..."):
        saleout = read_excel(f_saleout)
        stock = read_excel(f_stock)

        # สำคัญ: ชื่อ sheet ต้องตรง
        salein = read_excel(f_salein, sheet_name="SaleIn_data")
        moq = read_excel(f_moq, sheet_name="MOQ_LT")

        pro = read_excel(f_pro) if f_pro is not None else pd.DataFrame()

    saleout, stock, salein, moq, pro = normalize_inputs(saleout, stock, salein, moq, pro)

    with st.spinner("Computing OOS Reconcile..."):
        oos_detail, oos_summary = build_oos_reconcile(stock, salein, cfg)

    with st.spinner("Computing Suggested Order Qty..."):
        suggested = build_suggested_order_qty(stock, salein, moq, saleout, cfg)

    st.success("Done ✅")

    st.subheader("📌 OOS Summary")
    st.dataframe(oos_summary, use_container_width=True)

    st.subheader("🔎 OOS Reconcile Detail (Top 200)")
    st.dataframe(oos_detail.head(200), use_container_width=True)

    st.subheader("🧾 Suggested Order Qty (Top 200)")
    st.dataframe(suggested.head(200), use_container_width=True)

    def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
        return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

    d1, d2, d3 = st.columns(3)
    with d1:
        st.download_button("⬇️ Download oos_summary.csv", df_to_csv_bytes(oos_summary), "oos_reconcile_summary.csv", "text/csv")
    with d2:
        st.download_button("⬇️ Download oos_detail.csv", df_to_csv_bytes(oos_detail), "oos_reconcile_detail.csv", "text/csv")
    with d3:
        st.download_button("⬇️ Download suggested_order.csv", df_to_csv_bytes(suggested), "suggested_order_qty.csv", "text/csv")
