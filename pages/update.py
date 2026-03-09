from datetime import datetime

import pandas as pd
import streamlit as st

from components.charts import monthly_trend_chart
from components.forms import stock_uploader_grid
from components.tables import fmt_eur
from data_processor import parse_sales


def render(months_es, save_state_fn, rebuild_fn):
    st.header("Actualizacion de Datos")

    last_upd = st.session_state.get("last_update")
    if last_upd:
        st.info(f"Ultima actualizacion: **{last_upd}**")
    else:
        st.warning("Sin datos cargados aun.")

    st.subheader("Ventas actuales (CY)")
    cy_file = st.file_uploader("Archivo de ventas CY (CSV o Excel)", type=["xlsx", "xls", "csv"], key="cy_up")
    if cy_file is not None:
        fid = f"cy_{cy_file.name}_{cy_file.size}"
        if fid not in st.session_state["_processed_files"]:
            with st.spinner("Procesando ventas CY..."):
                df_cy = parse_sales(cy_file)
                st.session_state["cy_sales"] = df_cy
                if "fecha" in df_cy.columns:
                    max_d = df_cy["fecha"].max()
                    if pd.notna(max_d):
                        st.session_state["reference_date"] = max_d.date()
                today_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                st.session_state["last_update"] = today_str
                st.session_state["_processed_files"].add(fid)
                save_state_fn("cy_sales", df_cy)
                save_state_fn("last_update", today_str)
                rebuild_fn()
        cy_loaded = st.session_state.get("cy_sales")
        if cy_loaded is not None:
            st.success(f"{len(cy_loaded):,} registros cargados. Fecha ref: {st.session_state['reference_date']}")

    st.subheader("Stock actual (CY) — por mes")
    st.caption("Formato: **Clave 1 | Codigo Articulo | Importe**")
    with st.expander("Cargador mensual Stock CY", expanded=True):
        stock_uploader_grid("stock_cy", "Stock CY", months_es, save_state_fn, rebuild_fn)
        stk_cy_dict = st.session_state["stock_cy"]
        if stk_cy_dict:
            loaded_months = [months_es[m - 1] for m in sorted(stk_cy_dict.keys())]
            st.info(f"Meses cargados: {', '.join(loaded_months)}")

    kpi_now = st.session_state.get("kpi_table")
    if kpi_now is not None:
        st.divider()
        st.subheader("Preview KPIs")
        m1, m2, m3 = st.columns(3)
        m1.metric("Revenue CY", fmt_eur(kpi_now["cy_revenue"].sum()))
        m2.metric("Revenue LY (LfL)", fmt_eur(kpi_now["ly_revenue"].sum()))
        m3.metric("Marcas activas", str(len(kpi_now)))
        monthly_trend_chart(st.session_state.get("cy_sales"))

    st.divider()
    if st.button("Cerrar sesion"):
        st.session_state["authenticated"] = False
        st.rerun()
