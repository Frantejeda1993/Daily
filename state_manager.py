from datetime import date

import pandas as pd
import streamlit as st

from data_processor import build_recap, lfl_filter, merge_kpis, project_month_end

MONTHS_ES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
GROUPS = ["2 Wheels", "Free Time", "Outdoor Tech"]
GROUP_COLORS = {
    "2 Wheels": "#4F81FF",
    "Free Time": "#FF7F3F",
    "Outdoor Tech": "#2EC4B6",
    "Other": "#888888",
}
DEFAULTS = {
    "cy_sales": None,
    "ly_sales": None,
    "stock_cy": {},
    "stock_ly": {},
    "budget": None,
    "family_map": {},
    "_pending_fm": {},
    "_processed_files": set(),
    "last_update": None,
    "reference_date": date.today(),
    "kpi_table": None,
    "recap_table": None,
}


def init_session_state():
    for key, value in DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = value


def get_combined_stock(stock_dict: dict) -> pd.DataFrame:
    if not stock_dict:
        return pd.DataFrame(columns=["brand", "stock_value"])
    frames = [df for df in stock_dict.values() if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame(columns=["brand", "stock_value"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.groupby("brand", as_index=False)["stock_value"].sum()


def rebuild_kpis():
    ref = st.session_state["reference_date"]
    cy = st.session_state["cy_sales"]
    ly = st.session_state["ly_sales"]
    bgt = st.session_state["budget"]
    stk_cy = get_combined_stock(st.session_state["stock_cy"])
    stk_ly = get_combined_stock(st.session_state["stock_ly"])

    if cy is None or ly is None:
        st.session_state["kpi_table"] = None
        st.session_state["recap_table"] = None
        return

    cy_lfl = lfl_filter(cy, ref)
    ly_lfl = lfl_filter(ly, ref)
    kpi = merge_kpis(cy_lfl, ly_lfl, bgt, stk_cy, stk_ly, ref)

    family_map = st.session_state["family_map"]
    kpi["group"] = kpi["brand"].map(family_map).fillna("Other")

    projection = project_month_end(cy, ref)
    kpi = kpi.merge(projection[["brand", "projected_revenue", "elapsed_pct"]], on="brand", how="left")

    st.session_state["kpi_table"] = kpi
    st.session_state["recap_table"] = build_recap(kpi, family_map)
