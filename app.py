"""app.py — KPI Dashboard."""

import streamlit as st

from google_auth import login_page
from pages import config, groups, margins, recap, update
from persistence import load_persisted_state, save_state
from state_manager import AppState, GROUPS, GROUP_COLORS, MONTHS_ES, init_session_state, rebuild_kpis
from ui_navigation import build_tab_index_map, build_tab_labels

st.set_page_config(
    page_title="KPI Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
<style>
[data-testid="stHeader"] { background: transparent; }
[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 700; }
.block-container { padding-top: 3.25rem; padding-bottom: 1rem; }
[data-baseweb="tab-list"] { margin-top: 0.5rem; }
.kpi-card {
    background: color-mix(in srgb, var(--secondary-background-color) 85%, transparent);
    color: var(--text-color);
    border-radius: 10px;
    padding: .9rem 1.1rem; margin-bottom: .4rem;
    border-left: 4px solid #4F81FF;
}
.positive { color: #22c55e; font-weight: 700; }
.negative { color: #f87171; font-weight: 700; }
.stMarkdown, .stText, p, label, .stMetricLabel, .stCaption {
    color: var(--text-color) !important;
}
</style>
""",
    unsafe_allow_html=True,
)

if not login_page():
    st.stop()

init_session_state()
state = AppState.from_session_state(st.session_state)
if not state.validate():
    st.error("Invalid application state")
    st.stop()

if st.session_state.get("cy_sales") is None:
    with st.spinner("Cargando datos guardados..."):
        load_persisted_state(rebuild_kpis)

tab_labels = build_tab_labels(GROUPS)
tab_index = build_tab_index_map(GROUPS)
tabs = st.tabs(tab_labels)

with tabs[tab_index["Margenes"]]:
    margins.render(rebuild_kpis, GROUPS, GROUP_COLORS)
with tabs[tab_index["Recap"]]:
    recap.render(GROUP_COLORS)
for group_name in GROUPS:
    with tabs[tab_index[group_name]]:
        groups.render_group(group_name)
with tabs[tab_index["Configuracion"]]:
    config.render(GROUPS, MONTHS_ES, save_state, rebuild_kpis)
with tabs[tab_index["Actualizacion"]]:
    update.render(MONTHS_ES, save_state, rebuild_kpis)
