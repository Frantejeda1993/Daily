"""app.py — KPI Dashboard."""

import streamlit as st

from google_auth import login_page
from pages import config, groups, margins, recap, update
from persistence import load_persisted_state, save_state
from state_manager import GROUPS, GROUP_COLORS, MONTHS_ES, init_session_state, rebuild_kpis

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
if st.session_state.get("cy_sales") is None:
    with st.spinner("Cargando datos guardados..."):
        load_persisted_state(rebuild_kpis)

tab_labels = ["Margenes", "Recap", "2 Wheels", "Free Time", "Outdoor Tech", "Configuracion", "Actualizacion"]
tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_labels)

with tab0:
    margins.render(rebuild_kpis, GROUPS, GROUP_COLORS)
with tab1:
    recap.render(GROUP_COLORS)
with tab2:
    groups.render_group("2 Wheels")
with tab3:
    groups.render_group("Free Time")
with tab4:
    groups.render_group("Outdoor Tech")
with tab5:
    config.render(GROUPS, MONTHS_ES, save_state, rebuild_kpis)
with tab6:
    update.render(MONTHS_ES, save_state, rebuild_kpis)
