import streamlit as st

from components.charts import recap_margin_chart, recap_revenue_chart
from components.tables import fmt_eur, fmt_pct


def render(group_colors):
    st.header("Recap — Grupos vs Totales")
    recap = st.session_state.get("recap_table")
    if recap is None:
        st.info("Sin datos disponibles.")
        return

    recap_revenue_chart(recap, group_colors)
    recap_margin_chart(recap, group_colors)

    rd = recap.copy()
    for c, fn in [
        ("cy_revenue", fmt_eur),
        ("ly_revenue", fmt_eur),
        ("growth_real", fmt_pct),
        ("cy_margin_pct", fmt_pct),
        ("budget_revenue", fmt_eur),
        ("budget_to_date_revenue", fmt_eur),
        ("budget_achievement", fmt_pct),
        ("budget_gap_eur", fmt_eur),
        ("budget_gap_pct", fmt_pct),
        ("stock_cy", fmt_eur),
    ]:
        if c in rd.columns:
            rd[c] = rd[c].apply(fn)
    st.dataframe(rd, use_container_width=True, hide_index=True)
