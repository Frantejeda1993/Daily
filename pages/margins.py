import streamlit as st

from components.charts import waterfall_chart
from components.tables import fmt_delta_html, fmt_eur, fmt_pct, kpi_summary_table


def render(rebuild_fn, groups, group_colors):
    st.header("Dashboard de Margenes")

    ref_date = st.date_input("Fecha de referencia (CY)", value=st.session_state["reference_date"], key="ref_date_input")
    if ref_date != st.session_state["reference_date"]:
        st.session_state["reference_date"] = ref_date
        rebuild_fn()
        st.rerun()

    kpi = st.session_state.get("kpi_table")
    if kpi is None:
        st.info("Sube los archivos de ventas en la pestana **Actualizacion** para ver los KPIs.")
        return

    total_cy_rev = kpi["cy_revenue"].sum()
    total_ly_rev = kpi["ly_revenue"].sum()
    total_cy_mg_eur = kpi["cy_margin_eur"].sum()
    total_ly_mg_eur = kpi["ly_margin_eur"].sum()
    total_budget = kpi["budget_to_date_revenue"].sum()
    total_cy_mg_pct = total_cy_mg_eur / total_cy_rev if total_cy_rev else 0
    total_ly_mg_pct = total_ly_mg_eur / total_ly_rev if total_ly_rev else 0

    st.subheader("Totales Generales")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Revenue CY", fmt_eur(total_cy_rev), delta=f"{(total_cy_rev / total_ly_rev - 1) * 100:+.1f}% vs LY" if total_ly_rev else None)
    c2.metric("Revenue LY (LfL)", fmt_eur(total_ly_rev))
    c3.metric("Margen% CY", fmt_pct(total_cy_mg_pct), delta=f"{(total_cy_mg_pct - total_ly_mg_pct) * 100:+.1f}pp" if total_ly_mg_pct else None)
    c4.metric("Margen EUR CY", fmt_eur(total_cy_mg_eur))
    c5.metric("% vs Budget To-Date", fmt_pct(total_cy_rev / total_budget) if total_budget else "—")

    st.divider()
    gcols = st.columns(3)
    for i, grp in enumerate(groups):
        g = kpi[kpi["group"] == grp]
        g_rev = g["cy_revenue"].sum()
        g_ly = g["ly_revenue"].sum()
        g_mg = g["cy_margin_eur"].sum()
        g_pct = g_mg / g_rev if g_rev else 0
        with gcols[i]:
            st.markdown(
                f"<div class='kpi-card' style='border-left-color:{group_colors[grp]}'>"
                f"<b>{grp}</b><br>"
                f"Revenue: <b>{fmt_eur(g_rev)}</b> &nbsp; Margen: <b>{fmt_pct(g_pct)}</b> ({fmt_eur(g_mg)})<br>"
                f"vs LY: {fmt_delta_html((g_rev - g_ly) / g_ly if g_ly else None)}"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.divider()
    waterfall_chart(kpi, group_colors, "cy_revenue", "Revenue CY por Marca")
    st.subheader("Tabla detallada")
    kpi_summary_table(kpi)
