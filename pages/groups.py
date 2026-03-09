import plotly.express as px
import streamlit as st

from components.tables import kpi_summary_table


def render_group(group_name: str):
    st.header(f"{group_name} — Detalle por Marca")
    kpi = st.session_state.get("kpi_table")
    if kpi is None:
        st.info("Sin datos disponibles.")
        return

    df = kpi[kpi["group"] == group_name].copy()
    if df.empty:
        st.warning(f"No hay marcas asignadas al grupo {group_name} todavia.")
        return

    fig_sc = px.scatter(
        df,
        x="cy_revenue",
        y="cy_margin_pct",
        size=df["stock_cy"].clip(lower=0) + 1,
        color="brand",
        hover_data=["ly_revenue", "budget_to_date_revenue", "days_stock"],
        title=f"{group_name} — Revenue vs Margen%",
        labels={"cy_revenue": "Revenue CY", "cy_margin_pct": "Margen% CY"},
    )
    fig_sc.update_yaxes(tickformat=".1%")
    fig_sc.update_layout(height=400)
    st.plotly_chart(fig_sc, use_container_width=True)

    df_p = df[df["budget_revenue"] > 0].copy()
    if not df_p.empty:
        fig_b = px.scatter(
            df_p,
            x="growth_real",
            y="budget_achievement",
            text="brand",
            color="brand",
            title="Crecimiento Real vs Consecucion Budget",
            labels={"growth_real": "Crec. vs LY LfL", "budget_achievement": "% Budget To-Date"},
        )
        fig_b.update_xaxes(tickformat=".1%")
        fig_b.update_yaxes(tickformat=".1%")
        fig_b.add_hline(y=1, line_dash="dash", line_color="gray")
        fig_b.add_vline(x=0, line_dash="dash", line_color="gray")
        fig_b.update_layout(height=360)
        st.plotly_chart(fig_b, use_container_width=True)

    stk = df[df["days_stock"].notna()].sort_values("days_stock", ascending=False)
    if not stk.empty:
        fig_s = px.bar(
            stk,
            x="brand",
            y="days_stock",
            color="brand",
            title="Dias de Stock",
            labels={"brand": "Marca", "days_stock": "Dias"},
            text_auto=".0f",
        )
        fig_s.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_s, use_container_width=True)

    kpi_summary_table(df)
