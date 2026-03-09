import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.tables import fmt_eur


def waterfall_chart(kpi_df, group_colors, col="cy_revenue", title="Revenue por Marca"):
    if kpi_df is None or kpi_df.empty:
        return
    df = kpi_df.sort_values(col, ascending=False).head(20)
    fig = px.bar(
        df,
        x="brand",
        y=col,
        color="group",
        color_discrete_map=group_colors,
        title=title,
        labels={"brand": "Marca", col: "EUR"},
        text_auto=".3s",
    )
    fig.update_layout(height=400, margin=dict(t=40, b=10), legend_title_text="Grupo")
    st.plotly_chart(fig, use_container_width=True)


def monthly_trend_chart(sales_df, brand=None):
    if sales_df is None or sales_df.empty:
        return
    df = sales_df.copy()
    if brand:
        df = df[df["brand"] == brand]
    if df.empty:
        return
    df["month"] = df["fecha"].dt.to_period("M").astype(str)
    monthly = df.groupby("month", as_index=False)["importe"].sum()
    fig = px.line(
        monthly,
        x="month",
        y="importe",
        markers=True,
        title=f"Tendencia mensual {'— ' + brand if brand else '(Total)'}",
        labels={"month": "Mes", "importe": "Revenue EUR"},
    )
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)


def recap_revenue_chart(recap_df, group_colors):
    fig = go.Figure()
    for _, row in recap_df.iterrows():
        fig.add_trace(
            go.Bar(
                name=row["group"],
                x=["Revenue CY", "Revenue LY (LfL)", "Budget To-Date"],
                y=[row["cy_revenue"], row["ly_revenue"], row["budget_to_date_revenue"]],
                marker_color=group_colors.get(row["group"], "#888"),
                text=[fmt_eur(row["cy_revenue"]), fmt_eur(row["ly_revenue"]), fmt_eur(row["budget_to_date_revenue"])],
                textposition="outside",
            )
        )
    fig.update_layout(barmode="group", title="Revenue por Grupo", height=420, yaxis_title="EUR")
    st.plotly_chart(fig, use_container_width=True)


def recap_margin_chart(recap_df, group_colors):
    fig = go.Figure()
    for _, row in recap_df.iterrows():
        fig.add_trace(
            go.Bar(
                name=row["group"],
                x=["Margen% CY", "Margen% LY"],
                y=[row["cy_margin_pct"] * 100, row["ly_margin_pct"] * 100],
                marker_color=group_colors.get(row["group"], "#888"),
            )
        )
    fig.update_layout(barmode="group", title="Comparativa Margen% CY vs LY", height=340, yaxis_title="%")
    st.plotly_chart(fig, use_container_width=True)
