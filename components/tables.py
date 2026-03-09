import pandas as pd
import streamlit as st


def fmt_eur(v):
    return "—" if pd.isna(v) else f"€{v:,.0f}"


def fmt_pct(v, d=1):
    return "—" if pd.isna(v) else f"{v * 100:.{d}f}%"


def fmt_delta_html(v, pct=True):
    if pd.isna(v):
        return "—"
    sign = "▲" if v >= 0 else "▼"
    css = "positive" if v >= 0 else "negative"
    val = fmt_pct(v) if pct else fmt_eur(v)
    return f"<span class='{css}'>{sign} {val}</span>"


def kpi_summary_table(df: pd.DataFrame, title: str = ""):
    if df is None or df.empty:
        st.info("Sin datos.")
        return
    if title:
        st.markdown(f"### {title}")

    want = [
        "brand", "brand_status", "cy_revenue", "ly_revenue", "growth_real", "cy_margin_pct",
        "margin_delta_pts", "cy_margin_eur", "budget_revenue", "budget_to_date_revenue",
        "budget_achievement", "budget_gap_eur", "budget_gap_pct", "mix_contribution_pct",
        "margin_contribution_pct", "cy_units", "revenue_per_unit", "margin_per_unit",
        "stock_cy", "days_stock", "projected_revenue", "metric_window",
    ]
    cols = [c for c in want if c in df.columns]
    disp = df[cols].copy()
    disp = disp.rename(
        columns={
            "brand": "Marca",
            "brand_status": "Tipo Marca",
            "cy_revenue": "Revenue CY",
            "ly_revenue": "Revenue LY (LfL)",
            "growth_real": "Crec. Real",
            "cy_margin_pct": "Margen% CY",
            "margin_delta_pts": "Delta Margen",
            "cy_margin_eur": "Margen EUR CY",
            "budget_revenue": "Budget Anual",
            "budget_to_date_revenue": "Budget To-Date",
            "budget_achievement": "% Cumpl. Budget To-Date",
            "budget_gap_eur": "Gap Budget €",
            "budget_gap_pct": "Gap Budget %",
            "mix_contribution_pct": "Mix % Revenue",
            "margin_contribution_pct": "Contrib. Margen %",
            "cy_units": "Unidades CY",
            "revenue_per_unit": "Revenue/Unidad",
            "margin_per_unit": "Margen/Unidad",
            "stock_cy": "Stock CY",
            "days_stock": "Dias Stock",
            "projected_revenue": "Proyeccion Mes",
            "metric_window": "Ventana KPI",
        }
    )

    fmt_map = {
        "Revenue CY": fmt_eur,
        "Revenue LY (LfL)": fmt_eur,
        "Crec. Real": fmt_pct,
        "Margen% CY": fmt_pct,
        "Margen EUR CY": fmt_eur,
        "Budget Anual": fmt_eur,
        "Budget To-Date": fmt_eur,
        "% Cumpl. Budget To-Date": fmt_pct,
        "Gap Budget €": fmt_eur,
        "Gap Budget %": fmt_pct,
        "Mix % Revenue": fmt_pct,
        "Contrib. Margen %": fmt_pct,
        "Revenue/Unidad": fmt_eur,
        "Margen/Unidad": fmt_eur,
        "Stock CY": fmt_eur,
        "Proyeccion Mes": fmt_eur,
    }
    for column, formatter in fmt_map.items():
        if column in disp.columns:
            disp[column] = disp[column].apply(formatter)

    if "Delta Margen" in disp.columns:
        disp["Delta Margen"] = disp["Delta Margen"].apply(lambda v: f"{v * 100:+.1f}pp" if not pd.isna(v) else "—")
    if "Dias Stock" in disp.columns:
        disp["Dias Stock"] = disp["Dias Stock"].apply(lambda v: f"{v:.0f}d" if not pd.isna(v) else "—")

    st.dataframe(disp, use_container_width=True, hide_index=True)
