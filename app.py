"""
app.py — KPI Dashboard
"""
import hashlib
import json
import os
import pickle
from datetime import date, datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data_processor import (
    parse_sales, parse_stock, parse_budget, parse_families,
    lfl_filter, merge_kpis, build_recap, project_month_end, summarise_sales,
)
from google_auth import (
    login_page,
    gcs_upload,
    gcs_download,
    firestore_upload_pickle,
    firestore_download_pickle,
)

# ─────────────────────────────────────────────
# PAGE CONFIG  — must be first Streamlit call
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="KPI Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ─────────────────────────────────────────────
# MINIMAL CSS  (FIX #1: no white-space/font rules on tabs)
# ─────────────────────────────────────────────
st.markdown("""
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
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────
if not login_page():
    st.stop()

# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────
MONTHS_ES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
             "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]
GROUPS = ["2 Wheels", "Free Time", "Outdoor Tech"]
GROUP_COLORS = {
    "2 Wheels": "#4F81FF",
    "Free Time": "#FF7F3F",
    "Outdoor Tech": "#2EC4B6",
    "Other": "#888888",
}

# ─────────────────────────────────────────────
# SESSION STATE DEFAULTS
# ─────────────────────────────────────────────
_DEFAULTS = {
    "cy_sales":         None,
    "ly_sales":         None,
    "stock_cy":         {},        # {month_int: DataFrame(brand, stock_value)}
    "stock_ly":         {},
    "budget":           None,
    "family_map":       {},        # {brand_uppercase: group}
    "_pending_fm":      {},        # staging area — not saved until button pressed
    "_processed_files": set(),     # FIX #2: track already-processed file names
    "last_update":      None,
    "reference_date":   date.today(),
    "kpi_table":        None,
    "recap_table":      None,
}
for k, v in _DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v

GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
GCS_PREFIX = "kpi_data/"
FIRESTORE_COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "kpi_state")

# ─────────────────────────────────────────────
# PERSISTENCE (Firestore first, GCS fallback)
# ─────────────────────────────────────────────

def _serialize_state(key, obj):
    if obj is None:
        return None
    if isinstance(obj, pd.DataFrame):
        return {"type": "dataframe", "value": obj.to_json(orient="split", date_format="iso")}
    if key in {"stock_cy", "stock_ly"}:
        return {
            "type": "stock_dict",
            "value": {str(month): df.to_json(orient="split", date_format="iso") for month, df in obj.items()},
        }
    if isinstance(obj, date):
        return {"type": "date", "value": obj.isoformat()}
    if isinstance(obj, (dict, list, str, int, float, bool)):
        return {"type": "json", "value": obj}
    raise TypeError(f"Tipo no soportado para persistencia segura: {type(obj)}")


def _deserialize_state(serialized):
    if not serialized:
        return None
    typ = serialized.get("type")
    value = serialized.get("value")
    if typ == "dataframe":
        return pd.read_json(value, orient="split")
    if typ == "stock_dict":
        return {int(month): pd.read_json(df_json, orient="split") for month, df_json in value.items()}
    if typ == "date":
        return date.fromisoformat(value)
    if typ == "json":
        return value
    return None


def _save_state(key, obj):
    serialized = _serialize_state(key, obj)
    if serialized is None:
        return
    payload = json.dumps(serialized, ensure_ascii=False).encode("utf-8")
    if firestore_upload_pickle(FIRESTORE_COLLECTION, key, payload):
        return
    if GCS_BUCKET:
        gcs_upload(GCS_BUCKET, GCS_PREFIX + key + ".json", payload)


def _load_state(key):
    def _decode_payload(raw_payload):
        if raw_payload is None:
            return None

        try:
            if hasattr(raw_payload, "__len__") and len(raw_payload) == 0:
                return None
        except Exception:
            # Some client wrappers can raise decoding/typing errors on len()/truthiness.
            pass

        if isinstance(raw_payload, memoryview):
            raw_payload = raw_payload.tobytes()
        elif isinstance(raw_payload, bytearray):
            raw_payload = bytes(raw_payload)
        elif isinstance(raw_payload, str):
            try:
                text_payload = raw_payload.strip()
            except Exception:
                text_payload = raw_payload

            # Some old deployments accidentally stored binary bytes as text.
            # Try to recover with the most common reversible encodings first.
            try:
                raw_payload = text_payload.encode("latin1")
            except UnicodeEncodeError:
                raw_payload = text_payload.encode("utf-8", errors="ignore")

            # Firestore/GCS tools may also expose payloads as base64 text.
            try:
                import base64
                decoded_b64 = base64.b64decode(text_payload, validate=True)
                if decoded_b64:
                    raw_payload = decoded_b64
            except Exception:
                pass

        if isinstance(raw_payload, dict):
            return raw_payload

        if not isinstance(raw_payload, bytes):
            return None

        # Fast path for legacy pickled payloads (pickle protocol marker).
        if raw_payload.startswith(b"\x80"):
            try:
                return pickle.loads(raw_payload)
            except Exception:
                pass

        # Preferred format: UTF-8 JSON payload written by _save_state.
        try:
            return json.loads(raw_payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass

        # Legacy compatibility: previously persisted pickled payloads.
        try:
            return pickle.loads(raw_payload)
        except Exception:
            return None

    raw = firestore_download_pickle(FIRESTORE_COLLECTION, key)
    if raw:
        try:
            decoded = _decode_payload(raw)
            return _deserialize_state(decoded)
        except Exception:
            return None
    if not GCS_BUCKET:
        return None
    raw = gcs_download(GCS_BUCKET, GCS_PREFIX + key + ".json")
    try:
        decoded = _decode_payload(raw) if raw else None
        return _deserialize_state(decoded)
    except Exception:
        return None


def load_persisted_state():
    for k in ["cy_sales", "ly_sales", "stock_cy", "stock_ly", "budget", "family_map", "last_update"]:
        try:
            val = _load_state(k)
        except Exception:
            val = None
        if val is not None:
            st.session_state[k] = val
    if st.session_state.get("cy_sales") is not None and st.session_state.get("ly_sales") is not None:
        rebuild_kpis()


if st.session_state.get("cy_sales") is None:
    with st.spinner("Cargando datos guardados..."):
        load_persisted_state()

# ─────────────────────────────────────────────
# STOCK HELPERS
# ─────────────────────────────────────────────

def get_combined_stock(stock_dict: dict) -> pd.DataFrame:
    """Combine all monthly stock dicts into a single brand|stock_value DataFrame."""
    if not stock_dict:
        return pd.DataFrame(columns=["brand","stock_value"])
    frames = [df for df in stock_dict.values() if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame(columns=["brand","stock_value"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.groupby("brand", as_index=False)["stock_value"].sum()

# ─────────────────────────────────────────────
# KPI REBUILD
# ─────────────────────────────────────────────

def rebuild_kpis():
    ref    = st.session_state["reference_date"]
    cy     = st.session_state["cy_sales"]
    ly     = st.session_state["ly_sales"]
    bgt    = st.session_state["budget"]
    stk_cy = get_combined_stock(st.session_state["stock_cy"])
    stk_ly = get_combined_stock(st.session_state["stock_ly"])

    if cy is None or ly is None:
        st.session_state["kpi_table"]   = None
        st.session_state["recap_table"] = None
        return

    cy_lfl = lfl_filter(cy, ref)
    ly_lfl = lfl_filter(ly, ref)
    kpi = merge_kpis(cy_lfl, ly_lfl, bgt, stk_cy, stk_ly, ref)

    fm = st.session_state["family_map"]
    kpi["group"] = kpi["brand"].map(fm).fillna("Other")

    proj = project_month_end(cy, ref)
    kpi = kpi.merge(proj[["brand","projected_revenue","elapsed_pct"]], on="brand", how="left")

    st.session_state["kpi_table"]   = kpi
    st.session_state["recap_table"] = build_recap(kpi, fm)

# ─────────────────────────────────────────────
# FORMATTERS
# ─────────────────────────────────────────────

def fmt_eur(v):
    return "—" if pd.isna(v) else f"€{v:,.0f}"

def fmt_pct(v, d=1):
    return "—" if pd.isna(v) else f"{v*100:.{d}f}%"

def fmt_delta_html(v, pct=True):
    if pd.isna(v): return "—"
    sign = "▲" if v >= 0 else "▼"
    css  = "positive" if v >= 0 else "negative"
    val  = fmt_pct(v) if pct else fmt_eur(v)
    return f"<span class='{css}'>{sign} {val}</span>"

# ─────────────────────────────────────────────
# SHARED UI COMPONENTS
# ─────────────────────────────────────────────

def kpi_summary_table(df: pd.DataFrame, title: str = ""):
    if df is None or df.empty:
        st.info("Sin datos.")
        return
    if title:
        st.markdown(f"### {title}")
    want = ["brand","brand_status","cy_revenue","ly_revenue","growth_real","cy_margin_pct",
            "margin_delta_pts","cy_margin_eur","budget_revenue","budget_to_date_revenue",
            "budget_achievement","budget_gap_eur","budget_gap_pct","mix_contribution_pct",
            "margin_contribution_pct","cy_units","revenue_per_unit","margin_per_unit",
            "stock_cy","days_stock","projected_revenue","metric_window"]
    cols = [c for c in want if c in df.columns]
    disp = df[cols].copy()
    rn = {
        "brand":"Marca","brand_status":"Tipo Marca","cy_revenue":"Revenue CY","ly_revenue":"Revenue LY (LfL)",
        "growth_real":"Crec. Real","cy_margin_pct":"Margen% CY",
        "margin_delta_pts":"Delta Margen","cy_margin_eur":"Margen EUR CY",
        "budget_revenue":"Budget Anual","budget_to_date_revenue":"Budget To-Date",
        "budget_achievement":"% Cumpl. Budget To-Date","budget_gap_eur":"Gap Budget €",
        "budget_gap_pct":"Gap Budget %","mix_contribution_pct":"Mix % Revenue",
        "margin_contribution_pct":"Contrib. Margen %","cy_units":"Unidades CY",
        "revenue_per_unit":"Revenue/Unidad","margin_per_unit":"Margen/Unidad",
        "stock_cy":"Stock CY","days_stock":"Dias Stock","projected_revenue":"Proyeccion Mes",
        "metric_window":"Ventana KPI",
    }
    disp = disp.rename(columns=rn)
    fmt_map = {
        "Revenue CY": fmt_eur, "Revenue LY (LfL)": fmt_eur, "Crec. Real": fmt_pct,
        "Margen% CY": fmt_pct, "Margen EUR CY": fmt_eur, "Budget Anual": fmt_eur,
        "Budget To-Date": fmt_eur, "% Cumpl. Budget To-Date": fmt_pct,
        "Gap Budget €": fmt_eur, "Gap Budget %": fmt_pct, "Mix % Revenue": fmt_pct,
        "Contrib. Margen %": fmt_pct, "Revenue/Unidad": fmt_eur, "Margen/Unidad": fmt_eur,
        "Stock CY": fmt_eur, "Proyeccion Mes": fmt_eur,
    }
    for c, fn in fmt_map.items():
        if c in disp.columns:
            disp[c] = disp[c].apply(fn)
    if "Delta Margen" in disp.columns:
        disp["Delta Margen"] = disp["Delta Margen"].apply(
            lambda v: f"{v*100:+.1f}pp" if not pd.isna(v) else "—")
    if "Dias Stock" in disp.columns:
        disp["Dias Stock"] = disp["Dias Stock"].apply(
            lambda v: f"{v:.0f}d" if not pd.isna(v) else "—")
    st.dataframe(disp, use_container_width=True, hide_index=True)


def waterfall_chart(kpi_df, col="cy_revenue", title="Revenue por Marca"):
    if kpi_df is None or kpi_df.empty: return
    df = kpi_df.sort_values(col, ascending=False).head(20)
    fig = px.bar(df, x="brand", y=col, color="group", color_discrete_map=GROUP_COLORS,
                 title=title, labels={"brand":"Marca", col:"EUR"}, text_auto=".3s")
    fig.update_layout(height=400, margin=dict(t=40, b=10), legend_title_text="Grupo")
    st.plotly_chart(fig, use_container_width=True)


def monthly_trend_chart(sales_df, brand=None):
    if sales_df is None or sales_df.empty: return
    df = sales_df.copy()
    if brand:
        df = df[df["brand"] == brand]
    if df.empty: return
    df["month"] = df["fecha"].dt.to_period("M").astype(str)
    monthly = df.groupby("month", as_index=False)["importe"].sum()
    fig = px.line(monthly, x="month", y="importe", markers=True,
                  title=f"Tendencia mensual {'— '+brand if brand else '(Total)'}",
                  labels={"month":"Mes","importe":"Revenue EUR"})
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)


def stock_uploader_grid(state_key: str, label_prefix: str):
    """
    FIX #2: Grid of 12 monthly uploaders.
    Uses _processed_files set to avoid re-processing already-loaded files,
    preventing the infinite rerun loop.
    """
    stock_dict = st.session_state[state_key]
    processed  = st.session_state["_processed_files"]
    changed    = False

    rows = [list(range(1, 5)), list(range(5, 9)), list(range(9, 13))]
    for row_months in rows:
        cols = st.columns(4)
        for j, m in enumerate(row_months):
            month_name = MONTHS_ES[m - 1]
            already_loaded = m in stock_dict
            label = f"{label_prefix} {month_name}" + (" ✅" if already_loaded else "")
            with cols[j]:
                uploaded = st.file_uploader(
                    label,
                    type=["xlsx","xls","csv"],
                    key=f"{state_key}_m{m}",
                )
                if uploaded is not None:
                    # FIX #2: unique file ID = key + name + size
                    file_hash = hashlib.sha256(uploaded.getvalue()).hexdigest()
                    file_id = f"{state_key}_m{m}_{file_hash}"
                    if file_id not in processed:
                        try:
                            df_stk = parse_stock(uploaded)
                            stock_dict[m] = df_stk
                            processed.add(file_id)
                            changed = True
                            st.success(f"{len(df_stk):,} marcas")
                        except Exception as e:
                            st.error(str(e))

    if changed:
        st.session_state[state_key]          = stock_dict
        st.session_state["_processed_files"] = processed
        _save_state(state_key, stock_dict)
        rebuild_kpis()
        # No st.rerun() here — let Streamlit re-render naturally


# ─────────────────────────────────────────────
# TABS  (FIX #1: plain string list, no emoji, no extra CSS on tabs)
# ─────────────────────────────────────────────
tab_labels = [
    "Margenes",
    "Recap",
    "2 Wheels",
    "Free Time",
    "Outdoor Tech",
    "Configuracion",
    "Actualizacion",
]
tab0, tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_labels)

kpi   = st.session_state.get("kpi_table")
recap = st.session_state.get("recap_table")

# ══════════════════════════════════════════════
# TAB 0 — MARGENES
# ══════════════════════════════════════════════
with tab0:
    st.header("Dashboard de Margenes")

    ref_date = st.date_input(
        "Fecha de referencia (CY)",
        value=st.session_state["reference_date"],
        key="ref_date_input",
    )
    if ref_date != st.session_state["reference_date"]:
        st.session_state["reference_date"] = ref_date
        rebuild_kpis()
        st.rerun()

    if kpi is None:
        st.info("Sube los archivos de ventas en la pestana **Actualizacion** para ver los KPIs.")
    else:
        total_cy_rev    = kpi["cy_revenue"].sum()
        total_ly_rev    = kpi["ly_revenue"].sum()
        total_cy_mg_eur = kpi["cy_margin_eur"].sum()
        total_ly_mg_eur = kpi["ly_margin_eur"].sum()
        total_budget    = kpi["budget_to_date_revenue"].sum()
        total_cy_mg_pct = total_cy_mg_eur / total_cy_rev if total_cy_rev else 0
        total_ly_mg_pct = total_ly_mg_eur / total_ly_rev if total_ly_rev else 0

        st.subheader("Totales Generales")
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Revenue CY", fmt_eur(total_cy_rev),
                  delta=f"{(total_cy_rev/total_ly_rev-1)*100:+.1f}% vs LY" if total_ly_rev else None)
        c2.metric("Revenue LY (LfL)", fmt_eur(total_ly_rev))
        c3.metric("Margen% CY", fmt_pct(total_cy_mg_pct),
                  delta=f"{(total_cy_mg_pct-total_ly_mg_pct)*100:+.1f}pp" if total_ly_mg_pct else None)
        c4.metric("Margen EUR CY", fmt_eur(total_cy_mg_eur))
        c5.metric("% vs Budget To-Date", fmt_pct(total_cy_rev/total_budget) if total_budget else "—")

        st.divider()
        gcols = st.columns(3)
        for i, grp in enumerate(GROUPS):
            g = kpi[kpi["group"] == grp]
            g_rev = g["cy_revenue"].sum(); g_ly = g["ly_revenue"].sum()
            g_mg  = g["cy_margin_eur"].sum()
            g_pct = g_mg / g_rev if g_rev else 0
            with gcols[i]:
                st.markdown(
                    f"<div class='kpi-card' style='border-left-color:{GROUP_COLORS[grp]}'>"
                    f"<b>{grp}</b><br>"
                    f"Revenue: <b>{fmt_eur(g_rev)}</b> &nbsp; Margen: <b>{fmt_pct(g_pct)}</b> ({fmt_eur(g_mg)})<br>"
                    f"vs LY: {fmt_delta_html((g_rev-g_ly)/g_ly if g_ly else None)}"
                    f"</div>", unsafe_allow_html=True)

        st.divider()
        waterfall_chart(kpi, "cy_revenue", "Revenue CY por Marca")
        st.subheader("Tabla detallada")
        kpi_summary_table(kpi)


# ══════════════════════════════════════════════
# TAB 1 — RECAP
# ══════════════════════════════════════════════
with tab1:
    st.header("Recap — Grupos vs Totales")
    if recap is None:
        st.info("Sin datos disponibles.")
    else:
        fig_r = go.Figure()
        for _, row in recap.iterrows():
            fig_r.add_trace(go.Bar(
                name=row["group"],
                x=["Revenue CY","Revenue LY (LfL)","Budget To-Date"],
                y=[row["cy_revenue"], row["ly_revenue"], row["budget_to_date_revenue"]],
                marker_color=GROUP_COLORS.get(row["group"], "#888"),
                text=[fmt_eur(row["cy_revenue"]), fmt_eur(row["ly_revenue"]), fmt_eur(row["budget_to_date_revenue"])],
                textposition="outside",
            ))
        fig_r.update_layout(barmode="group", title="Revenue por Grupo", height=420, yaxis_title="EUR")
        st.plotly_chart(fig_r, use_container_width=True)

        fig_mg = go.Figure()
        for _, row in recap.iterrows():
            fig_mg.add_trace(go.Bar(
                name=row["group"],
                x=["Margen% CY","Margen% LY"],
                y=[row["cy_margin_pct"]*100, row["ly_margin_pct"]*100],
                marker_color=GROUP_COLORS.get(row["group"], "#888"),
            ))
        fig_mg.update_layout(barmode="group", title="Comparativa Margen% CY vs LY",
                              height=340, yaxis_title="%")
        st.plotly_chart(fig_mg, use_container_width=True)

        rd = recap.copy()
        for c, fn in [("cy_revenue",fmt_eur),("ly_revenue",fmt_eur),("growth_real",fmt_pct),
                      ("cy_margin_pct",fmt_pct),("budget_revenue",fmt_eur), ("budget_to_date_revenue",fmt_eur),
                      ("budget_achievement",fmt_pct), ("budget_gap_eur",fmt_eur), ("budget_gap_pct",fmt_pct),("stock_cy",fmt_eur)]:
            if c in rd.columns:
                rd[c] = rd[c].apply(fn)
        st.dataframe(rd, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
# VERTICAL GROUP TAB FUNCTION
# ══════════════════════════════════════════════
def vertical_tab(group_name: str):
    st.header(f"{group_name} — Detalle por Marca")
    if kpi is None:
        st.info("Sin datos disponibles.")
        return
    df = kpi[kpi["group"] == group_name].copy()
    if df.empty:
        st.warning(f"No hay marcas asignadas al grupo {group_name} todavia.")
        return

    fig_sc = px.scatter(
        df, x="cy_revenue", y="cy_margin_pct",
        size=df["stock_cy"].clip(lower=0) + 1,
        color="brand",
        hover_data=["ly_revenue","budget_to_date_revenue","days_stock"],
        title=f"{group_name} — Revenue vs Margen%",
        labels={"cy_revenue":"Revenue CY","cy_margin_pct":"Margen% CY"},
    )
    fig_sc.update_yaxes(tickformat=".1%")
    fig_sc.update_layout(height=400)
    st.plotly_chart(fig_sc, use_container_width=True)

    df_p = df[df["budget_revenue"] > 0].copy()
    if not df_p.empty:
        fig_b = px.scatter(
            df_p, x="growth_real", y="budget_achievement",
            text="brand", color="brand",
            title="Crecimiento Real vs Consecucion Budget",
            labels={"growth_real":"Crec. vs LY LfL","budget_achievement":"% Budget To-Date"},
        )
        fig_b.update_xaxes(tickformat=".1%")
        fig_b.update_yaxes(tickformat=".1%")
        fig_b.add_hline(y=1, line_dash="dash", line_color="gray")
        fig_b.add_vline(x=0, line_dash="dash", line_color="gray")
        fig_b.update_layout(height=360)
        st.plotly_chart(fig_b, use_container_width=True)

    stk = df[df["days_stock"].notna()].sort_values("days_stock", ascending=False)
    if not stk.empty:
        fig_s = px.bar(stk, x="brand", y="days_stock", color="brand",
                       title="Dias de Stock", labels={"brand":"Marca","days_stock":"Dias"},
                       text_auto=".0f")
        fig_s.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_s, use_container_width=True)

    kpi_summary_table(df)


with tab2: vertical_tab("2 Wheels")
with tab3: vertical_tab("Free Time")
with tab4: vertical_tab("Outdoor Tech")


# ══════════════════════════════════════════════
# TAB 5 — CONFIGURACION
# ══════════════════════════════════════════════
with tab5:
    st.header("Configuracion — Inputs Anuales")

    # Marcas detectadas SOLO desde archivos de ventas (no budget)
    known_brands: set = set()
    for src in ["cy_sales", "ly_sales"]:
        df_src = st.session_state.get(src)
        if df_src is not None and "brand" in df_src.columns:
            known_brands.update(df_src["brand"].dropna().unique().tolist())
    noise = {"NAN", "", "SIN CLASIFICAR", "0 - SIN CLASIFICAR", "0", "NONE"}
    known_brands -= noise
    known_brands = {b for b in known_brands if isinstance(b, str) and b.strip()}

    # ── 1. Archivos anuales ────────────────────────
    st.subheader("1. Archivos Maestros (Anuales)")
    with st.expander("Cargar archivos anuales", expanded=True):
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Ventas Año Anterior (LY)**")
            ly_file = st.file_uploader("Ventas LY", type=["xlsx","xls","csv"], key="ly_up")
            if ly_file is not None:
                fid = f"ly_{ly_file.name}_{ly_file.size}"
                if fid not in st.session_state["_processed_files"]:
                    with st.spinner("Procesando LY..."):
                        df_ly = parse_sales(ly_file)
                        st.session_state["ly_sales"] = df_ly
                        st.session_state["_processed_files"].add(fid)
                        _save_state("ly_sales", df_ly)
                    rebuild_kpis()
                ly_loaded = st.session_state.get("ly_sales")
                if ly_loaded is not None:
                    st.success(f"{len(ly_loaded):,} registros LY cargados")

        with col_b:
            st.markdown("**Budget Año Actual (CY)**")
            bgt_file = st.file_uploader("Budget CY", type=["xlsx","xls","csv"], key="bgt_up")
            if bgt_file is not None:
                fid = f"bgt_{bgt_file.name}_{bgt_file.size}"
                if fid not in st.session_state["_processed_files"]:
                    with st.spinner("Procesando budget..."):
                        df_bgt = parse_budget(bgt_file)
                        st.session_state["budget"] = df_bgt
                        st.session_state["_processed_files"].add(fid)
                        _save_state("budget", df_bgt)
                    rebuild_kpis()
                bgt_loaded = st.session_state.get("budget")
                if bgt_loaded is not None:
                    st.success(f"Budget: {len(bgt_loaded):,} marcas")

    # ── Stock LY mensual ──────────────────────────
    st.subheader("Stock Año Anterior (LY) — por mes")
    st.caption("Formato por archivo: **Clave 1 | Codigo Articulo | Importe**")
    with st.expander("Cargador mensual Stock LY", expanded=False):
        stock_uploader_grid("stock_ly", "Stock LY")
        stk_ly_dict = st.session_state["stock_ly"]
        if stk_ly_dict:
            loaded_months = [MONTHS_ES[m-1] for m in sorted(stk_ly_dict.keys())]
            st.info(f"Meses cargados: {', '.join(loaded_months)}")

    st.divider()

    # ── 2. Asignacion de Grupos ────────────────────
    st.subheader("2. Asignacion de Grupos por Marca")

    # FIX #3: import from Familias file — generates brand_key (uppercase) → group mapping
    with st.expander("Importar asignacion desde archivo de familias (opcional)", expanded=False):
        st.caption(
            "Sube el Excel maestro (con hoja *INPUT (Anual) Familias*) o un CSV simple "
            "con columnas **Familia** (o Nombre) y **Columna1** (o Grupo). "
            "La columna **Familia** debe contener el codigo completo como *300 - FAMILIA SHOKZ*."
        )
        fam_file = st.file_uploader(
            "Archivo de familias",
            type=["xlsx","xls","csv"],
            key="fam_up",
        )
        if fam_file is not None:
            fid = f"fam_{fam_file.name}_{fam_file.size}"
            if fid not in st.session_state["_processed_files"]:
                try:
                    fam_df = parse_families(fam_file)
                    # fam_df has: brand_key (uppercase), grupo (title-cased)
                    auto_map = {
                        row["brand_key"]: row["grupo"]
                        for _, row in fam_df.iterrows()
                        if (
                            pd.notna(row.get("grupo"))
                            and row["grupo"] in GROUPS
                            and row.get("brand_key") in known_brands
                        )
                    }

                    # Merge only for already-detected sales brands.
                    existing = {
                        k: v for k, v in st.session_state.get("family_map", {}).items()
                        if k in known_brands
                    }
                    merged_fm = {**existing, **auto_map}
                    st.session_state["family_map"]  = merged_fm
                    st.session_state["_pending_fm"] = dict(merged_fm)
                    st.session_state["_processed_files"].add(fid)
                    _save_state("family_map", merged_fm)
                    st.success(
                        f"{len(auto_map)} marcas detectadas en ventas fueron completadas automaticamente. "
                        f"Revisa los desplegables y pulsa **Guardar** para recalcular."
                    )
                except Exception as e:
                    st.error(f"Error leyendo familias: {e}")
            else:
                # Show how many were imported
                st.info("Archivo ya procesado.")

    if known_brands:
        brands_sorted = sorted(known_brands)
        st.markdown(f"*{len(brands_sorted)} marcas detectadas*")

        current_fm = st.session_state.get("family_map", {})
        # Init _pending_fm once from saved family_map (FIX #3)
        if not st.session_state["_pending_fm"] and current_fm:
            st.session_state["_pending_fm"] = dict(current_fm)

        OPTIONS = ["Other"] + GROUPS

        # 5-column grid of selectboxes
        for i in range(0, len(brands_sorted), 5):
            chunk = brands_sorted[i:i+5]
            cols = st.columns(5)
            for j, brand in enumerate(chunk):
                saved = st.session_state["_pending_fm"].get(
                    brand, current_fm.get(brand, "Other"))
                idx = OPTIONS.index(saved) if saved in OPTIONS else 0
                # FIX #3: write to _pending_fm only — NO rebuild on every change
                sel = cols[j].selectbox(brand, OPTIONS, index=idx, key=f"grp_{brand}")
                st.session_state["_pending_fm"][brand] = sel

        st.divider()
        # Only save + rebuild when button clicked
        if st.button("Guardar asignacion y recalcular KPIs", type="primary"):
            final_map = dict(st.session_state["_pending_fm"])
            st.session_state["family_map"] = final_map
            _save_state("family_map", final_map)
            rebuild_kpis()
            st.success("Grupos guardados. KPIs actualizados.")
    else:
        st.info("Sube un archivo de ventas (LY o CY) para detectar las marcas.")


# ══════════════════════════════════════════════
# TAB 6 — ACTUALIZACION
# ══════════════════════════════════════════════
with tab6:
    st.header("Actualizacion de Datos")

    last_upd = st.session_state.get("last_update")
    if last_upd:
        st.info(f"Ultima actualizacion: **{last_upd}**")
    else:
        st.warning("Sin datos cargados aun.")

    # ── Ventas CY ─────────────────────────────────
    st.subheader("Ventas actuales (CY)")
    cy_file = st.file_uploader(
        "Archivo de ventas CY (CSV o Excel)",
        type=["xlsx","xls","csv"],
        key="cy_up",
    )
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
                st.session_state["last_update"]    = today_str
                st.session_state["_processed_files"].add(fid)
                _save_state("cy_sales", df_cy)
                _save_state("last_update", today_str)
                rebuild_kpis()
        cy_loaded = st.session_state.get("cy_sales")
        if cy_loaded is not None:
            st.success(
                f"{len(cy_loaded):,} registros cargados. "
                f"Fecha ref: {st.session_state['reference_date']}"
            )

    # ── Stock CY mensual ───────────────────────────
    st.subheader("Stock actual (CY) — por mes")
    st.caption("Formato: **Clave 1 | Codigo Articulo | Importe**")
    with st.expander("Cargador mensual Stock CY", expanded=True):
        stock_uploader_grid("stock_cy", "Stock CY")
        stk_cy_dict = st.session_state["stock_cy"]
        if stk_cy_dict:
            loaded_months = [MONTHS_ES[m-1] for m in sorted(stk_cy_dict.keys())]
            st.info(f"Meses cargados: {', '.join(loaded_months)}")

    # ── Preview ────────────────────────────────────
    kpi_now = st.session_state.get("kpi_table")
    if kpi_now is not None:
        st.divider()
        st.subheader("Preview KPIs")
        m1, m2, m3 = st.columns(3)
        m1.metric("Revenue CY",       fmt_eur(kpi_now["cy_revenue"].sum()))
        m2.metric("Revenue LY (LfL)", fmt_eur(kpi_now["ly_revenue"].sum()))
        m3.metric("Marcas activas",   str(len(kpi_now)))
        monthly_trend_chart(st.session_state.get("cy_sales"))

    st.divider()
    if st.button("Cerrar sesion"):
        st.session_state["authenticated"] = False
        st.rerun()
