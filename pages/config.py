import pandas as pd
import streamlit as st

from components.forms import stock_uploader_grid
from data_processor import parse_budget, parse_families, parse_sales


def render(groups, months_es, save_state_fn, rebuild_fn):
    st.header("Configuracion — Inputs Anuales")

    known_brands = set()
    for source in ["cy_sales", "ly_sales"]:
        df_src = st.session_state.get(source)
        if isinstance(df_src, pd.DataFrame) and not df_src.empty and "brand" in df_src.columns:
            cleaned = df_src["brand"].dropna()
            if not cleaned.empty:
                known_brands.update(cleaned.unique().tolist())
    noise = {"NAN", "", "SIN CLASIFICAR", "0 - SIN CLASIFICAR", "0", "NONE"}
    known_brands -= noise
    known_brands = {b for b in known_brands if isinstance(b, str) and b.strip()}

    st.subheader("1. Archivos Maestros (Anuales)")
    with st.expander("Cargar archivos anuales", expanded=True):
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("**Ventas Año Anterior (LY)**")
            ly_file = st.file_uploader("Ventas LY", type=["xlsx", "xls", "csv"], key="ly_up")
            if ly_file is not None:
                fid = f"ly_{ly_file.name}_{ly_file.size}"
                if fid not in st.session_state["_processed_files"]:
                    with st.spinner("Procesando LY..."):
                        df_ly = parse_sales(ly_file)
                        st.session_state["ly_sales"] = df_ly
                        st.session_state["_processed_files"].add(fid)
                        save_state_fn("ly_sales", df_ly)
                    rebuild_fn()
                ly_loaded = st.session_state.get("ly_sales")
                if ly_loaded is not None:
                    st.success(f"{len(ly_loaded):,} registros LY cargados")

        with col_b:
            st.markdown("**Budget Año Actual (CY)**")
            bgt_file = st.file_uploader("Budget CY", type=["xlsx", "xls", "csv"], key="bgt_up")
            if bgt_file is not None:
                fid = f"bgt_{bgt_file.name}_{bgt_file.size}"
                if fid not in st.session_state["_processed_files"]:
                    with st.spinner("Procesando budget..."):
                        df_bgt = parse_budget(bgt_file)
                        st.session_state["budget"] = df_bgt
                        st.session_state["_processed_files"].add(fid)
                        save_state_fn("budget", df_bgt)
                    rebuild_fn()
                bgt_loaded = st.session_state.get("budget")
                if bgt_loaded is not None:
                    st.success(f"Budget: {len(bgt_loaded):,} marcas")

    st.subheader("Stock Año Anterior (LY) — por mes")
    st.caption("Formato por archivo: **Clave 1 | Codigo Articulo | Importe**")
    with st.expander("Cargador mensual Stock LY", expanded=False):
        stock_uploader_grid("stock_ly", "Stock LY", months_es, save_state_fn, rebuild_fn)
        stk_ly_dict = st.session_state["stock_ly"]
        if stk_ly_dict:
            loaded_months = [months_es[m - 1] for m in sorted(stk_ly_dict.keys())]
            st.info(f"Meses cargados: {', '.join(loaded_months)}")

    st.divider()
    st.subheader("2. Asignacion de Grupos por Marca")

    with st.expander("Importar asignacion desde archivo de familias (opcional)", expanded=False):
        st.caption(
            "Sube el Excel maestro (con hoja *INPUT (Anual) Familias*) o un CSV simple "
            "con columnas **Familia** (o Nombre) y **Columna1** (o Grupo). "
            "La columna **Familia** debe contener el codigo completo como *300 - FAMILIA SHOKZ*."
        )
        fam_file = st.file_uploader("Archivo de familias", type=["xlsx", "xls", "csv"], key="fam_up")
        if fam_file is not None:
            fid = f"fam_{fam_file.name}_{fam_file.size}"
            if fid not in st.session_state["_processed_files"]:
                try:
                    fam_df = parse_families(fam_file)
                    auto_map = {
                        row["brand_key"]: row["grupo"]
                        for _, row in fam_df.iterrows()
                        if pd.notna(row.get("grupo")) and row["grupo"] in groups and row.get("brand_key") in known_brands
                    }
                    existing = {k: v for k, v in st.session_state.get("family_map", {}).items() if k in known_brands}
                    merged_fm = {**existing, **auto_map}
                    st.session_state["family_map"] = merged_fm
                    st.session_state["_pending_fm"] = dict(merged_fm)
                    st.session_state["_processed_files"].add(fid)
                    save_state_fn("family_map", merged_fm)
                    st.success(
                        f"{len(auto_map)} marcas detectadas en ventas fueron completadas automaticamente. "
                        f"Revisa los desplegables y pulsa **Guardar** para recalcular."
                    )
                except Exception as exc:
                    st.error(f"Error leyendo familias: {exc}")
            else:
                st.info("Archivo ya procesado.")

    if not known_brands:
        st.info("Sube un archivo de ventas (LY o CY) para detectar las marcas.")
        return

    brands_sorted = sorted(known_brands)
    st.markdown(f"*{len(brands_sorted)} marcas detectadas*")
    current_fm = st.session_state.get("family_map", {})
    if not st.session_state["_pending_fm"] and current_fm:
        st.session_state["_pending_fm"] = dict(current_fm)

    options = ["Other"] + groups
    for i in range(0, len(brands_sorted), 5):
        chunk = brands_sorted[i:i + 5]
        cols = st.columns(5)
        for j, brand in enumerate(chunk):
            saved = st.session_state["_pending_fm"].get(brand, current_fm.get(brand, "Other"))
            idx = options.index(saved) if saved in options else 0
            sel = cols[j].selectbox(brand, options, index=idx, key=f"grp_{brand}")
            st.session_state["_pending_fm"][brand] = sel

    st.divider()
    if st.button("Guardar asignacion y recalcular KPIs", type="primary"):
        final_map = dict(st.session_state["_pending_fm"])
        st.session_state["family_map"] = final_map
        save_state_fn("family_map", final_map)
        rebuild_fn()
        st.success("Grupos guardados. KPIs actualizados.")
