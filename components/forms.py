import hashlib

import streamlit as st

from data_processor import parse_stock


def stock_uploader_grid(state_key: str, label_prefix: str, months_es, save_state_fn, rebuild_fn):
    stock_dict = st.session_state[state_key]
    processed = st.session_state["_processed_files"]
    changed = False

    rows = [list(range(1, 5)), list(range(5, 9)), list(range(9, 13))]
    for row_months in rows:
        cols = st.columns(4)
        for j, month in enumerate(row_months):
            month_name = months_es[month - 1]
            already_loaded = month in stock_dict
            label = f"{label_prefix} {month_name}" + (" ✅" if already_loaded else "")
            with cols[j]:
                uploaded = st.file_uploader(label, type=["xlsx", "xls", "csv"], key=f"{state_key}_m{month}")
                if uploaded is not None:
                    file_hash = hashlib.sha256(uploaded.getvalue()).hexdigest()
                    file_id = f"{state_key}_m{month}_{file_hash}"
                    if file_id not in processed:
                        try:
                            df_stk = parse_stock(uploaded)
                            stock_dict[month] = df_stk
                            processed.add(file_id)
                            changed = True
                            st.success(f"{len(df_stk):,} marcas")
                        except Exception as exc:
                            st.error(str(exc))

    if changed:
        st.session_state[state_key] = stock_dict
        st.session_state["_processed_files"] = processed
        save_state_fn(state_key, stock_dict)
        rebuild_fn()
