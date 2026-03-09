from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Optional

import pandas as pd
import streamlit as st

from data_processor import build_recap, lfl_filter, merge_kpis, project_month_end

MONTHS_ES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]
GROUPS = ["2 Wheels", "Free Time", "Outdoor Tech"]
GROUP_COLORS = {
    "2 Wheels": "#4F81FF",
    "Free Time": "#FF7F3F",
    "Outdoor Tech": "#2EC4B6",
    "Other": "#888888",
}
DEFAULTS = {
    "cy_sales": None,
    "ly_sales": None,
    "stock_cy": {},
    "stock_ly": {},
    "budget": None,
    "family_map": {},
    "_pending_fm": {},
    "_processed_files": set(),
    "last_update": None,
    "reference_date": date.today(),
    "kpi_table": None,
    "recap_table": None,
}


@dataclass
class AppState:
    cy_sales: Optional[pd.DataFrame] = None
    ly_sales: Optional[pd.DataFrame] = None
    stock_cy: dict = field(default_factory=dict)
    stock_ly: dict = field(default_factory=dict)
    budget: Optional[pd.DataFrame] = None
    family_map: dict = field(default_factory=dict)
    pending_family_map: dict = field(default_factory=dict)
    processed_files: set = field(default_factory=set)
    last_update: Optional[str] = None
    reference_date: date = field(default_factory=date.today)
    kpi_table: Optional[pd.DataFrame] = None
    recap_table: Optional[pd.DataFrame] = None

    @classmethod
    def from_session_state(cls, session_state) -> "AppState":
        return cls(
            cy_sales=session_state.get("cy_sales"),
            ly_sales=session_state.get("ly_sales"),
            stock_cy=session_state.get("stock_cy", {}),
            stock_ly=session_state.get("stock_ly", {}),
            budget=session_state.get("budget"),
            family_map=session_state.get("family_map", {}),
            pending_family_map=session_state.get("_pending_fm", {}),
            processed_files=session_state.get("_processed_files", set()),
            last_update=session_state.get("last_update"),
            reference_date=session_state.get("reference_date", date.today()),
            kpi_table=session_state.get("kpi_table"),
            recap_table=session_state.get("recap_table"),
        )

    def validate(self) -> bool:
        if self.reference_date is None or not isinstance(self.reference_date, date):
            return False
        if self.stock_cy is None or not isinstance(self.stock_cy, dict):
            return False
        if self.stock_ly is None or not isinstance(self.stock_ly, dict):
            return False
        if self.family_map is None or not isinstance(self.family_map, dict):
            return False
        if self.pending_family_map is None or not isinstance(self.pending_family_map, dict):
            return False
        if self.processed_files is None or not isinstance(self.processed_files, set):
            return False
        return True

    def to_dict(self) -> dict[str, Any]:
        return {
            "cy_sales": self.cy_sales,
            "ly_sales": self.ly_sales,
            "stock_cy": self.stock_cy,
            "stock_ly": self.stock_ly,
            "budget": self.budget,
            "family_map": self.family_map,
            "_pending_fm": self.pending_family_map,
            "_processed_files": self.processed_files,
            "last_update": self.last_update,
            "reference_date": self.reference_date,
            "kpi_table": self.kpi_table,
            "recap_table": self.recap_table,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "AppState":
        if not isinstance(data, dict):
            return cls()
        return cls(
            cy_sales=data.get("cy_sales"),
            ly_sales=data.get("ly_sales"),
            stock_cy=data.get("stock_cy", {}),
            stock_ly=data.get("stock_ly", {}),
            budget=data.get("budget"),
            family_map=data.get("family_map", {}),
            pending_family_map=data.get("_pending_fm", {}),
            processed_files=data.get("_processed_files", set()),
            last_update=data.get("last_update"),
            reference_date=data.get("reference_date", date.today()),
            kpi_table=data.get("kpi_table"),
            recap_table=data.get("recap_table"),
        )

    def sync_to_session(self, session_state):
        for key, value in self.to_dict().items():
            session_state[key] = value


def _default_value(value):
    if isinstance(value, (dict, list, set)):
        return deepcopy(value)
    return value


def init_session_state():
    for key, value in DEFAULTS.items():
        if key not in st.session_state:
            st.session_state[key] = _default_value(value)

    app_state = AppState.from_session_state(st.session_state)
    if not app_state.validate():
        app_state = AppState()
        app_state.sync_to_session(st.session_state)


def get_combined_stock(stock_dict: dict) -> pd.DataFrame:
    if not stock_dict:
        return pd.DataFrame(columns=["brand", "stock_value"])
    frames = [df for df in stock_dict.values() if df is not None and not df.empty]
    if not frames:
        return pd.DataFrame(columns=["brand", "stock_value"])
    combined = pd.concat(frames, ignore_index=True)
    return combined.groupby("brand", as_index=False)["stock_value"].sum()


def rebuild_kpis():
    app_state = AppState.from_session_state(st.session_state)
    if not app_state.validate():
        st.session_state["kpi_table"] = None
        st.session_state["recap_table"] = None
        return

    ref = app_state.reference_date
    cy = app_state.cy_sales
    ly = app_state.ly_sales
    bgt = app_state.budget
    stk_cy = get_combined_stock(app_state.stock_cy)
    stk_ly = get_combined_stock(app_state.stock_ly)

    if cy is None or ly is None:
        st.session_state["kpi_table"] = None
        st.session_state["recap_table"] = None
        return

    cy_lfl = lfl_filter(cy, ref)
    ly_lfl = lfl_filter(ly, ref)
    kpi = merge_kpis(cy_lfl, ly_lfl, bgt, stk_cy, stk_ly, ref)

    family_map = app_state.family_map
    kpi["group"] = kpi["brand"].map(family_map).fillna("Other")

    projection = project_month_end(cy, ref)
    kpi = kpi.merge(projection[["brand", "projected_revenue", "elapsed_pct"]], on="brand", how="left")

    st.session_state["kpi_table"] = kpi
    st.session_state["recap_table"] = build_recap(kpi, family_map)
