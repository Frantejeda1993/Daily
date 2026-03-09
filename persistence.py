import json
import os
import pickle
import logging
from datetime import date, datetime
from io import StringIO

import pandas as pd
import streamlit as st

from google_auth import gcs_download, gcs_upload, firestore_download_pickle, firestore_upload_pickle

GCS_BUCKET = os.environ.get("GCS_BUCKET", "")
GCS_PREFIX = "kpi_data/"
FIRESTORE_COLLECTION = os.environ.get("FIRESTORE_COLLECTION", "kpi_state")

logger = logging.getLogger(__name__)


def serialize_state(key, obj):
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


def _decode_dataframe_payload(df_payload):
    try:
        if isinstance(df_payload, pd.DataFrame):
            return df_payload
        if isinstance(df_payload, str):
            try:
                return pd.read_json(StringIO(df_payload), orient="split")
            except Exception:
                pass
            try:
                df_payload = df_payload.encode("latin1")
            except UnicodeEncodeError:
                return None

        if isinstance(df_payload, (bytes, bytearray, memoryview)):
            raw_bytes = bytes(df_payload)
            trimmed_bytes = raw_bytes.lstrip()
            if trimmed_bytes.startswith(b"\x80"):
                try:
                    unpickled = pickle.loads(trimmed_bytes)
                    return unpickled if isinstance(unpickled, pd.DataFrame) else None
                except Exception:
                    return None
            try:
                return pd.read_json(StringIO(trimmed_bytes.decode("utf-8")), orient="split")
            except Exception:
                pass
            try:
                unpickled = pickle.loads(trimmed_bytes)
                return unpickled if isinstance(unpickled, pd.DataFrame) else None
            except Exception:
                return None
        return None
    except Exception:
        return None


def deserialize_state(serialized):
    if not serialized:
        return None
    if isinstance(serialized, pd.DataFrame):
        return serialized
    if isinstance(serialized, (list, str, int, float, bool, date)):
        return serialized
    if isinstance(serialized, dict) and "type" not in serialized and "value" not in serialized:
        return serialized

    typ = serialized.get("type")
    value = serialized.get("value")
    if typ == "dataframe":
        return _decode_dataframe_payload(value)
    if typ == "stock_dict":
        if not isinstance(value, dict):
            return {}
        parsed = {}
        for month, df_json in value.items():
            decoded_df = _decode_dataframe_payload(df_json)
            if decoded_df is not None:
                parsed[int(month)] = decoded_df
        return parsed
    if typ == "date":
        if isinstance(value, date):
            return value
        if isinstance(value, datetime):
            return value.date()
        return date.fromisoformat(value)
    if typ == "json":
        return value
    return None


def save_state(key, obj):
    serialized = serialize_state(key, obj)
    if serialized is None:
        return
    payload = json.dumps(serialized, ensure_ascii=False).encode("utf-8")
    if firestore_upload_pickle(FIRESTORE_COLLECTION, key, payload):
        return
    if GCS_BUCKET:
        gcs_upload(GCS_BUCKET, GCS_PREFIX + key + ".json", payload)


def _decode_payload(raw_payload):
    try:
        if raw_payload is None:
            return None
        try:
            if hasattr(raw_payload, "__len__") and len(raw_payload) == 0:
                return None
        except Exception:
            pass

        if isinstance(raw_payload, memoryview):
            raw_payload = raw_payload.tobytes()
        elif isinstance(raw_payload, bytearray):
            raw_payload = bytes(raw_payload)
        elif isinstance(raw_payload, dict):
            return raw_payload
        elif isinstance(raw_payload, str):
            try:
                text_payload = raw_payload.strip()
            except Exception:
                text_payload = raw_payload
            try:
                return json.loads(text_payload)
            except Exception:
                pass
            try:
                import base64

                decoded_b64 = base64.b64decode(text_payload, validate=True)
                raw_payload = decoded_b64 if decoded_b64 else text_payload.encode("latin1")
            except Exception:
                try:
                    raw_payload = text_payload.encode("latin1")
                except UnicodeEncodeError:
                    raw_payload = text_payload.encode("utf-8", errors="ignore")

        if not isinstance(raw_payload, bytes):
            return None

        trimmed = raw_payload.lstrip()
        if trimmed.startswith(b"\x80"):
            try:
                return pickle.loads(trimmed)
            except Exception:
                pass
        try:
            return json.loads(trimmed.decode("utf-8"))
        except Exception:
            pass
        try:
            return json.loads(trimmed.decode("latin1"))
        except Exception:
            pass
        try:
            return pickle.loads(trimmed)
        except Exception:
            return None
    except Exception:
        return None


def load_state(key):
    raw = firestore_download_pickle(FIRESTORE_COLLECTION, key)
    if raw is not None:
        return deserialize_state(_decode_payload(raw))
    if not GCS_BUCKET:
        return None
    raw = gcs_download(GCS_BUCKET, GCS_PREFIX + key + ".json")
    return deserialize_state(_decode_payload(raw)) if raw is not None else None


def load_persisted_state(rebuild_fn):
    load_errors = []
    for key in ["cy_sales", "ly_sales", "stock_cy", "stock_ly", "budget", "family_map", "last_update"]:
        try:
            value = load_state(key)
        except Exception as exc:
            logger.exception("Error loading persisted key '%s'", key)
            load_errors.append((key, exc))
            value = None
        if value is not None:
            st.session_state[key] = value

    if load_errors:
        st.warning("No se pudieron recuperar algunos datos guardados. Revisa los logs para más detalle.")

    if st.session_state.get("cy_sales") is not None and st.session_state.get("ly_sales") is not None:
        try:
            rebuild_fn()
        except Exception:
            logger.exception("Error rebuilding KPI tables after loading persisted state")
            st.session_state["kpi_table"] = None
            st.session_state["recap_table"] = None
            st.error("Se cargaron datos guardados, pero falló el recálculo de KPIs.")
