"""
google_auth.py
Handles:
  - Basic app login (single shared password via st.secrets or env vars)
  - Firebase Firestore upload / download helpers
  - Google Cloud Storage upload / download helpers (legacy fallback)
"""
import hmac
import importlib
import json
import logging
import os
import time

import streamlit as st
from google.cloud import storage
from google.oauth2 import service_account


logger = logging.getLogger(__name__)
FIRESTORE_CHUNK_SIZE = 900_000
FIRESTORE_BATCH_MAX_BYTES = 3_000_000
FIRESTORE_BATCH_MAX_WRITES = 200
MAX_LOGIN_ATTEMPTS = 5


def _coerce_binary_payload(payload) -> bytes:
    """Best-effort conversion for persisted payloads that may come back with mixed types."""
    if payload is None:
        return b""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, memoryview):
        return payload.tobytes()
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, str):
        try:
            return payload.encode("latin1")
        except UnicodeEncodeError:
            return payload.encode("utf-8", errors="ignore")
    try:
        return bytes(payload)
    except Exception:
        return b""


def _has_adc_credentials() -> bool:
    """Return True when ADC env vars are explicitly configured."""
    return bool(
        os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
    )

def _normalize_private_key(value: str) -> str:
    """Normalize private key formatting from secrets/env into PEM-compatible text."""
    if not isinstance(value, str):
        return ""
    key = value.strip()
    if "\\n" in key:
        key = key.replace("\\n", "\n")
    return key


def _extract_service_account_from_mapping(mapping) -> dict | None:
    """Extract service account dict from multiple accepted secret layouts."""
    if not mapping:
        return None

    # Layout A (recommended): [firebase.service_account]
    service_account_block = mapping.get("service_account") if hasattr(mapping, 'get') else None
    if service_account_block:
        return dict(service_account_block)

    # Layout B: service-account keys directly under [firebase]
    required_hints = {"project_id", "client_email", "private_key"}
    if hasattr(mapping, 'keys') and required_hints.issubset(set(mapping.keys())):
        return dict(mapping)

    return None



# ─────────────────────────────────────────────
# BASIC AUTH
# ─────────────────────────────────────────────


def _get_app_password() -> str:
    """Load shared app password from Streamlit secrets or env var APP_PASSWORD."""
    secret_password = st.secrets.get("APP_PASSWORD", "")
    env_password = os.environ.get("APP_PASSWORD", "")
    password = secret_password or env_password
    return password.strip() if isinstance(password, str) else ""


def _get_dev_password() -> str:
    """Return explicit development password fallback only in APP_ENV=dev."""
    if os.environ.get("APP_ENV", "").lower() != "dev":
        return ""
    return os.environ.get("APP_DEV_PASSWORD", "").strip()


def check_credentials(password: str) -> bool:
    app_password = _get_app_password() or _get_dev_password()
    if not app_password:
        return False
    return hmac.compare_digest(password or "", app_password)


def _can_attempt_login() -> tuple[bool, float]:
    now = time.time()
    lock_until = st.session_state.get("auth_lock_until", 0.0)
    if now < lock_until:
        return False, lock_until - now
    return True, 0.0


def _register_login_failure() -> None:
    failed_attempts = int(st.session_state.get("auth_failed_attempts", 0)) + 1
    st.session_state["auth_failed_attempts"] = failed_attempts
    if failed_attempts >= MAX_LOGIN_ATTEMPTS:
        delay = min(2 ** (failed_attempts - MAX_LOGIN_ATTEMPTS), 60)
        st.session_state["auth_lock_until"] = time.time() + delay


def login_page():
    """Render login form. Returns True if authenticated."""
    if st.session_state.get("authenticated"):
        return True

    st.markdown(
        """
        <style>
        .login-box {max-width:380px; margin:100px auto; padding:2rem;
                    border-radius:12px; box-shadow:0 4px 20px rgba(0,0,0,.12);}
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.container():
        st.markdown("<div class='login-box'>", unsafe_allow_html=True)
        st.title("🔐 KPI Dashboard")
        st.subheader("Iniciar sesión")
        password = st.text_input("Contraseña general", type="password", key="login_pw")
        can_try, remaining = _can_attempt_login()

        if not _get_app_password() and not _get_dev_password():
            st.error("No hay contraseña configurada. Define APP_PASSWORD para habilitar acceso.")

        if st.button("Entrar", use_container_width=True, disabled=not can_try):
            if not can_try:
                st.error(f"Demasiados intentos. Espera {remaining:.0f}s e inténtalo de nuevo.")
            elif check_credentials(password):
                st.session_state["authenticated"] = True
                st.session_state["username"] = "app_user"
                st.session_state["auth_failed_attempts"] = 0
                st.session_state["auth_lock_until"] = 0.0
                st.rerun()
            else:
                _register_login_failure()
                st.error("Credenciales incorrectas.")
        st.markdown("</div>", unsafe_allow_html=True)

    return False


# ─────────────────────────────────────────────
# FIREBASE FIRESTORE
# ─────────────────────────────────────────────


def _get_firebase_service_account_info() -> dict | None:
    """Read Firebase service account from Streamlit secrets or env JSON."""
    info = None

    try:
        if "firebase" in st.secrets:
            info = _extract_service_account_from_mapping(st.secrets["firebase"])
    except Exception as exc:
        logger.warning("Unable to read firebase secrets: %s", exc)

    if not info:
        raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "")
        if raw:
            try:
                info = json.loads(raw)
            except json.JSONDecodeError as exc:
                logger.error("Invalid FIREBASE_SERVICE_ACCOUNT_JSON: %s", exc)
                return None

    if not info:
        return None

    info = dict(info)
    info["private_key"] = _normalize_private_key(info.get("private_key", ""))

    missing = [k for k in ("project_id", "client_email", "private_key") if not info.get(k)]
    if missing:
        logger.error("Firebase service account is missing required field(s): %s", ", ".join(missing))
        return None

    return info


def _firestore_module():
    return importlib.import_module("google.cloud.firestore")


def _get_firestore_client():
    """Return a Firestore client, or None if not configured."""
    try:
        firestore = _firestore_module()
        service_account_info = _get_firebase_service_account_info()
        if service_account_info:
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            project_id = service_account_info.get("project_id")
            return firestore.Client(project=project_id, credentials=credentials)
        if not _has_adc_credentials():
            logger.info("Firestore disabled: no credentials configured")
            return None
        return firestore.Client()
    except Exception as e:
        logger.exception("Firestore client initialization failed")
        st.warning(f"Firestore no configurado: {e}")
        return None


def firestore_upload_pickle(collection: str, key: str, payload: bytes) -> bool:
    """Upload binary payload to Firestore (chunked when needed)."""
    client = _get_firestore_client()
    if client is None:
        return False
    try:
        firestore = _firestore_module()
        doc_ref = client.collection(collection).document(key)

        if len(payload) <= FIRESTORE_CHUNK_SIZE:
            doc_ref.set({"payload": payload, "updated_at": firestore.SERVER_TIMESTAMP})
            return True

        chunks = [
            payload[i:i + FIRESTORE_CHUNK_SIZE]
            for i in range(0, len(payload), FIRESTORE_CHUNK_SIZE)
        ]

        doc_ref.set({
            "chunked": True,
            "chunk_count": len(chunks),
            "updated_at": firestore.SERVER_TIMESTAMP,
        })

        def _commit_chunk_batch(chunk_batch):
            if not chunk_batch:
                return
            batch = client.batch()
            for chunk_ref, chunk_payload in chunk_batch:
                batch.set(chunk_ref, {"payload": chunk_payload})
            try:
                batch.commit()
            except Exception as batch_exc:
                err_msg = str(batch_exc).lower()
                if "request payload size exceeds the limit" not in err_msg:
                    raise
                logger.warning(
                    "Firestore batch commit exceeded payload limit for key=%s; falling back to single writes",
                    key,
                )
                for chunk_ref, chunk_payload in chunk_batch:
                    chunk_ref.set({"payload": chunk_payload})

        pending_chunks = []
        batch_bytes = 0
        batch_writes = 0
        for idx, chunk in enumerate(chunks):
            chunk_ref = doc_ref.collection("chunks").document(f"{idx:05d}")
            chunk_size = len(chunk)

            if (
                batch_writes >= FIRESTORE_BATCH_MAX_WRITES
                or (batch_writes > 0 and batch_bytes + chunk_size > FIRESTORE_BATCH_MAX_BYTES)
            ):
                _commit_chunk_batch(pending_chunks)
                pending_chunks = []
                batch_bytes = 0
                batch_writes = 0

            pending_chunks.append((chunk_ref, chunk))
            batch_writes += 1
            batch_bytes += chunk_size

        _commit_chunk_batch(pending_chunks)
        return True
    except Exception as e:
        logger.exception("Firestore upload failed for key=%s", key)
        st.error(f"Error guardando en Firestore: {e}")
        return False


def firestore_download_pickle(collection: str, key: str) -> bytes | None:
    """Download binary payload from Firestore. Returns None if not found."""
    client = _get_firestore_client()
    if client is None:
        return None
    try:
        doc = client.collection(collection).document(key).get()
        if not doc.exists:
            return None
        data = doc.to_dict() or {}

        if data.get("chunked"):
            chunk_count = int(data.get("chunk_count", 0) or 0)
            if chunk_count <= 0:
                raise ValueError(f"Invalid chunk_count for document '{key}'")
            chunk_docs = (
                client.collection(collection)
                .document(key)
                .collection("chunks")
                .stream()
            )
            chunks = {
                d.id: _coerce_binary_payload((d.to_dict() or {}).get("payload"))
                for d in chunk_docs
            }
            ordered = [chunks.get(f"{i:05d}", b"") for i in range(chunk_count)]
            if not all(ordered):
                raise ValueError(f"Missing Firestore chunk(s) for key '{key}'")
            return b"".join(ordered)

        payload = data.get("payload")
        coerced = _coerce_binary_payload(payload)
        return coerced or None
    except Exception as exc:
        logger.exception("Firestore download failed for key=%s", key)
        st.error(f"Error cargando estado '{key}' desde Firestore: {exc}")
        return None


# ─────────────────────────────────────────────
# GOOGLE CLOUD STORAGE (legacy fallback)
# ─────────────────────────────────────────────


def _get_gcs_client():
    """Return a GCS client, or None if not configured."""
    try:
        creds_json = os.environ.get("GCS_CREDENTIALS_JSON", "")
        if creds_json:
            info = json.loads(creds_json)
            credentials = service_account.Credentials.from_service_account_info(info)
            project = info.get("project_id")
            return storage.Client(project=project, credentials=credentials)
        if not _has_adc_credentials():
            logger.info("GCS disabled: no credentials configured")
            return None
        return storage.Client()
    except Exception as e:
        logger.exception("GCS client initialization failed")
        st.warning(f"GCS no configurado: {e}")
        return None


def gcs_upload(bucket_name: str, destination_blob: str, data: bytes):
    """Upload bytes to GCS."""
    client = _get_gcs_client()
    if client is None:
        return False
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_string(data)
        return True
    except Exception as e:
        logger.exception("GCS upload failed for blob=%s", destination_blob)
        st.error(f"Error subiendo a GCS: {e}")
        return False


def gcs_download(bucket_name: str, blob_name: str) -> bytes | None:
    """Download blob bytes from GCS. Returns None if not found."""
    client = _get_gcs_client()
    if client is None:
        return None
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.download_as_bytes()
    except Exception as exc:
        logger.exception("GCS download failed for blob=%s", blob_name)
        st.error(f"Error descargando de GCS ({blob_name}): {exc}")
        return None


def gcs_list(bucket_name: str, prefix: str = "") -> list[str]:
    """List blob names in bucket."""
    client = _get_gcs_client()
    if client is None:
        return []
    try:
        return [b.name for b in client.list_blobs(bucket_name, prefix=prefix)]
    except Exception as exc:
        logger.exception("GCS list failed for bucket=%s prefix=%s", bucket_name, prefix)
        st.error(f"Error listando GCS: {exc}")
        return []
