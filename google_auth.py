"""
google_auth.py
Handles:
  - Basic app login (single shared password via st.secrets or env vars)
  - Firebase Firestore upload / download helpers
  - Google Cloud Storage upload / download helpers (legacy fallback)
"""
import importlib
import json
import os

import streamlit as st
from google.cloud import storage
from google.oauth2 import service_account


FIRESTORE_CHUNK_SIZE = 900_000

# ─────────────────────────────────────────────
# BASIC AUTH
# ─────────────────────────────────────────────


def _get_app_password() -> str:
    """Load shared app password from Streamlit secrets or env var APP_PASSWORD."""
    secret_password = st.secrets.get("APP_PASSWORD", "")
    env_password = os.environ.get("APP_PASSWORD", "")
    password = secret_password or env_password
    return password.strip() if isinstance(password, str) else ""


def check_credentials(password: str) -> bool:
    app_password = _get_app_password()
    if not app_password:
        # Default dev credential for local testing only
        return password == "admin123"
    return password == app_password


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
        if st.button("Entrar", use_container_width=True):
            if check_credentials(password):
                st.session_state["authenticated"] = True
                st.session_state["username"] = "app_user"
                st.rerun()
            else:
                st.error("Credenciales incorrectas.")
        st.markdown("</div>", unsafe_allow_html=True)

    return False


# ─────────────────────────────────────────────
# FIREBASE FIRESTORE
# ─────────────────────────────────────────────


def _get_firebase_service_account_info() -> dict | None:
    """Read Firebase service account from Streamlit secrets or env JSON."""
    try:
        # Expected Streamlit Cloud format: [firebase.service_account]
        if "firebase" in st.secrets and "service_account" in st.secrets["firebase"]:
            return dict(st.secrets["firebase"]["service_account"])
    except Exception:
        pass

    raw = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "")
    if raw:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return None
    return None


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
        return firestore.Client()
    except Exception as e:
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

        # Keep single-doc writes for small payloads (legacy-compatible format).
        if len(payload) <= FIRESTORE_CHUNK_SIZE:
            doc_ref.set({"payload": payload, "updated_at": firestore.SERVER_TIMESTAMP})
            return True

        chunks = [
            payload[i:i + FIRESTORE_CHUNK_SIZE]
            for i in range(0, len(payload), FIRESTORE_CHUNK_SIZE)
        ]

        # Root doc stores metadata only; payload bytes are saved in chunk subcollection.
        doc_ref.set({
            "chunked": True,
            "chunk_count": len(chunks),
            "updated_at": firestore.SERVER_TIMESTAMP,
        })

        batch = client.batch()
        for idx, chunk in enumerate(chunks):
            chunk_ref = doc_ref.collection("chunks").document(f"{idx:05d}")
            batch.set(chunk_ref, {"payload": chunk})
        batch.commit()
        return True
    except Exception as e:
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
                return None
            chunk_docs = (
                client.collection(collection)
                .document(key)
                .collection("chunks")
                .stream()
            )
            chunks = {
                d.id: bytes((d.to_dict() or {}).get("payload") or b"")
                for d in chunk_docs
            }
            ordered = [chunks.get(f"{i:05d}", b"") for i in range(chunk_count)]
            return b"".join(ordered) if all(ordered) else None

        payload = data.get("payload")
        return bytes(payload) if payload else None
    except Exception:
        return None


# ─────────────────────────────────────────────
# GOOGLE CLOUD STORAGE (legacy fallback)
# ─────────────────────────────────────────────


def _get_gcs_client():
    """Return a GCS client, or None if not configured."""
    try:
        # If running on Cloud Run, ADC is used automatically.
        # Locally, set GOOGLE_APPLICATION_CREDENTIALS env var.
        creds_json = os.environ.get("GCS_CREDENTIALS_JSON", "")
        if creds_json:
            import tempfile

            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json", mode="w")
            tmp.write(creds_json)
            tmp.close()
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = tmp.name
        return storage.Client()
    except Exception as e:
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
    except Exception:
        return None


def gcs_list(bucket_name: str, prefix: str = "") -> list[str]:
    """List blob names in bucket."""
    client = _get_gcs_client()
    if client is None:
        return []
    try:
        return [b.name for b in client.list_blobs(bucket_name, prefix=prefix)]
    except Exception:
        return []
