"""
google_auth.py
Handles:
  - Basic employee login (username/password via st.secrets or env vars)
  - Google Cloud Storage upload / download helpers
"""
import os
import json
import hashlib
import streamlit as st

# ─────────────────────────────────────────────
# BASIC AUTH
# ─────────────────────────────────────────────

def _get_users() -> dict:
    """
    Load user dict from st.secrets['users'] or AUTH_USERS env var.
    Format: {"alice": "hashed_password", ...}
    Passwords are SHA-256 hex strings.
    To generate: python -c "import hashlib; print(hashlib.sha256(b'mypassword').hexdigest())"
    """
    try:
        return dict(st.secrets.get("users", {}))
    except Exception:
        raw = os.environ.get("AUTH_USERS", "")
        if raw:
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                pass
    # Default dev credentials (admin / admin123) — change in production!
    default_hash = hashlib.sha256(b"admin123").hexdigest()
    return {"admin": default_hash}


def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def check_credentials(username: str, password: str) -> bool:
    users = _get_users()
    stored_hash = users.get(username.strip().lower())
    if stored_hash is None:
        return False
    return hash_password(password) == stored_hash


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
        username = st.text_input("Usuario", key="login_user")
        password = st.text_input("Contraseña", type="password", key="login_pw")
        if st.button("Entrar", use_container_width=True):
            if check_credentials(username, password):
                st.session_state["authenticated"] = True
                st.session_state["username"] = username.strip().lower()
                st.rerun()
            else:
                st.error("Credenciales incorrectas.")
        st.markdown("</div>", unsafe_allow_html=True)

    return False


# ─────────────────────────────────────────────
# GOOGLE CLOUD STORAGE
# ─────────────────────────────────────────────

def _get_gcs_client():
    """Return a GCS client, or None if not configured."""
    try:
        from google.cloud import storage
        # If running on Cloud Run, ADC is used automatically.
        # Locally, set GOOGLE_APPLICATION_CREDENTIALS env var.
        creds_json = os.environ.get("GCS_CREDENTIALS_JSON", "")
        if creds_json:
            import tempfile, json as _json
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
