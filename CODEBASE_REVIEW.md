# Production Readiness Code Review (Strict)

Scope reviewed: `app.py`, `data_processor.py`, `google_auth.py`, `Dockerfile`, `requirements.txt`, and deployment guidance in `README.md`.

## 1) Insecure deserialization (`pickle.loads`) on externally stored data
- **Where**: `app.py` state loading helpers.
- **Problem**: Persisted bytes from Firestore/GCS are deserialized with `pickle.loads` directly.
- **Why this is problematic**: If storage is ever tampered with, `pickle` payloads can execute arbitrary code during load. This is a critical RCE class risk for production.
- **Concrete improvement**:
  - Replace pickle with a safe format (JSON/Parquet/Arrow) for known data types.
  - If binary serialization is unavoidable, cryptographically sign payloads and verify before loading.
- **Example (safer JSON path)**:
```python
import json

# save
payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")

# load
obj = json.loads(raw.decode("utf-8"))
```

## 2) Authentication uses plain equality and lacks brute-force controls
- **Where**: `google_auth.py` (`check_credentials`, `login_page`).
- **Problem**: Password comparison is simple `==` and there is no lockout, retry delay, or attempt tracking.
- **Why this is problematic**: Brute-force attacks are trivial (especially with a single shared password).
- **Concrete improvement**:
  - Use `hmac.compare_digest` for constant-time compare.
  - Track failed attempts per session/IP and apply exponential backoff.
  - Prefer identity-based auth (OIDC/Google auth) over shared password.
- **Example**:
```python
import hmac

def check_credentials(password: str) -> bool:
    app_password = _get_app_password()
    if not app_password:
        return False
    return hmac.compare_digest(password, app_password)
```

## 3) Hardcoded fallback password (`admin123`) is production-dangerous
- **Where**: `google_auth.py` and documented in `README.md`.
- **Problem**: If env/secrets are misconfigured, app accepts a known default password.
- **Why this is problematic**: A config mistake instantly creates an easy compromise path.
- **Concrete improvement**:
  - Fail closed when password is not configured in non-dev environments.
  - Gate local fallback by explicit `APP_ENV=dev` and randomize at startup if used.

## 4) Temporary credential file leak risk
- **Where**: `google_auth.py` (`_get_gcs_client`).
- **Problem**: `NamedTemporaryFile(delete=False)` writes service-account JSON to disk and never deletes it.
- **Why this is problematic**: Secrets persist on filesystem and can be exfiltrated from container snapshots/logs.
- **Concrete improvement**:
  - Avoid temp files. Load creds in-memory with `service_account.Credentials.from_service_account_info`.

## 5) Silent exception swallowing hides operational failures
- **Where**: multiple broad `except Exception` blocks, e.g. Firestore/GCS download helpers.
- **Problem**: Errors are suppressed and returned as `None`/empty list.
- **Why this is problematic**: Corruption, auth failures, and schema breaks appear as “no data”, making incidents hard to detect.
- **Concrete improvement**:
  - Log structured errors (without secrets), classify expected exceptions, and bubble up actionable failures to UI.

## 6) Input parsing is overly permissive; bad files can produce misleading KPIs
- **Where**: `data_processor.py` (`parse_sales`, `parse_budget`, `parse_stock`).
- **Problem**: Missing required columns default to zeros instead of failing fast.
- **Why this is problematic**: Dashboard can show apparently valid numbers built from malformed input, leading to wrong business decisions.
- **Concrete improvement**:
  - Validate required schema and stop with explicit error listing missing columns.
- **Example**:
```python
required = {"fecha", "clave", "importe"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Sales file missing columns: {sorted(missing)}")
```

## 7) Numeric parsing for European formats is fragile
- **Where**: `data_processor.py` (`parse_stock`).
- **Problem**: Replacing comma with dot is insufficient for values like `1.234,56`.
- **Why this is problematic**: Valid monetary values become `NaN` then `0`, underreporting stock.
- **Concrete improvement**:
  - Strip thousands separators before decimal normalization, or use locale-aware parsing.
- **Example**:
```python
s = result["stock_value"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
result["stock_value"] = pd.to_numeric(s, errors="coerce")
```

## 8) LfL filter uses row-wise `.apply` and does not scale
- **Where**: `data_processor.py` (`lfl_filter`).
- **Problem**: Python lambda over each row is slow for large datasets.
- **Why this is problematic**: Latency increases non-linearly with data growth and hurts interactive UX.
- **Concrete improvement**:
  - Use vectorized datetime accessors.
- **Example**:
```python
mask = (df["fecha"].dt.month < reference_date.month) | (
    (df["fecha"].dt.month == reference_date.month) & (df["fecha"].dt.day <= reference_date.day)
)
return df[mask]
```

## 9) KPI logic hardcodes 30-day denominator
- **Where**: `data_processor.py` (`merge_kpis`, `daily_revenue_cy = cy_revenue / 30`).
- **Problem**: Days-in-stock is based on 30 days regardless of selected month or elapsed days.
- **Why this is problematic**: Metric bias (up to ~10%+) for 28/31-day months; misleading operational conclusions.
- **Concrete improvement**:
  - Compute denominator from real days elapsed or calendar month length.

## 10) Persisted state load does not trigger KPI rebuild
- **Where**: `app.py` startup flow (`load_persisted_state` call).
- **Problem**: Persisted CY/LY/budget/stock may load, but `kpi_table`/`recap_table` stay `None` until user action triggers rebuild.
- **Why this is problematic**: Users can see empty dashboards despite data existing.
- **Concrete improvement**:
  - Call `rebuild_kpis()` after successful persisted-state hydration when CY and LY are present.

## 11) File de-duplication key can miss real updates
- **Where**: `app.py` upload handlers using `name + size` keys in `_processed_files`.
- **Problem**: Different file content with same name and size is treated as already processed.
- **Why this is problematic**: Stale data is silently kept; users think they uploaded fresh data.
- **Concrete improvement**:
  - Hash content bytes (SHA-256) for dedupe keys.

## 12) Maintainability: business logic and UI are tightly coupled in one large file
- **Where**: `app.py` (~700 lines with state, persistence, KPI logic, and presentation).
- **Problem**: Single module has multiple responsibilities.
- **Why this is problematic**: Harder testing, harder refactoring, higher change risk.
- **Concrete improvement (architecture)**:
  - Split into layers:
    - `services/persistence.py`
    - `services/kpi_service.py`
    - `ui/tabs/*.py`
    - `models/schemas.py` (pydantic/dataclasses)
  - Add unit tests for parser and KPI functions.

## 13) Docker image is larger than needed and includes build tooling at runtime
- **Where**: `Dockerfile`.
- **Problem**: Installs `build-essential` in final image and keeps it.
- **Why this is problematic**: Bigger attack surface and slower deploy/startup.
- **Concrete improvement**:
  - Use multi-stage build or remove compiler tools after wheel install.

## 14) Dependency strategy is too loose for production reproducibility
- **Where**: `requirements.txt` (only lower bounds).
- **Problem**: No upper bounds/lockfile; transitive upgrades may break behavior unexpectedly.
- **Why this is problematic**: Non-reproducible deployments and difficult incident rollback.
- **Concrete improvement**:
  - Maintain pinned lockfile (`pip-tools`/`poetry`) and scheduled upgrade process.

## 15) Security posture of deployment guidance is permissive by default
- **Where**: `README.md` Cloud Run example includes `--allow-unauthenticated`.
- **Problem**: Public exposure + shared-password model is weak for sensitive KPI data.
- **Why this is problematic**: Increased risk of data leakage and brute-force attacks.
- **Concrete improvement**:
  - Recommend authenticated ingress by default (IAP/OIDC), private networking, and WAF/rate-limits.
