# 📊 KPI Dashboard — Internal Sales & Margin Tracker

Aplicación Streamlit para seguimiento de KPIs de ventas, márgenes y stock con comparativa Like-for-Like (CY vs LY a la misma fecha).

---

## Estructura del proyecto

```
kpi_app/
├── app.py               # Aplicación principal Streamlit
├── data_processor.py    # Lógica de datos (Pandas), cálculos LfL y KPIs
├── google_auth.py       # Autenticación básica + helpers GCS
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## 🚀 Opción 1 — Streamlit Community Cloud

1. Sube el repositorio a GitHub (repo público o privado).
2. Ve a [share.streamlit.io](https://share.streamlit.io) → **New app**.
3. Selecciona el repo, rama `main` y fichero `app.py`.
4. (Recomendado) Añade `runtime.txt` en la raíz con `python-3.12` para evitar incompatibilidades de dependencias en Python 3.13.
5. Para evitar errores de `inotify instance limit reached` en Streamlit Cloud, crea `.streamlit/config.toml` con:

```toml
[server]
fileWatcherType = "none"
```

6. En **Advanced settings → Secrets** añade:

```toml
# .streamlit/secrets.toml (o en el panel de Streamlit Cloud)

APP_PASSWORD = "tu-contraseña-general"

[firebase.service_account]
type = "service_account"
project_id = "daily-athena"
private_key_id = "..."
private_key = """-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"""
client_email = "firebase-adminsdk-...@daily-athena.iam.gserviceaccount.com"
client_id = "..."
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/..."
universe_domain = "googleapis.com"
```

7. Haz clic en **Deploy**.

---

## 🐳 Opción 2 — Google Cloud Run (Dockerfile)

### Pre-requisitos
- Google Cloud SDK (`gcloud`) instalado y autenticado.
- Un bucket GCS creado: `gs://kpi-data-bucket` (o el nombre que prefieras).
- Un Service Account con roles `Storage Object Admin` y `Storage Object Viewer`.

### Build & Deploy

```bash
# 1. Configura variables
PROJECT_ID="tu-proyecto-gcp"
REGION="europe-west1"
IMAGE="gcr.io/$PROJECT_ID/kpi-dashboard"
BUCKET="kpi-data-bucket"
SA_EMAIL="kpi-sa@$PROJECT_ID.iam.gserviceaccount.com"

# 2. Build y push imagen
gcloud builds submit --tag $IMAGE

# 3. Deploy en Cloud Run
gcloud run deploy kpi-dashboard \
  --image $IMAGE \
  --platform managed \
  --region $REGION \
    --service-account $SA_EMAIL \
  --set-env-vars APP_PASSWORD="tu-contraseña-general" \
  --set-env-vars FIREBASE_SERVICE_ACCOUNT_JSON="{...json_service_account...}" \
  --set-env-vars FIRESTORE_COLLECTION="kpi_state" \
  --set-env-vars GCS_BUCKET=$BUCKET \
  --memory 1Gi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 3
```

> **Nota:** Se recomienda desplegar autenticado por defecto (Cloud Run IAM/IAP), con rate-limits/WAF y red privada cuando aplique.

---

## 🔐 Gestión de acceso

La app usa **una contraseña general** definida en `APP_PASSWORD` (secrets o variable de entorno).

- En Streamlit Cloud: define `APP_PASSWORD` en secrets.
- En otros entornos: exporta `APP_PASSWORD` como variable de entorno.

> Si `APP_PASSWORD` no está configurada, la app bloquea el acceso. Solo en `APP_ENV=dev` se puede usar `APP_DEV_PASSWORD` explícita para desarrollo local.

---

## 📂 Formato de archivos esperados

### Ventas CY / LY (Excel o CSV)
Columnas requeridas (igual que el template):

| Columna | Descripción |
|---|---|
| `Nombre Cliente` | Cliente |
| `Año Factura` | Año |
| `Mes Factura` | Mes (1-12) |
| `Fecha Factura` | Fecha (dd/mm/yyyy) |
| `Clave 1` | Código familia (p.ej. `300 - FAMILIA SHOKZ`) |
| `Importe Neto` | Revenue en € |
| `CR3: % Margen s/Venta` | % margen |
| `€ Margen` | Margen absoluto |
| `Unidades Venta` | Unidades |

### Stock (Excel con estructura mensual)
Mismo formato que la pestaña `INPUT (Mensual) Stock` del TemplateMonthly.xlsx.  
Columnas por mes: `Clave 1 | Código Artículo | Importe`

### Budget (Excel)
Mismo formato que la pestaña `INPUT (Anual) Budget`:  
`Marca | Budget Venta | Margen% | (Venta por mes…)`

---

## 📊 KPIs calculados

| KPI | Descripción |
|---|---|
| **Revenue CY / LY** | Ventas filtradas hasta misma fecha (Like-for-Like) |
| **Crec. Real** | (CY - LY) / LY |
| **Margen% CY / LY** | Margen€ / Revenue |
| **Δ Margen pts** | Diferencia en puntos porcentuales CY vs LY |
| **% vs Budget** | CY Revenue / Budget Revenue |
| **Días de Stock** | Stock CY / (Revenue diario CY) |
| **Proyección Mes** | Extrapolación lineal a fin de mes |

---

## 🛠️ Desarrollo local

```bash
pip install -r requirements.txt
streamlit run app.py
```

Accede en `http://localhost:8501`  
Define `APP_PASSWORD` antes de arrancar. Para desarrollo local puedes usar `APP_ENV=dev` junto a `APP_DEV_PASSWORD`.
