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
4. En **Advanced settings → Secrets** añade:

```toml
# .streamlit/secrets.toml (o en el panel de Streamlit Cloud)

[users]
admin = "<sha256 de tu contraseña>"
# Genera el hash con:
# python -c "import hashlib; print(hashlib.sha256(b'tuContraseña').hexdigest())"

GCS_BUCKET = "nombre-de-tu-bucket"
GCS_CREDENTIALS_JSON = """{ ... contenido JSON de tu service account ... }"""
```

5. Haz clic en **Deploy**.

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
  --allow-unauthenticated \
  --service-account $SA_EMAIL \
  --set-env-vars GCS_BUCKET=$BUCKET \
  --set-env-vars AUTH_USERS='{"admin":"<sha256_hash>"}' \
  --memory 1Gi \
  --cpu 1 \
  --min-instances 0 \
  --max-instances 3
```

> **Nota:** Si no quieres exponer la app públicamente, elimina `--allow-unauthenticated` y usa Cloud IAP o VPC connector.

---

## 🔐 Gestión de usuarios

Las contraseñas se almacenan como SHA-256.  
Para añadir un usuario nuevo:

```python
import hashlib
print(hashlib.sha256(b"miNuevaContraseña").hexdigest())
```

Añade el par `usuario: hash` en `secrets.toml` o en la variable `AUTH_USERS` (JSON).

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
Usuario por defecto: `admin` / contraseña: `admin123` *(cambiar en producción)*
