"""
data_processor.py
"""
import re
import logging
import pandas as pd
import numpy as np
from datetime import date


logger = logging.getLogger(__name__)


def extract_short_name(familia_str: str) -> str:
    """'300 - FAMILIA SHOKZ' -> 'SHOKZ'  (uppercase, stripped)"""
    if not isinstance(familia_str, str):
        return str(familia_str).upper().strip()
    s = re.sub(r'^\d+\s*-\s*FAMILIA\s*', '', familia_str, flags=re.IGNORECASE).strip()
    s = re.sub(r'^FAMILIA\s*', '', s, flags=re.IGNORECASE).strip()
    return (s if s else familia_str.strip()).upper()


SALES_COL_MAP = {
    'Fecha Factura':                          'fecha',
    'Mes Factura':                            'mes',
    'Año Factura':                            'anio',
    'Clave 1':                                'clave',
    'Importe Neto':                           'importe',
    'CR3: % Margen s/Venta':                  'margen_pct_raw',
    'CR3: % Margen s/Venta + Transport':      'margen_pct_raw',
    '€ Margen':                               'margen_eur',
    'Unidades Venta':                         'unidades',
}

STOCK_EXPECTED_COLUMNS = {
    'clave': ['clave', 'clave 1', 'clave1', 'codigo', 'código'],
    'importe': ['importe', 'importe neto', 'monto', 'valor', 'amount'],
}




def _validate_required_columns(df: pd.DataFrame, required: set[str], dataset_name: str) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{dataset_name} missing required columns: {missing}")


def _parse_european_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    return pd.to_numeric(s, errors='coerce')


def coerce_numeric_with_logging(series: pd.Series, col_name: str) -> pd.Series:
    """Coerce to numeric and log conversion failures."""
    before = series.notna().sum()
    numeric = pd.to_numeric(series, errors='coerce')
    after = numeric.notna().sum()
    lost = before - after
    if lost > 0:
        logger.warning("Column %s: %s non-numeric values coerced to NaN", col_name, lost)
    return numeric.fillna(0)


def _normalize_column_name(name: str) -> str:
    return re.sub(r'\s+', ' ', str(name)).strip().lower().replace('á', 'a').replace('é', 'e') \
        .replace('í', 'i').replace('ó', 'o').replace('ú', 'u')


def _find_expected_column(df: pd.DataFrame, col_type: str) -> str | None:
    candidates = STOCK_EXPECTED_COLUMNS.get(col_type, [])
    normalized_candidates = [_normalize_column_name(candidate) for candidate in candidates]

    columns_by_normalized_name: dict[str, str] = {
        _normalize_column_name(column): column
        for column in df.columns
    }

    for candidate in normalized_candidates:
        if candidate in columns_by_normalized_name:
            return columns_by_normalized_name[candidate]
    return None


def safe_divide(numerator, denominator, fill_value: float = np.nan):
    """Divide safely for scalars or Series, returning fill_value for zero/NaN denominators."""
    if np.isscalar(numerator) and np.isscalar(denominator):
        if pd.isna(denominator) or denominator == 0:
            return fill_value
        return numerator / denominator

    numerator_series = numerator if isinstance(numerator, pd.Series) else pd.Series(numerator)
    denominator_series = denominator if isinstance(denominator, pd.Series) else pd.Series(denominator, index=numerator_series.index)

    valid_denominator = denominator_series.notna() & (denominator_series != 0)
    result = pd.Series(fill_value, index=denominator_series.index, dtype=float)
    result.loc[valid_denominator] = (
        numerator_series.loc[valid_denominator] / denominator_series.loc[valid_denominator]
    )
    return result


def parse_sales(file) -> pd.DataFrame:
    """
    Parse sales file (Excel or CSV).
    - brand extracted uppercase from Clave 1
    - margen_eur calculated when column absent/zero
    """
    if hasattr(file, 'name') and str(file.name).lower().endswith('.csv'):
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file, sheet_name=0)

    df.columns = df.columns.str.strip()
    df = df.rename(columns=SALES_COL_MAP, errors='ignore')

    _validate_required_columns(df, {'fecha', 'clave', 'importe'}, 'Sales file')

    df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')
    df['brand'] = df['clave'].astype(str).apply(extract_short_name)

    for col in ['importe', 'margen_pct_raw', 'margen_eur', 'unidades']:
        if col in df.columns:
            df[col] = coerce_numeric_with_logging(df[col], col)
        elif col in {'importe'}:
            raise ValueError(f"Sales file missing required numeric column: {col}")
        else:
            df[col] = 0.0

    # Calculate margen_eur from Importe * (pct/100) when absent or all-zero
    if df['margen_eur'].abs().sum() == 0 and df['margen_pct_raw'].abs().sum() > 0:
        df['margen_eur'] = df['importe'] * (df['margen_pct_raw'] / 100.0)

    df['margen_pct'] = safe_divide(df['margen_eur'], df['importe'], fill_value=np.nan)
    fallback_mask = df['margen_pct'].isna()
    df.loc[fallback_mask, 'margen_pct'] = df.loc[fallback_mask, 'margen_pct_raw'] / 100.0
    return df


def parse_stock(file) -> pd.DataFrame:
    """
    Simple 3-column file: Clave 1 | Código Artículo | Importe
    Returns: brand (uppercase) | stock_value
    """
    if hasattr(file, 'name') and str(file.name).lower().endswith('.csv'):
        raw = pd.read_csv(file, header=None, dtype=str)
    else:
        raw = pd.read_excel(file, sheet_name=0, header=None, dtype=str)

    # Detect header row
    first_row = raw.iloc[0].fillna('').str.lower()
    has_header = any(kw in ' '.join(first_row)
                     for kw in ['clave', 'importe', 'código', 'codigo', 'articulo'])
    if has_header:
        raw.columns = raw.iloc[0]
        raw = raw.iloc[1:].reset_index(drop=True)

    raw.columns = [str(c).strip() for c in raw.columns]

    clave_col = _find_expected_column(raw, 'clave')
    importe_col = _find_expected_column(raw, 'importe')

    if clave_col is None or importe_col is None:
        raise ValueError("Stock file must contain 'Clave' and 'Importe' columns")

    result = raw[[clave_col, importe_col]].copy()
    result.columns = ['clave_raw', 'stock_value']
    result = result.dropna(subset=['clave_raw'])
    result = result[result['clave_raw'].str.strip().ne('')]

    result['brand'] = result['clave_raw'].astype(str).apply(extract_short_name)
    result['stock_value'] = _parse_european_numeric(result['stock_value']).fillna(0)

    return result.groupby('brand', as_index=False)['stock_value'].sum()


def parse_budget(file) -> pd.DataFrame:
    try:
        df = pd.read_excel(file, sheet_name='INPUT (Anual) Budget', header=0)
    except Exception:
        try:
            df = pd.read_excel(file, sheet_name=0, header=0)
        except Exception:
            df = pd.read_csv(file)

    df.columns = df.columns.str.strip()

    normalized_cols = {
        c: re.sub(r'\s+', ' ', str(c)).strip().lower().replace('á', 'a').replace('é', 'e')
        .replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
        for c in df.columns
    }
    rename_map = {}
    for original, norm in normalized_cols.items():
        if norm in {'marca', 'brand', 'clave 1', 'clave1', 'familia'}:
            rename_map[original] = 'brand'
        elif norm in {'budget venta', 'budget', 'budget revenue', 'presupuesto venta'}:
            rename_map[original] = 'budget_revenue'
        elif norm in {'margen%', 'margen %', 'budget margen%', 'budget margen %', 'margin%'}:
            rename_map[original] = 'budget_margin_pct'

    df = df.rename(columns=rename_map, errors='ignore')

    if 'brand' not in df.columns:
        raise ValueError("Budget file missing required column: Marca/brand")

    _validate_required_columns(df, {'brand', 'budget_revenue', 'budget_margin_pct'}, 'Budget file')

    # Budget files are usually exported in European numeric format (e.g. 1.234.567,89)
    # and sometimes with % symbols in margin.
    if 'budget_revenue' in df.columns:
        df['budget_revenue'] = _parse_european_numeric(df['budget_revenue']).fillna(0)
    else:
        df['budget_revenue'] = 0.0

    if 'budget_margin_pct' in df.columns:
        margin_raw = df['budget_margin_pct'].astype(str).str.replace('%', '', regex=False)
        df['budget_margin_pct'] = _parse_european_numeric(margin_raw).fillna(0)
    else:
        df['budget_margin_pct'] = 0.0

    df['brand'] = df['brand'].astype(str).apply(extract_short_name)
    df['brand'] = df['brand'].str.replace(r'\s+', ' ', regex=True).str.strip().str.upper()
    df = df[df['brand'].ne('') & ~df['brand'].isin({'NAN', 'NONE'})]

    # If duplicates exist (e.g. mixed brand labels in source file), aggregate by brand.
    df = df.groupby('brand', as_index=False).agg(
        budget_revenue=('budget_revenue', 'sum'),
        budget_margin_pct=('budget_margin_pct', 'mean'),
    )

    return df[['brand', 'budget_revenue', 'budget_margin_pct']].dropna(subset=['brand'])


def parse_families(file) -> pd.DataFrame:
    """
    Parse the families/groups mapping file.

    Reads the 'INPUT (Anual) Familias' sheet (or sheet 0 as fallback).
    Columns expected: Nombre | Familia | Columna1

    Logic:
      - 'Familia' column contains full codes like '300 - FAMILIA SHOKZ'
        → extract_short_name() → 'SHOKZ'  (matches brand in sales)
      - 'Columna1' contains the group: '2 WHEELS', 'FREE TIME', 'OUTDOOR TECH'
      - Also try 'Nombre' column (display name like 'Shokz') as fallback key

    Returns DataFrame: brand_key (uppercase) | display_name | group (title-cased)
    """
    df = None
    try:
        df = pd.read_excel(file, sheet_name='INPUT (Anual) Familias', header=0)
    except Exception:
        try:
            df = pd.read_excel(file, sheet_name=0, header=0)
        except Exception:
            df = pd.read_csv(file)

    df.columns = df.columns.str.strip()

    # Normalise column names flexibly
    col_lower = {c.lower(): c for c in df.columns}

    nombre_col  = col_lower.get('nombre',  col_lower.get('marca',  col_lower.get('brand', None)))
    familia_col = col_lower.get('familia', col_lower.get('clave 1', col_lower.get('clave1', None)))
    grupo_col   = col_lower.get('columna1', col_lower.get('grupo', col_lower.get('group',
                   col_lower.get('vertical', col_lower.get('categoria', None)))))

    def candidate_brand_keys(raw_value: str) -> set[str]:
        """Build uppercase key variants to improve matching against detected brands."""
        if raw_value is None:
            return set()
        txt = str(raw_value).strip()
        if not txt or txt.lower() in ('nan', 'none', ''):
            return set()

        keys = set()
        compact = re.sub(r'\s+', ' ', txt).strip().upper()
        if compact:
            keys.add(compact)

        short = extract_short_name(txt)
        short = re.sub(r'\s+', ' ', short).strip().upper()
        if short:
            keys.add(short)
        return keys

    records = []
    for _, row in df.iterrows():
        grupo_raw = str(row[grupo_col]).strip() if grupo_col and pd.notna(row.get(grupo_col)) else ''
        if not grupo_raw or grupo_raw.lower() in ('nan', 'none', ''):
            continue  # skip rows with no group assigned

        # Normalise group to title-case matching app GROUPS list
        grupo_upper = grupo_raw.upper()
        if '2 WHEEL' in grupo_upper:
            grupo = '2 Wheels'
        elif 'FREE' in grupo_upper:
            grupo = 'Free Time'
        elif 'OUTDOOR' in grupo_upper:
            grupo = 'Outdoor Tech'
        else:
            continue  # unknown group, skip

        # Primary key: extract_short_name from Familia column → uppercase → matches sales brand
        if familia_col and pd.notna(row.get(familia_col)):
            for brand_key in candidate_brand_keys(row[familia_col]):
                records.append({'brand_key': brand_key, 'grupo': grupo})

        # Secondary key: Nombre column (title-cased display name) → uppercase
        if nombre_col and pd.notna(row.get(nombre_col)):
            for brand_key in candidate_brand_keys(row[nombre_col]):
                records.append({'brand_key': brand_key, 'grupo': grupo})

    result = pd.DataFrame(records).drop_duplicates(subset=['brand_key'])
    return result


def lfl_filter(df: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    fecha = pd.to_datetime(df['fecha'], errors='coerce')
    mask = (fecha.dt.month < reference_date.month) | (
        (fecha.dt.month == reference_date.month) & (fecha.dt.day <= reference_date.day)
    )
    return df[mask.fillna(False)]


def summarise_sales(df: pd.DataFrame, group_col: str = 'brand') -> pd.DataFrame:
    agg = df.groupby(group_col, as_index=False).agg(
        revenue=('importe', 'sum'),
        margin_eur=('margen_eur', 'sum'),
    )
    agg['margin_pct'] = safe_divide(agg['margin_eur'], agg['revenue'], fill_value=0.0)
    return agg


def merge_kpis(cy_sales, ly_sales, budget, stock_cy, stock_ly, reference_date: date):
    cy = summarise_sales(cy_sales).rename(columns={
        'revenue': 'cy_revenue', 'margin_eur': 'cy_margin_eur', 'margin_pct': 'cy_margin_pct'})
    ly = summarise_sales(ly_sales).rename(columns={
        'revenue': 'ly_revenue', 'margin_eur': 'ly_margin_eur', 'margin_pct': 'ly_margin_pct'})
    merged = cy.merge(ly, on='brand', how='outer').fillna(0)

    if budget is not None and not budget.empty:
        merged = merged.merge(
            budget[['brand', 'budget_revenue', 'budget_margin_pct']], on='brand', how='left')
    else:
        merged['budget_revenue'] = 0.0
        merged['budget_margin_pct'] = 0.0
    merged.fillna(0, inplace=True)

    for stk_df, col_name in [(stock_cy, 'stock_cy'), (stock_ly, 'stock_ly')]:
        if stk_df is not None and not stk_df.empty:
            stk = stk_df.groupby('brand', as_index=False)['stock_value'].sum()
            merged = merged.merge(stk.rename(columns={'stock_value': col_name}),
                                  on='brand', how='left')
        else:
            merged[col_name] = 0.0
    merged.fillna(0, inplace=True)

    merged['growth_real'] = safe_divide(
        merged['cy_revenue'] - merged['ly_revenue'],
        merged['ly_revenue'],
        fill_value=np.nan,
    )
    year_days = (date(reference_date.year, 12, 31) - date(reference_date.year, 1, 1)).days + 1
    elapsed_days = (reference_date - date(reference_date.year, 1, 1)).days + 1
    budget_to_date_factor = min(max(elapsed_days / year_days, 0.0), 1.0)
    merged['budget_to_date_revenue'] = merged['budget_revenue'] * budget_to_date_factor
    merged['budget_achievement'] = safe_divide(
        merged['cy_revenue'],
        merged['budget_to_date_revenue'],
        fill_value=np.nan,
    )
    merged['budget_gap_eur'] = merged['cy_revenue'] - merged['budget_to_date_revenue']
    merged['budget_gap_pct'] = safe_divide(
        merged['cy_revenue'],
        merged['budget_to_date_revenue'],
        fill_value=np.nan,
    ) - 1
    merged['margin_delta_pts'] = merged['cy_margin_pct'] - merged['ly_margin_pct']
    merged['margin_delta_eur'] = merged['cy_margin_eur'] - merged['ly_margin_eur']
    days_elapsed = max((reference_date - date(reference_date.year, 1, 1)).days + 1, 1)
    merged['daily_revenue_cy'] = merged['cy_revenue'] / days_elapsed
    merged['days_stock'] = safe_divide(merged['stock_cy'], merged['daily_revenue_cy'], fill_value=np.nan)
    total_cy_revenue = merged['cy_revenue'].sum()
    total_cy_margin_eur = merged['cy_margin_eur'].sum()
    merged['mix_contribution_pct'] = safe_divide(
        merged['cy_revenue'],
        pd.Series(total_cy_revenue, index=merged.index),
        fill_value=np.nan,
    )
    merged['margin_contribution_pct'] = safe_divide(
        merged['cy_margin_eur'],
        pd.Series(total_cy_margin_eur, index=merged.index),
        fill_value=np.nan,
    )
    merged['brand_status'] = np.where(merged['ly_revenue'] > 0, 'Existing', 'New')

    cy_units = cy_sales.groupby('brand', as_index=False)['unidades'].sum().rename(columns={'unidades': 'cy_units'})
    ly_units = ly_sales.groupby('brand', as_index=False)['unidades'].sum().rename(columns={'unidades': 'ly_units'})
    merged = merged.merge(cy_units, on='brand', how='left').merge(ly_units, on='brand', how='left')
    merged[['cy_units', 'ly_units']] = merged[['cy_units', 'ly_units']].fillna(0)
    merged['revenue_per_unit'] = safe_divide(merged['cy_revenue'], merged['cy_units'], fill_value=np.nan)
    merged['margin_per_unit'] = safe_divide(merged['cy_margin_eur'], merged['cy_units'], fill_value=np.nan)
    merged['metric_window'] = 'YTD_LfL'
    return merged


def project_month_end(cy_sales_full: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    import calendar
    days_in_month = calendar.monthrange(reference_date.year, reference_date.month)[1]
    days_elapsed = max(reference_date.day, 1)
    cy_month = cy_sales_full[
        (cy_sales_full['fecha'].dt.month == reference_date.month) &
        (cy_sales_full['fecha'].dt.year == reference_date.year)
    ]
    agg = summarise_sales(cy_month)[['brand', 'revenue']].rename(
        columns={'revenue': 'cy_revenue_todate'})
    agg['projected_revenue'] = agg['cy_revenue_todate'] * (days_in_month / days_elapsed)
    agg['elapsed_pct'] = days_elapsed / days_in_month
    return agg


def build_recap(kpi_df: pd.DataFrame, family_map: dict) -> pd.DataFrame:
    df = kpi_df.copy()
    df['group'] = df['brand'].map(family_map).fillna('Other')
    grp = df.groupby('group', as_index=False).agg(
        cy_revenue=('cy_revenue', 'sum'), ly_revenue=('ly_revenue', 'sum'),
        cy_margin_eur=('cy_margin_eur', 'sum'), ly_margin_eur=('ly_margin_eur', 'sum'),
        budget_revenue=('budget_revenue', 'sum'),
        budget_to_date_revenue=('budget_to_date_revenue', 'sum'),
        budget_gap_eur=('budget_gap_eur', 'sum'),
        stock_cy=('stock_cy', 'sum'), stock_ly=('stock_ly', 'sum'),
    )
    grp['cy_margin_pct'] = safe_divide(grp['cy_margin_eur'], grp['cy_revenue'], fill_value=0.0)
    grp['ly_margin_pct'] = safe_divide(grp['ly_margin_eur'], grp['ly_revenue'], fill_value=0.0)
    grp['growth_real'] = safe_divide(grp['cy_revenue'] - grp['ly_revenue'], grp['ly_revenue'], fill_value=np.nan)
    grp['budget_achievement'] = safe_divide(grp['cy_revenue'], grp['budget_to_date_revenue'], fill_value=np.nan)
    grp['budget_gap_pct'] = safe_divide(grp['cy_revenue'], grp['budget_to_date_revenue'], fill_value=np.nan) - 1
    return grp
