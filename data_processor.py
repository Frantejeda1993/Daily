"""
data_processor.py
"""
import re
import pandas as pd
import numpy as np
from datetime import date


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

    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], dayfirst=True, errors='coerce')

    if 'clave' in df.columns:
        df['brand'] = df['clave'].astype(str).apply(extract_short_name)

    for col in ['importe', 'margen_pct_raw', 'margen_eur', 'unidades']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0.0

    # Calculate margen_eur from Importe * (pct/100) when absent or all-zero
    if df['margen_eur'].abs().sum() == 0 and df['margen_pct_raw'].abs().sum() > 0:
        df['margen_eur'] = df['importe'] * (df['margen_pct_raw'] / 100.0)

    df['margen_pct'] = np.where(
        df['importe'] != 0,
        df['margen_eur'] / df['importe'],
        df['margen_pct_raw'] / 100.0,
    )
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

    clave_col, importe_col = None, None
    for col in raw.columns:
        cl = col.lower()
        if 'clave' in cl and clave_col is None:
            clave_col = col
        if 'importe' in cl and importe_col is None:
            importe_col = col

    if clave_col is None:
        clave_col = raw.columns[0]
    if importe_col is None:
        importe_col = raw.columns[min(2, len(raw.columns) - 1)]

    result = raw[[clave_col, importe_col]].copy()
    result.columns = ['clave_raw', 'stock_value']
    result = result.dropna(subset=['clave_raw'])
    result = result[result['clave_raw'].str.strip().ne('')]

    result['brand'] = result['clave_raw'].astype(str).apply(extract_short_name)
    result['stock_value'] = pd.to_numeric(
        result['stock_value'].astype(str).str.replace(',', '.', regex=False),
        errors='coerce',
    ).fillna(0)

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
    df = df.rename(columns={
        'Marca': 'brand',
        'Budget Venta': 'budget_revenue',
        'Margen%': 'budget_margin_pct',
    }, errors='ignore')

    if 'brand' not in df.columns:
        df = df.rename(columns={df.columns[0]: 'brand'})

    for col in ['budget_revenue', 'budget_margin_pct']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            df[col] = 0.0

    df['brand'] = df['brand'].astype(str).str.strip().str.upper()
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
    ref_mmdd = (reference_date.month, reference_date.day)
    mask = df['fecha'].apply(
        lambda d: (d.month, d.day) <= ref_mmdd if pd.notna(d) else False
    )
    return df[mask]


def summarise_sales(df: pd.DataFrame, group_col: str = 'brand') -> pd.DataFrame:
    agg = df.groupby(group_col, as_index=False).agg(
        revenue=('importe', 'sum'),
        margin_eur=('margen_eur', 'sum'),
    )
    agg['margin_pct'] = np.where(
        agg['revenue'] != 0, agg['margin_eur'] / agg['revenue'], 0.0
    )
    return agg


def merge_kpis(cy_sales, ly_sales, budget, stock_cy, stock_ly):
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

    merged['growth_real'] = np.where(
        merged['ly_revenue'] != 0,
        (merged['cy_revenue'] - merged['ly_revenue']) / merged['ly_revenue'], np.nan)
    merged['budget_achievement'] = np.where(
        merged['budget_revenue'] != 0,
        merged['cy_revenue'] / merged['budget_revenue'], np.nan)
    merged['margin_delta_pts'] = merged['cy_margin_pct'] - merged['ly_margin_pct']
    merged['margin_delta_eur'] = merged['cy_margin_eur'] - merged['ly_margin_eur']
    merged['daily_revenue_cy'] = merged['cy_revenue'] / 30
    merged['days_stock'] = np.where(
        merged['daily_revenue_cy'] > 0,
        merged['stock_cy'] / merged['daily_revenue_cy'], np.nan)
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
        stock_cy=('stock_cy', 'sum'), stock_ly=('stock_ly', 'sum'),
    )
    grp['cy_margin_pct'] = np.where(
        grp['cy_revenue'] != 0, grp['cy_margin_eur'] / grp['cy_revenue'], 0.0)
    grp['ly_margin_pct'] = np.where(
        grp['ly_revenue'] != 0, grp['ly_margin_eur'] / grp['ly_revenue'], 0.0)
    grp['growth_real'] = np.where(
        grp['ly_revenue'] != 0,
        (grp['cy_revenue'] - grp['ly_revenue']) / grp['ly_revenue'], np.nan)
    grp['budget_achievement'] = np.where(
        grp['budget_revenue'] != 0, grp['cy_revenue'] / grp['budget_revenue'], np.nan)
    return grp
