"""
data_processor.py
"""
import re
import logging
import pandas as pd
import numpy as np
from datetime import date

from app_settings import AppConfig

logger = logging.getLogger(__name__)


def _read_tabular_with_fallbacks(
    file,
    dataset_name: str,
    preferred_sheet: str,
) -> pd.DataFrame:
    """Read a tabular file with logged Excel/CSV fallbacks."""
    readers = [
        ("excel_named_sheet", lambda: pd.read_excel(file, sheet_name=preferred_sheet, header=0)),
        ("excel_first_sheet", lambda: pd.read_excel(file, sheet_name=0, header=0)),
        ("csv", lambda: pd.read_csv(file)),
    ]
    errors: list[str] = []

    for method_name, reader in readers:
        try:
            if hasattr(file, "seek"):
                file.seek(0)
            return reader()
        except Exception as exc:
            errors.append(f"{method_name}: {exc}")
            logger.warning(
                "Failed to parse %s using %s fallback: %s",
                dataset_name,
                method_name,
                exc,
            )

    raise ValueError(
        f"Unable to parse {dataset_name}. Attempts: {' | '.join(errors)}"
    )


def validate_reference_date(reference_date: date, data_max_date: date | None) -> bool:
    """Ensure a reference date is not in the future or after available data."""
    if reference_date > date.today():
        raise ValueError(f"Reference date {reference_date} cannot be in the future")

    if data_max_date is not None and reference_date > data_max_date:
        raise ValueError(
            f"Reference date {reference_date} is after latest data {data_max_date}"
        )

    return True


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


def _detect_family_columns(df: pd.DataFrame) -> tuple[str | None, str | None, str | None]:
    """Detect source columns for nombre, familia and grupo using normalized aliases."""
    aliases = {
        'nombre': ('nombre', 'marca', 'brand'),
        'familia': ('familia', 'clave 1', 'clave1'),
        'grupo': ('columna1', 'grupo', 'group', 'vertical', 'categoria'),
    }
    columns_by_normalized_name: dict[str, str] = {
        _normalize_column_name(column): column
        for column in df.columns
    }

    detected: list[str | None] = []
    for key in ('nombre', 'familia', 'grupo'):
        match = None
        for alias in aliases[key]:
            normalized_alias = _normalize_column_name(alias)
            if normalized_alias in columns_by_normalized_name:
                match = columns_by_normalized_name[normalized_alias]
                break
        detected.append(match)
    return detected[0], detected[1], detected[2]


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


def normalize_group_names_vectorized(frame: pd.DataFrame, col: str) -> pd.Series:
    """Vectorized group normalization from raw text labels to app group names."""
    normalized = (
        frame[col]
        .astype('string')
        .fillna('')
        .str.strip()
        .str.upper()
    )
    result = pd.Series(pd.NA, index=frame.index, dtype='string')
    result.loc[normalized.str.contains('2 WHEEL', regex=False, na=False)] = '2 Wheels'
    result.loc[normalized.str.contains('FREE', regex=False, na=False)] = 'Free Time'
    result.loc[normalized.str.contains('OUTDOOR', regex=False, na=False)] = 'Outdoor Tech'
    return result


def _expand_brand_keys(
    valid_rows: pd.DataFrame,
    source_col: str,
    group_col: str = 'grupo',
) -> pd.DataFrame:
    """Explode key variants from a source column into brand_key/group pairs."""
    keys = valid_rows[source_col].apply(candidate_brand_keys)
    expanded = valid_rows[[group_col]].copy()
    expanded['brand_key'] = keys
    expanded = expanded.explode('brand_key')
    expanded = expanded[expanded['brand_key'].notna()]
    return expanded[['brand_key', group_col]].rename(columns={group_col: 'grupo'})


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
    df = _read_tabular_with_fallbacks(file, 'Budget file', 'INPUT (Anual) Budget')

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

    Returns DataFrame: brand_key (uppercase) | grupo
    """
    df = _read_tabular_with_fallbacks(file, 'Families file', 'INPUT (Anual) Familias')

    df.columns = df.columns.str.strip()

    nombre_col, familia_col, grupo_col = _detect_family_columns(df)
    if not grupo_col or (not nombre_col and not familia_col):
        raise ValueError(
            "Families file missing required columns. "
            f"Detected columns: nombre={nombre_col}, familia={familia_col}, grupo={grupo_col}"
        )

    group_series = normalize_group_names_vectorized(df, grupo_col)
    valid_rows = df[group_series.notna()].copy()
    valid_rows['grupo'] = group_series[group_series.notna()].values

    if valid_rows.empty:
        return pd.DataFrame(columns=['brand_key', 'grupo'])

    frames = []
    for source_col in [familia_col, nombre_col]:
        if not source_col:
            continue
        expanded = _expand_brand_keys(valid_rows, source_col, group_col='grupo')
        if not expanded.empty:
            frames.append(expanded[['brand_key', 'grupo']])

    if not frames:
        return pd.DataFrame(columns=['brand_key', 'grupo'])

    result = pd.concat(frames, ignore_index=True).drop_duplicates(subset=['brand_key'])
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
        units=('unidades', 'sum'),
    )
    agg['margin_pct'] = safe_divide(agg['margin_eur'], agg['revenue'], fill_value=0.0)
    return agg


def merge_kpis(
    cy_sales: pd.DataFrame,
    ly_sales: pd.DataFrame,
    budget: pd.DataFrame | None,
    stock_cy: pd.DataFrame,
    stock_ly: pd.DataFrame,
    reference_date: date,
) -> pd.DataFrame:
    """Merge sales, budget, and stock inputs into a KPI table by brand."""
    cy = summarise_sales(cy_sales).rename(columns={
        'revenue': 'cy_revenue',
        'margin_eur': 'cy_margin_eur',
        'margin_pct': 'cy_margin_pct',
        'units': 'cy_units',
    })
    ly = summarise_sales(ly_sales).rename(columns={
        'revenue': 'ly_revenue',
        'margin_eur': 'ly_margin_eur',
        'margin_pct': 'ly_margin_pct',
        'units': 'ly_units',
    })

    merged = _merge_base_tables(cy, ly, budget, stock_cy, stock_ly)
    merged = _compute_growth_metrics(merged)
    merged = _compute_budget_metrics(merged, reference_date)
    merged = _compute_stock_metrics(merged, reference_date)
    merged = _compute_mix_and_contribution_metrics(merged)
    merged = _compute_unit_metrics(merged)
    merged['metric_window'] = 'YTD_LfL'
    return merged


def _merge_base_tables(
    cy: pd.DataFrame,
    ly: pd.DataFrame,
    budget: pd.DataFrame | None,
    stock_cy: pd.DataFrame,
    stock_ly: pd.DataFrame,
) -> pd.DataFrame:
    """Merge core CY/LY sales with optional budget and stock inputs."""
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
    return merged


def _compute_growth_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute revenue/margin growth deltas between CY and LY."""
    result = df.copy()
    result['growth_real'] = safe_divide(
        result['cy_revenue'] - result['ly_revenue'],
        result['ly_revenue'],
        fill_value=np.nan,
    )
    result['margin_delta_pts'] = result['cy_margin_pct'] - result['ly_margin_pct']
    result['margin_delta_eur'] = result['cy_margin_eur'] - result['ly_margin_eur']
    result['brand_status'] = np.where(result['ly_revenue'] > 0, 'Existing', 'New')
    return result


def _compute_budget_metrics(df: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    """Compute budget-to-date progress and gap metrics."""
    result = df.copy()
    year_start = AppConfig.get_year_start(reference_date.year)
    year_days = AppConfig.get_days_in_year(reference_date.year)
    elapsed_days = (reference_date - year_start).days + 1
    budget_to_date_factor = min(max(elapsed_days / year_days, 0.0), 1.0)
    result['budget_to_date_revenue'] = result['budget_revenue'] * budget_to_date_factor
    result['budget_achievement'] = safe_divide(
        result['cy_revenue'],
        result['budget_to_date_revenue'],
        fill_value=np.nan,
    )
    result['budget_gap_eur'] = result['cy_revenue'] - result['budget_to_date_revenue']
    result['budget_gap_pct'] = result['budget_achievement'] - 1
    return result


def _compute_stock_metrics(df: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    """Compute stock pressure metrics such as days of stock."""
    result = df.copy()
    year_start = AppConfig.get_year_start(reference_date.year)
    days_elapsed = max((reference_date - year_start).days + 1, 1)
    result['daily_revenue_cy'] = safe_divide(result['cy_revenue'], days_elapsed, fill_value=np.nan)
    result['days_stock'] = safe_divide(result['stock_cy'], result['daily_revenue_cy'], fill_value=np.nan)
    return result


def _compute_mix_and_contribution_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute brand mix and margin contribution percentages."""
    result = df.copy()
    total_cy_revenue = result['cy_revenue'].sum()
    total_cy_margin_eur = result['cy_margin_eur'].sum()
    result['mix_contribution_pct'] = safe_divide(
        result['cy_revenue'],
        pd.Series(total_cy_revenue, index=result.index),
        fill_value=np.nan,
    )
    result['margin_contribution_pct'] = safe_divide(
        result['cy_margin_eur'],
        pd.Series(total_cy_margin_eur, index=result.index),
        fill_value=np.nan,
    )
    return result


def _compute_unit_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute unit economics from CY sales and units."""
    result = df.copy()
    result[['cy_units', 'ly_units']] = result[['cy_units', 'ly_units']].fillna(0)
    result['revenue_per_unit'] = safe_divide(result['cy_revenue'], result['cy_units'], fill_value=np.nan)
    result['margin_per_unit'] = safe_divide(result['cy_margin_eur'], result['cy_units'], fill_value=np.nan)
    return result


def project_month_end(cy_sales_full: pd.DataFrame, reference_date: date) -> pd.DataFrame:
    import calendar

    data_max_ts = cy_sales_full['fecha'].max() if 'fecha' in cy_sales_full else None
    data_max_date = data_max_ts.date() if pd.notna(data_max_ts) else None
    validate_reference_date(reference_date, data_max_date)

    days_in_month = calendar.monthrange(reference_date.year, reference_date.month)[1]
    days_elapsed = max(reference_date.day, 1)
    cy_month = cy_sales_full[
        (cy_sales_full['fecha'].dt.month == reference_date.month) &
        (cy_sales_full['fecha'].dt.year == reference_date.year)
    ]

    if cy_month.empty:
        return pd.DataFrame(columns=['brand', 'cy_revenue_todate', 'projected_revenue', 'elapsed_pct'])

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
