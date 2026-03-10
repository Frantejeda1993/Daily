from datetime import date
import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from app_settings import AppConfig
from data_processor import (
    _compute_budget_metrics,
    _compute_growth_metrics,
    _compute_mix_and_contribution_metrics,
    _compute_stock_metrics,
    _compute_unit_metrics,
    _merge_base_tables,
    merge_kpis,
    safe_divide,
    summarise_sales,
)


class DataProcessorPipelineTests(unittest.TestCase):
    def test_merge_base_tables_merges_budget_and_stock(self):
        cy = pd.DataFrame({'brand': ['A'], 'cy_revenue': [100], 'cy_margin_eur': [40], 'cy_margin_pct': [0.4], 'cy_units': [5]})
        ly = pd.DataFrame({'brand': ['B'], 'ly_revenue': [50], 'ly_margin_eur': [10], 'ly_margin_pct': [0.2], 'ly_units': [2]})
        budget = pd.DataFrame({'brand': ['A'], 'budget_revenue': [200], 'budget_margin_pct': [20]})
        stock_cy = pd.DataFrame({'brand': ['A'], 'stock_value': [300]})
        stock_ly = pd.DataFrame({'brand': ['B'], 'stock_value': [100]})

        result = _merge_base_tables(cy, ly, budget, stock_cy, stock_ly).sort_values('brand').reset_index(drop=True)

        self.assertEqual(set(result['brand']), {'A', 'B'})
        self.assertEqual(result.loc[result['brand'] == 'A', 'stock_cy'].iat[0], 300)
        self.assertEqual(result.loc[result['brand'] == 'B', 'stock_ly'].iat[0], 100)
        self.assertEqual(result.loc[result['brand'] == 'B', 'budget_revenue'].iat[0], 0)

    def test_compute_growth_metrics(self):
        df = pd.DataFrame({
            'brand': ['A', 'B'],
            'cy_revenue': [120, 80],
            'ly_revenue': [100, 0],
            'cy_margin_pct': [0.3, 0.2],
            'ly_margin_pct': [0.25, 0.1],
            'cy_margin_eur': [36, 16],
            'ly_margin_eur': [25, 0],
        })

        result = _compute_growth_metrics(df)

        self.assertAlmostEqual(result.loc[0, 'growth_real'], 0.2)
        self.assertTrue(np.isnan(result.loc[1, 'growth_real']))
        self.assertEqual(result.loc[0, 'brand_status'], 'Existing')
        self.assertEqual(result.loc[1, 'brand_status'], 'New')

    def test_compute_budget_metrics_reuses_budget_achievement(self):
        ref = date(2024, 6, 30)
        df = pd.DataFrame({'cy_revenue': [100.0], 'budget_revenue': [365.0]})

        result = _compute_budget_metrics(df, ref)

        year_start = AppConfig.get_year_start(ref.year)
        factor = ((ref - year_start).days + 1) / AppConfig.get_days_in_year(ref.year)
        expected_to_date = 365.0 * factor
        self.assertAlmostEqual(result.loc[0, 'budget_to_date_revenue'], expected_to_date)
        self.assertAlmostEqual(result.loc[0, 'budget_gap_pct'], result.loc[0, 'budget_achievement'] - 1)

    def test_compute_stock_metrics(self):
        ref = date(2024, 1, 10)
        df = pd.DataFrame({'cy_revenue': [100.0], 'stock_cy': [200.0]})

        result = _compute_stock_metrics(df, ref)

        self.assertAlmostEqual(result.loc[0, 'daily_revenue_cy'], 10.0)
        self.assertAlmostEqual(result.loc[0, 'days_stock'], 20.0)

    def test_compute_mix_and_contribution_metrics(self):
        df = pd.DataFrame({'cy_revenue': [100.0, 300.0], 'cy_margin_eur': [20.0, 80.0]})

        result = _compute_mix_and_contribution_metrics(df)

        self.assertAlmostEqual(result.loc[0, 'mix_contribution_pct'], 0.25)
        self.assertAlmostEqual(result.loc[1, 'margin_contribution_pct'], 0.8)

    def test_compute_unit_metrics(self):
        df = pd.DataFrame({'cy_revenue': [100.0], 'cy_margin_eur': [20.0], 'cy_units': [4.0], 'ly_units': [np.nan]})

        result = _compute_unit_metrics(df)

        self.assertEqual(result.loc[0, 'ly_units'], 0)
        self.assertAlmostEqual(result.loc[0, 'revenue_per_unit'], 25.0)
        self.assertAlmostEqual(result.loc[0, 'margin_per_unit'], 5.0)

    def test_merge_kpis_regression_matches_legacy_behavior(self):
        reference_date = date(2024, 3, 31)
        cy_sales = pd.DataFrame({
            'brand': ['A', 'A', 'B'],
            'importe': [100.0, 60.0, 90.0],
            'margen_eur': [30.0, 12.0, 18.0],
            'unidades': [5, 3, 9],
        })
        ly_sales = pd.DataFrame({
            'brand': ['A', 'C'],
            'importe': [80.0, 70.0],
            'margen_eur': [16.0, 14.0],
            'unidades': [4, 7],
        })
        budget = pd.DataFrame({
            'brand': ['A', 'B', 'C'],
            'budget_revenue': [365.0, 730.0, 365.0],
            'budget_margin_pct': [20.0, 25.0, 30.0],
        })
        stock_cy = pd.DataFrame({'brand': ['A', 'B'], 'stock_value': [320.0, 100.0]})
        stock_ly = pd.DataFrame({'brand': ['A', 'C'], 'stock_value': [200.0, 150.0]})

        expected = self._legacy_merge_kpis(cy_sales, ly_sales, budget, stock_cy, stock_ly, reference_date)
        actual = merge_kpis(cy_sales, ly_sales, budget, stock_cy, stock_ly, reference_date)

        self.assertEqual(set(actual.columns), set(expected.columns))
        sort_cols = ['brand']
        expected = expected.sort_values(sort_cols).reset_index(drop=True)
        actual = actual.sort_values(sort_cols).reset_index(drop=True)
        expected = expected[actual.columns]
        assert_frame_equal(actual, expected, check_dtype=False, check_exact=False, atol=1e-10, rtol=1e-10)

    @staticmethod
    def _legacy_merge_kpis(cy_sales, ly_sales, budget, stock_cy, stock_ly, reference_date):
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
        merged = cy.merge(ly, on='brand', how='outer').fillna(0)

        merged = merged.merge(budget[['brand', 'budget_revenue', 'budget_margin_pct']], on='brand', how='left')
        merged.fillna(0, inplace=True)

        for stk_df, col_name in [(stock_cy, 'stock_cy'), (stock_ly, 'stock_ly')]:
            stk = stk_df.groupby('brand', as_index=False)['stock_value'].sum()
            merged = merged.merge(stk.rename(columns={'stock_value': col_name}), on='brand', how='left')
        merged.fillna(0, inplace=True)

        merged['growth_real'] = safe_divide(merged['cy_revenue'] - merged['ly_revenue'], merged['ly_revenue'], fill_value=np.nan)
        year_start = AppConfig.get_year_start(reference_date.year)
        year_days = AppConfig.get_days_in_year(reference_date.year)
        elapsed_days = (reference_date - year_start).days + 1
        budget_to_date_factor = min(max(elapsed_days / year_days, 0.0), 1.0)
        merged['budget_to_date_revenue'] = merged['budget_revenue'] * budget_to_date_factor
        merged['budget_achievement'] = safe_divide(merged['cy_revenue'], merged['budget_to_date_revenue'], fill_value=np.nan)
        merged['budget_gap_eur'] = merged['cy_revenue'] - merged['budget_to_date_revenue']
        merged['budget_gap_pct'] = safe_divide(merged['cy_revenue'], merged['budget_to_date_revenue'], fill_value=np.nan) - 1
        merged['margin_delta_pts'] = merged['cy_margin_pct'] - merged['ly_margin_pct']
        merged['margin_delta_eur'] = merged['cy_margin_eur'] - merged['ly_margin_eur']
        days_elapsed = max((reference_date - year_start).days + 1, 1)
        merged['daily_revenue_cy'] = merged['cy_revenue'] / days_elapsed
        merged['days_stock'] = safe_divide(merged['stock_cy'], merged['daily_revenue_cy'], fill_value=np.nan)
        total_cy_revenue = merged['cy_revenue'].sum()
        total_cy_margin_eur = merged['cy_margin_eur'].sum()
        merged['mix_contribution_pct'] = safe_divide(merged['cy_revenue'], pd.Series(total_cy_revenue, index=merged.index), fill_value=np.nan)
        merged['margin_contribution_pct'] = safe_divide(merged['cy_margin_eur'], pd.Series(total_cy_margin_eur, index=merged.index), fill_value=np.nan)
        merged['brand_status'] = np.where(merged['ly_revenue'] > 0, 'Existing', 'New')

        merged[['cy_units', 'ly_units']] = merged[['cy_units', 'ly_units']].fillna(0)
        merged['revenue_per_unit'] = safe_divide(merged['cy_revenue'], merged['cy_units'], fill_value=np.nan)
        merged['margin_per_unit'] = safe_divide(merged['cy_margin_eur'], merged['cy_units'], fill_value=np.nan)
        merged['metric_window'] = 'YTD_LfL'
        return merged


if __name__ == '__main__':
    unittest.main()
