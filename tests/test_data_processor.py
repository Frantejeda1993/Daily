import io
import unittest
from unittest.mock import patch

import pandas as pd

from data_processor import (
    _normalize_column_name,
    _detect_family_columns,
    candidate_brand_keys,
    normalize_group_names_vectorized,
    _expand_brand_keys,
    parse_families,
    safe_max_date,
)


class DataProcessorHelpersTest(unittest.TestCase):
    def test_normalize_column_name_removes_accents_spacing_and_case(self):
        self.assertEqual(_normalize_column_name('  ClávE   1  '), 'clave 1')

    def test_detect_family_columns_with_aliases_and_variants(self):
        df = pd.DataFrame(columns=['BRAND', 'Clave1', 'Categoría'])
        nombre_col, familia_col, grupo_col = _detect_family_columns(df)
        self.assertEqual(nombre_col, 'BRAND')
        self.assertEqual(familia_col, 'Clave1')
        self.assertEqual(grupo_col, 'Categoría')

    def test_candidate_brand_keys_generates_compact_and_short_name_variants(self):
        keys = candidate_brand_keys('300 - FAMILIA SHOKZ')
        self.assertIn('300 - FAMILIA SHOKZ', keys)
        self.assertIn('SHOKZ', keys)

    def test_normalize_group_names_vectorized_maps_known_groups(self):
        frame = pd.DataFrame({'raw': ['2 Wheels', 'Free Time', 'Outdoor Tech', 'Other']})
        result = normalize_group_names_vectorized(frame, 'raw')
        self.assertEqual(result.iloc[0], '2 Wheels')
        self.assertEqual(result.iloc[1], 'Free Time')
        self.assertEqual(result.iloc[2], 'Outdoor Tech')
        self.assertTrue(pd.isna(result.iloc[3]))

    def test_expand_brand_keys_explodes_brand_variants(self):
        valid_rows = pd.DataFrame(
            {
                'familia': ['300 - FAMILIA SHOKZ'],
                'grupo': ['Outdoor Tech'],
            }
        )
        expanded = _expand_brand_keys(valid_rows, 'familia')
        actual = set(map(tuple, expanded[['brand_key', 'grupo']].to_numpy()))
        self.assertEqual(
            actual,
            {
                ('300 - FAMILIA SHOKZ', 'Outdoor Tech'),
                ('SHOKZ', 'Outdoor Tech'),
            },
        )


class SafeMaxDateTest(unittest.TestCase):
    def test_safe_max_date_handles_string_dates(self):
        series = pd.Series(["01/01/2024", "31/01/2024", "bad"])
        self.assertEqual(safe_max_date(series), pd.Timestamp("2024-01-31").date())

    def test_safe_max_date_returns_none_when_no_valid_dates(self):
        series = pd.Series(["bad", None])
        self.assertIsNone(safe_max_date(series))


class ParseFamiliesIntegrationTest(unittest.TestCase):
    @patch('data_processor._read_tabular_with_fallbacks')
    def test_parse_families_with_column_variants(self, mock_reader):
        mock_reader.return_value = pd.DataFrame(
            {
                'Marca': ['Shokz', 'Nemo'],
                'Clave 1': ['300 - FAMILIA SHOKZ', '400 - FAMILIA NEMO'],
                'Group': ['outdoor tech', 'free time'],
            }
        )

        result = parse_families(io.BytesIO(b'test'))
        result_keys = set(map(tuple, result[['brand_key', 'grupo']].to_numpy()))

        expected_keys = {
            ('SHOKZ', 'Outdoor Tech'),
            ('300 - FAMILIA SHOKZ', 'Outdoor Tech'),
            ('NEMO', 'Free Time'),
            ('400 - FAMILIA NEMO', 'Free Time'),
        }
        self.assertEqual(result_keys, expected_keys)
        self.assertEqual(list(result.columns), ['brand_key', 'grupo'])

    @patch('data_processor._read_tabular_with_fallbacks')
    def test_parse_families_raises_when_required_columns_missing(self, mock_reader):
        mock_reader.return_value = pd.DataFrame({'Nombre': ['Shokz'], 'Familia': ['x']})

        with self.assertRaisesRegex(ValueError, r"Detected columns: nombre=Nombre, familia=Familia, grupo=None"):
            parse_families(io.BytesIO(b'test'))


if __name__ == '__main__':
    unittest.main()
