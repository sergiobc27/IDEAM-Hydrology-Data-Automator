import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ideam_socrata.exporting import export_by_department_municipality
from ideam_socrata.query_validation import build_department_filter
from ideam_socrata.transform import normalize_chunk


class QueryExportHelperTests(unittest.TestCase):
    def test_department_filter_includes_accent_variants(self):
        mapping = {"ATLANTICO": ["ATLANTICO", "ATLÁNTICO"]}

        where, replacements, variants = build_department_filter(["ATLANTICO"], mapping)

        self.assertIn("upper(departamento) IN", where)
        self.assertIn("'ATLANTICO'", where)
        self.assertIn("'ATLÁNTICO'", where)
        self.assertIn("ATLANTICO", variants)
        self.assertEqual(replacements["ATLANTICO"], "ATLANTICO")
        self.assertEqual(replacements["ATLÁNTICO"], "ATLANTICO")

    def test_department_normalization_is_accent_insensitive(self):
        data = [
            {
                "codigoestacion": "1",
                "codigosensor": "2",
                "fechaobservacion": "2024-01-01T00:00:00.000",
                "valorobservado": "1.2",
                "departamento": "Atlántico",
                "municipio": "Barranquilla",
            }
        ]
        replacements = {"ATLANTICO": "ATLANTICO", "ATLÁNTICO": "ATLANTICO"}

        df = normalize_chunk(data, "s54a-sgyg", "fechaobservacion", replacements)

        self.assertEqual(df.loc[0, "departamento"], "ATLANTICO")
        self.assertIn("floating_id", df.columns)

    def test_export_organizes_and_splits_csv(self):
        df = pd.DataFrame(
            {
                "departamento": ["ATLANTICO"] * 5,
                "municipio": ["BARRANQUILLA"] * 5,
                "valorobservado": [1, 2, 3, 4, 5],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            outputs = export_by_department_municipality(
                df,
                "Precipitación",
                base_dir=tmpdir,
                include_csv=True,
                timestamp="1200_010126",
                max_csv_rows=3,
            )

            self.assertEqual(len(outputs), 1)
            parquet_path = Path(outputs[0]["parquet"])
            csv_paths = [Path(path) for path in outputs[0]["csv"]]

            self.assertTrue(parquet_path.exists())
            self.assertEqual(parquet_path.parent.name, "BARRANQUILLA")
            self.assertEqual(parquet_path.parent.parent.name, "ATLANTICO")
            self.assertEqual(len(csv_paths), 3)
            self.assertTrue(csv_paths[0].name.endswith("1200_010126.csv"))
            self.assertTrue(csv_paths[1].name.endswith("1200_010126_2.csv"))
            self.assertTrue(csv_paths[2].name.endswith("1200_010126_3.csv"))


if __name__ == "__main__":
    unittest.main()
