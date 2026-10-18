import ast
import unittest
from pathlib import Path

NOTEBOOK = Path(__file__).resolve().parents[1] / "src" / "silver_metrics.py"


class SilverMetricsNotebookTests(unittest.TestCase):
    def test_notebook_is_valid_python_with_separate_before_and_after_cells(self):
        source = NOTEBOOK.read_text()
        ast.parse(source)
        cells = source.split("# COMMAND ----------")

        self.assertEqual(len(cells), 4)
        self.assertIn("FROM {bronze_expenses}", cells[2])
        self.assertIn("GROUP BY source_file_path", cells[2])
        self.assertIn("FROM {silver_expenses}", cells[3])
        self.assertNotIn("__END_AT", cells[3])
        self.assertIn("dbutils.widgets.text", cells[1])
