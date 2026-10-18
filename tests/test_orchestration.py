import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
JOB = PROJECT_ROOT / "resources" / "expense_refresh.job.yml"


class ExpenseRefreshOrchestrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.job = JOB.read_text()

    def test_job_runs_bronze_silver_then_gold(self):
        task_order = [
            self.job.index("task_key: ingest_bronze"),
            self.job.index("task_key: refresh_silver"),
            self.job.index("task_key: refresh_gold"),
            self.job.index("task_key: apply_governance"),
        ]
        self.assertEqual(task_order, sorted(task_order))
        self.assertIn("- task_key: ingest_bronze", self.job)
        self.assertIn("- task_key: refresh_silver", self.job)
        self.assertIn("- task_key: refresh_gold", self.job)
        self.assertIn("- task_key: apply_governance", self.job)
        self.assertIn("depends_on:\n            - task_key: ingest_bronze", self.job)
        self.assertIn("depends_on:\n            - task_key: refresh_silver", self.job)
        self.assertIn("depends_on:\n            - task_key: refresh_gold", self.job)
        self.assertIn("notebook_path: ../src/apply_governance.py", self.job)

    def test_bronze_task_retries_transient_failures(self):
        bronze_task = self.job.split("- task_key: ingest_bronze", 1)[1].split(
            "- task_key: refresh_silver", 1
        )[0]
        self.assertIn("max_retries: 2", bronze_task)
        self.assertIn("min_retry_interval_millis: 30000", bronze_task)

    def test_file_arrival_trigger_watches_target_specific_landing_volume(self):
        self.assertIn("pause_status: UNPAUSED", self.job)
        self.assertIn("file_arrival:", self.job)
        self.assertIn(
            "url: /Volumes/${var.catalog}/${var.landing_schema}/${var.transactions_volume}/",
            self.job,
        )
        self.assertIn("wait_after_last_change_seconds: 60", self.job)


if __name__ == "__main__":
    unittest.main()
