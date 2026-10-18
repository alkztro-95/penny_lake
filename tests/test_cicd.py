import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CicdConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bundle = (PROJECT_ROOT / "databricks.yml").read_text()
        cls.ci = (PROJECT_ROOT / ".github" / "workflows" / "ci.yml").read_text()
        cls.deploy = (PROJECT_ROOT / ".github" / "workflows" / "deploy.yml").read_text()

    def test_bundle_has_separate_test_catalogs_and_service_principal_root(self):
        for catalog in (
            "test_penny_lake_01_bronze",
            "test_penny_lake_02_silver",
            "test_penny_lake_03_gold",
            "test_penny_lake_04_governance",
        ):
            with self.subTest(catalog=catalog):
                self.assertIn(catalog, self.bundle)
        self.assertIn(
            "/Workspace/Users/cb2f526a-1a03-47da-a4f6-30729febe372/.bundle/${bundle.name}/${bundle.target}",
            self.bundle,
        )
        self.assertIn(
            "/Workspace/Users/76668225-6dad-4f36-bc24-e9b41f811953/.bundle/${bundle.name}/${bundle.target}",
            self.bundle,
        )

    def test_ci_runs_for_pull_requests_to_test_and_main(self):
        self.assertIn("pull_request:\n    branches: [test, main]", self.ci)
        self.assertIn("push:\n    branches: [test, main]", self.ci)

    def test_deploy_flow_uses_feature_test_and_merged_test_to_main(self):
        self.assertIn('"feature/**"', self.deploy)
        self.assertIn("github.ref == 'refs/heads/test'", self.deploy)
        self.assertIn("github.event.pull_request.head.ref == 'test'", self.deploy)
        self.assertIn("github.event.pull_request.base.ref == 'main'", self.deploy)
        self.assertIn("github.event.pull_request.merged == true", self.deploy)
        self.assertIn("github.event_name == 'pull_request'", self.deploy)

    def test_deployments_require_oidc_and_explicit_ready_flag(self):
        self.assertEqual(self.deploy.count("DATABRICKS_AUTH_TYPE: github-oidc"), 3)
        self.assertEqual(
            self.deploy.count("vars.DATABRICKS_DEPLOYMENTS_ENABLED == 'true'"),
            3,
        )
        self.assertEqual(self.deploy.count("id-token: write"), 1)


if __name__ == "__main__":
    unittest.main()
