import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class RepositoryContractTests(unittest.TestCase):
    def test_python_package_is_hyacinth_sentry(self) -> None:
        spec = importlib.util.find_spec("hyacinth_sentry")
        self.assertIsNotNone(spec)

    def test_readme_uses_clone_and_package_entrypoint(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("git clone", readme)
        self.assertIn("hyacinth-sentry-douyu.git", readme)
        self.assertIn("cd hyacinth-sentry-douyu", readme)
        self.assertIn("python -m uvicorn hyacinth_sentry.server:app", readme)

    def test_repo_contains_local_screenshots(self) -> None:
        self.assertTrue((ROOT / "img" / "1.png").exists())
        self.assertTrue((ROOT / "img" / "2.png").exists())

    def test_tools_and_manual_tests_are_reorganized(self) -> None:
        self.assertTrue((ROOT / "tools" / "maintenance" / "clear_db.py").exists())
        self.assertTrue((ROOT / "tools" / "maintenance" / "cleanup_bad_sc.py").exists())
        self.assertTrue((ROOT / "tools" / "forensics" / "playwright_monitor.py").exists())
        self.assertTrue((ROOT / "tools" / "forensics" / "sniff.py").exists())
        self.assertTrue((ROOT / "tools" / "forensics" / "probe.py").exists())
        self.assertTrue((ROOT / "tools" / "forensics" / "capture_chat.py").exists())
        self.assertTrue((ROOT / "tools" / "forensics" / "gid_ab.py").exists())
        self.assertTrue((ROOT / "tests" / "manual_e2e_smoke.py").exists())

    def test_readme_documents_clear_db_command(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("python -m tools.maintenance.clear_db --yes", readme)
        self.assertIn("会先备份数据库", readme)

    def test_readme_documents_admin_password_source(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("DOUYU_ADMIN_PASSWORD", readme)
        self.assertIn("未设置时默认 `admin`", readme)
        self.assertIn("公网部署前必须设置", readme)

    def test_prd_is_not_linked_from_readme_and_is_gitignored(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertNotIn("PRD.md", readme)
        self.assertIn("PRD.md", gitignore)


if __name__ == "__main__":
    unittest.main()
