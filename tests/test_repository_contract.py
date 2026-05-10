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
        self.assertIn("cd hyacinth-sentry", readme)
        self.assertIn("python -m uvicorn hyacinth_sentry.server:app", readme)

    def test_repo_contains_local_screenshots(self) -> None:
        self.assertTrue((ROOT / "img" / "1.png").exists())
        self.assertTrue((ROOT / "img" / "2.png").exists())

    def test_prd_is_not_linked_from_readme_and_is_gitignored(self) -> None:
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
        self.assertNotIn("PRD.md", readme)
        self.assertIn("PRD.md", gitignore)


if __name__ == "__main__":
    unittest.main()
