import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "check_docs_updated", Path(__file__).resolve().parent.parent / "scripts" / "check_docs_updated.py"
)
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)


def test_code_change_without_docs_is_reported():
    assert check.missing_docs(["jobs/job.py", "tests/test_job.py", "main.py"]) == ["jobs/job.py", "main.py"]


def test_code_change_with_wiki_or_readme_passes():
    assert check.missing_docs(["jobs/job.py", "openwiki/architecture.md"]) == []
    assert check.missing_docs(["config/dev/example.yaml", "README.md"]) == []


def test_changes_outside_the_framework_need_no_docs():
    assert check.missing_docs(["tests/test_job.py", "training/04_x.py", ".github/workflows/ci.yml"]) == []
    assert check.missing_docs([]) == []
