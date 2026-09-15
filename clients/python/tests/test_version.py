"""`zydecodb.__version__` must match the version published in pyproject.toml."""

import re
from pathlib import Path

import zydecodb

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _pyproject_version() -> str:
    # Regex rather than tomllib: the package supports Python 3.9, which has no
    # stdlib TOML parser, and the test must not add a dependency.
    text = PYPROJECT.read_text(encoding="utf-8")
    project = re.search(r"^\[project\]\n(.*?)(?=^\[|\Z)", text, re.M | re.S)
    assert project, "no [project] table in pyproject.toml"
    m = re.search(r'^version\s*=\s*"([^"]+)"', project.group(1), re.M)
    assert m, "no version in [project] table"
    return m.group(1)


def test_version_matches_pyproject():
    assert zydecodb.__version__ == _pyproject_version()
