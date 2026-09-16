#!/usr/bin/env python3
"""Execute the first ```python fence in each given markdown file."""

from __future__ import annotations

import pathlib
import re
import subprocess
import sys
import tempfile


def first_python(path: pathlib.Path) -> str:
    text = path.read_text()
    match = re.search(r"```python\n(.*?)```", text, re.S)
    if not match:
        raise SystemExit(f"no python fence in {path}")
    return match.group(1)


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit("usage: run-agent-python-fences.py FILE...")
    for raw in sys.argv[1:]:
        path = pathlib.Path(raw)
        code = first_python(path)
        with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as handle:
            handle.write(code)
            tmp = handle.name
        print(f"running first python fence from {path}")
        subprocess.check_call([sys.executable, tmp])


if __name__ == "__main__":
    main()
