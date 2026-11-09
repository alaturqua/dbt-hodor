"""`dbt_hodor.core` holds the pure modules: they import no dbt, no store and no adapter code."""

import ast
import sys
from pathlib import Path

import pytest

import dbt_hodor.core

CORE = Path(dbt_hodor.core.__file__).parent
ALLOWED = {"sqlglot"}  # the one third-party import: parsing is not I/O


def _imports(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            names.add("." * node.level + (node.module or ""))
    return names


@pytest.mark.parametrize("path", sorted(CORE.glob("*.py")), ids=lambda p: p.name)
def test_core_module_imports_only_the_standard_library_sqlglot_and_core(path):
    for name in _imports(path):
        top = name.split(".")[0]
        if name.startswith("."):  # `.records` stays in core, `..store` leaves it
            assert not name.startswith(".."), f"{path.name} imports {name}"
        elif top == "dbt_hodor":
            assert name.startswith("dbt_hodor.core"), f"{path.name} imports {name}"
        else:
            assert top in sys.stdlib_module_names or top in ALLOWED, f"{path.name} imports {name}"
