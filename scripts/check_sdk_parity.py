#!/usr/bin/env python3
"""Guard: the siphon-sip SDK mock of the ``smpp`` namespace must not drift.

The siphon-sip SDK (``pip install siphon-sip``) ships a mock of the ``smpp``
namespace that this crate injects into siphon at runtime, so SMPP scripts can be
unit-tested and authored with type hints without a running SMSC. This script
derives the namespace surface from *this repo's* runtime sources and asserts
every exposed name is present on the installed SDK mock.

Run in CI after ``pip install siphon-sip``. Exits non-zero (listing the missing
names) if the mock is behind — update ``sdk/siphon_sdk/smpp.py`` in siphon-sip to
match, then land that before this repo's change.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def python_surface() -> set[str]:
    """Top-level decorators + config readouts defined in ``python/smpp.py``."""
    tree = ast.parse((ROOT / "python" / "smpp.py").read_text())
    return {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and not node.name.startswith("_")
    }


def pyfunction_surface() -> set[str]:
    """``#[pyfunction] pub fn NAME`` send helpers from ``src/sends.rs``."""
    src = (ROOT / "src" / "sends.rs").read_text()
    names: set[str] = set()
    for chunk in src.split("#[pyfunction]")[1:]:
        match = re.search(r"pub fn (\w+)", chunk)
        if match:
            names.add(match.group(1))
    return names


def pyclass_surface() -> set[str]:
    """``#[pyclass(..., name = "NAME")]`` pyclasses from the Rust sources."""
    names: set[str] = set()
    for rel in ("src/pyclasses.rs", "src/sends.rs"):
        src = (ROOT / rel).read_text()
        for block in re.finditer(r"#\[pyclass\(([^)]*)\)\]", src):
            match = re.search(r'name\s*=\s*"(\w+)"', block.group(1))
            if match:
                names.add(match.group(1))
    return names


def main() -> int:
    try:
        from siphon_sdk import mock_module
    except ImportError:
        print(
            "ERROR: siphon-sip SDK not installed — run `pip install siphon-sip`",
            file=sys.stderr,
        )
        return 2

    mock_module.install()
    from siphon import smpp  # resolvable after install()

    expected = python_surface() | pyfunction_surface() | pyclass_surface()
    if not expected:
        print("ERROR: derived an empty smpp surface — parser out of date?",
              file=sys.stderr)
        return 2

    missing = sorted(name for name in expected if not hasattr(smpp, name))

    print(f"smpp namespace surface: {len(expected)} names checked")
    if missing:
        print(
            "\nMISSING from the siphon-sip SDK mock (sdk/siphon_sdk/smpp.py):",
            file=sys.stderr,
        )
        for name in missing:
            print(f"  - smpp.{name}", file=sys.stderr)
        print(
            "\nAdd them to sdk/siphon_sdk/smpp.py in siphon-sip and merge that first.",
            file=sys.stderr,
        )
        return 1

    print("OK — the SDK mock covers the full smpp runtime surface.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
