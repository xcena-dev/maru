# SPDX-License-Identifier: Apache-2.0
"""Retired terms stay retired.

Maru once had several names for one idea (a "parked", "deferred" or "async"
load) and names that read as something else ("read promote"). Each rule
below lists a retired term and where it must not come back. A line that
names a deprecated alias on purpose says "legacy" or "deprecated".
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Deprecated knob names users may still pass; they keep their spelling.
ALLOWED_TOKENS = ("maru_enable_deferred_loading", "maru_enable_write_behind")


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: str
    paths: tuple[str, ...]


RULES: list[Rule] = [
    Rule(
        "async-load",
        r"\b(un)?park(s|ed|ing)?\b|\bdeferred\b|_deferred|deferred_"
        r"|\binline[ _-](load|loads|part|parts|request|requests)\b",
        (
            "maru_vllm/**/*.py",
            "tests/unit/test_vllm_*.py",
            "tests/unit/vllm_connector_helpers.py",
            "docs/source/integration/vllm.md",
            "docs/source/getting_started/examples/vllm/*.md",
            "examples/vllm/**/README.md",
        ),
    ),
]


def _offending_lines(rule: Rule) -> list[str]:
    pattern = re.compile(rule.pattern, re.IGNORECASE)
    files = sorted({p for glob in rule.paths for p in ROOT.glob(glob) if p.is_file()})
    found = []
    for path in files:
        for number, line in enumerate(path.read_text().splitlines(), start=1):
            for token in ALLOWED_TOKENS:
                line = line.replace(token, "")
            lowered = line.lower()
            if "legacy" in lowered or "deprecated" in lowered:
                continue
            if pattern.search(line):
                found.append(f"{path.relative_to(ROOT)}:{number}: {line.strip()}")
    return found


@pytest.mark.parametrize("rule", RULES, ids=lambda r: r.name)
def test_no_retired_term(rule: Rule) -> None:
    assert rule.paths and any(any(ROOT.glob(g)) for g in rule.paths)
    assert _offending_lines(rule) == []
