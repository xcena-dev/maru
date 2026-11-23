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


def term(*words: str) -> str:
    """Match any of ``words`` as a whole word, also inside snake_case names.

    ``\\b`` treats ``_`` as part of a word, so it would miss ``_parked_blocks``.
    """
    return rf"(?<![a-z])(?:{'|'.join(words)})(?![a-z])"


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: str
    paths: tuple[str, ...]


RULES: list[Rule] = [
    Rule(
        "async-load",
        term(
            r"(un)?park(s|ed|ing)?",
            "deferred",
            r"inline[ _-](loads?|parts?|requests?|layerwise)",
        ),
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


def _rule(name: str) -> re.Pattern[str]:
    return re.compile(next(r for r in RULES if r.name == name).pattern, re.IGNORECASE)


@pytest.mark.parametrize(
    "text",
    [
        "self._parked_blocks",
        "def test_a_parked_load_runs",
        "_admit_parked(",
        "_note_failed_inline_load",
        "def test_inline_loads_of_groups",
        "self._deferred_lock",
        "A parked request",
    ],
)
def test_async_load_rule_catches_terms_inside_identifiers(text: str) -> None:
    assert _rule("async-load").search(text)


@pytest.mark.parametrize("text", ["sparkline", "DSpark drafter", "an inline scale"])
def test_async_load_rule_leaves_other_words_alone(text: str) -> None:
    assert not _rule("async-load").search(text)


@pytest.mark.parametrize("rule", RULES, ids=lambda r: r.name)
def test_no_retired_term(rule: Rule) -> None:
    assert rule.paths and any(any(ROOT.glob(g)) for g in rule.paths)
    assert _offending_lines(rule) == []
