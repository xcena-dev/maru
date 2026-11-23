# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Only the drivers and the POOL_KINDS table branch on a pool medium. A
branch is a comparison, membership test or match case against a PoolKind
member or a medium name ("dram", "cxl"), a set or table of PoolKind
members, or a PoolKind(...) / PoolKind[...] conversion. The wire-boundary
checks that stay are listed with their count and reason."""

import ast
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import pytest

from maru_common.storage_types import PoolKind

ROOT = Path(__file__).resolve().parents[2]
SCANNED = ("maru_server", "maru_common")
# Where medium branches belong: the drivers, and the POOL_KINDS table itself
FREE = ("maru_storage_service/drivers/",)
TABLE = ("maru_common/storage_types.py", "<module>", "POOL_KINDS")
MEMBERS = frozenset(k.name for k in PoolKind)
NAMES = frozenset(k.value for k in PoolKind)

# (file, function): (branches, why they stay)
ALLOWED: dict[tuple[str, str], tuple[int, str]] = {
    ("maru_server/node_registry.py", "NodeRegistry._pool_error"): (
        1,
        "NODE_OPEN check: each wire kind has its own pool id rule",
    ),
    ("maru_common/config.py", "MaruConfig.__post_init__"): (
        1,
        "MaruHandler storage_backend names, not a PoolKind",
    ),
}


@dataclass(frozen=True)
class Branch:
    path: str
    scope: str  # enclosing classes and functions, dotted; <module> at the top
    line: int
    form: str

    def __str__(self) -> str:
        return f"{self.path}:{self.line} in {self.scope}: {self.form}"


class _Finder(ast.NodeVisitor):
    def __init__(self, path: str, tree: ast.AST):
        self.path = path
        self.scope: list[str] = []
        self.found: list[Branch] = []
        self._operands: set[int] = set()  # counted with their comparison
        self._target = ""  # the name a module-level assignment binds
        # Names bound to PoolKind or to one of its members, aliases included
        self.kinds = {"PoolKind"}
        self.members: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name == "PoolKind":
                        self.kinds.add(alias.asname or alias.name)
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                names = [t.id for t in node.targets if isinstance(t, ast.Name)]
                if self._is_pool_kind(node.value):
                    self.kinds.update(names)
                elif self._member(node.value):
                    self.members.update(names)

    def _is_pool_kind(self, node: ast.AST) -> bool:
        return (isinstance(node, ast.Name) and node.id in self.kinds) or (
            isinstance(node, ast.Attribute) and node.attr == "PoolKind"
        )

    def _member(self, node: ast.AST) -> bool:
        """PoolKind.X, qualified or aliased, or a name bound to one."""
        if isinstance(node, ast.Name):
            return node.id in self.members
        return (
            isinstance(node, ast.Attribute)
            and node.attr in MEMBERS
            and self._is_pool_kind(node.value)
        )

    def _kind(self, node: ast.AST) -> bool:
        """A member, a medium name, or a literal collection holding one."""
        if self._member(node) or (
            isinstance(node, ast.Constant) and node.value in NAMES
        ):
            return True
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            return any(self._kind(e) for e in node.elts)
        return False

    def _add(self, node: ast.AST, form: str) -> None:
        scope = ".".join(self.scope) or "<module>"
        if (self.path, scope, self._target) == TABLE:
            return
        self.found.append(Branch(self.path, scope, node.lineno, form))

    def visit_Assign(self, node: ast.Assign) -> None:
        names = [t.id for t in node.targets if isinstance(t, ast.Name)]
        self._target = names[0] if len(names) == 1 and not self.scope else ""
        self.generic_visit(node)
        self._target = ""

    def _enter(self, node) -> None:
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._enter(node)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._enter(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._enter(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        operands = [node.left, *node.comparators]
        if any(self._kind(o) for o in operands):
            self._add(node, "comparison")
            self._operands.update(id(o) for o in operands)
        self.generic_visit(node)

    def visit_match_case(self, node: ast.match_case) -> None:
        for pattern in ast.walk(node.pattern):
            if isinstance(pattern, ast.MatchValue) and self._kind(pattern.value):
                self._add(pattern, "match case")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        if self._is_pool_kind(node.func):
            self._add(node, "PoolKind() conversion")
        self.generic_visit(node)

    def visit_Subscript(self, node: ast.Subscript) -> None:
        if self._is_pool_kind(node.value):
            self._add(node, "PoolKind[] conversion")
        self.generic_visit(node)

    def _collection(self, node: ast.AST, items: list) -> None:
        if id(node) not in self._operands and any(self._member(i) for i in items if i):
            self._add(node, "set or table of kinds")
        self.generic_visit(node)

    def visit_Tuple(self, node: ast.Tuple) -> None:
        self._collection(node, node.elts)

    def visit_List(self, node: ast.List) -> None:
        self._collection(node, node.elts)

    def visit_Set(self, node: ast.Set) -> None:
        self._collection(node, node.elts)

    def visit_Dict(self, node: ast.Dict) -> None:
        self._collection(node, [*node.keys, *node.values])


def find_branches(source: str, path: str) -> list[Branch]:
    tree = ast.parse(source, path)
    finder = _Finder(path, tree)
    finder.visit(tree)
    return finder.found


def scan() -> list[Branch]:
    found: list[Branch] = []
    for package in SCANNED:
        for file in sorted((ROOT / package).rglob("*.py")):
            path = file.relative_to(ROOT).as_posix()
            found += find_branches(file.read_text(), path)
    return found


def unlisted(found: list[Branch]) -> list[Branch]:
    """Branches outside FREE that ALLOWED does not cover: in a function it
    does not list, or more than it lists there."""
    outside = [b for b in found if not b.path.startswith(FREE)]
    counts = Counter((b.path, b.scope) for b in outside)
    return [
        b
        for b in outside
        if counts[b.path, b.scope] > ALLOWED.get((b.path, b.scope), (0, ""))[0]
    ]


# =============================================================================
# The code as it is
# =============================================================================


def test_no_kind_branch_outside_the_drivers_and_the_list():
    extra = unlisted(scan())
    assert not extra, (
        "branches on a pool medium outside the drivers: use POOL_KINDS or the "
        "tier, or list a wire-boundary check in ALLOWED with its reason:\n"
        + "\n".join(map(str, extra))
    )


def test_each_listed_branch_is_still_there():
    counts = Counter((b.path, b.scope) for b in scan())
    have = {key: counts[key] for key in ALLOWED}
    assert have == {key: n for key, (n, _) in ALLOWED.items()}, "update ALLOWED"


def test_the_scan_reads_every_package_and_skips_only_the_kind_table():
    for package in SCANNED:
        assert any((ROOT / package).rglob("*.py")), package
    source = (ROOT / TABLE[0]).read_text()
    assert "POOL_KINDS = {" in source
    table_only = source.replace("POOL_KINDS = {", "POOL_KINDS_OTHER = {")
    found = find_branches(table_only, TABLE[0])
    assert any(b.form == "set or table of kinds" for b in found)


# =============================================================================
# The checker on sources made up here
# =============================================================================


NEW = "maru_storage_service/new.py"


@pytest.mark.parametrize(
    "source",
    [
        "if info.kind == PoolKind.CXL:\n    pass",
        "if kind is storage_types.PoolKind.DRAM:\n    pass",
        "ok = kind in (PoolKind.DRAM, PoolKind.CXL)",
        "match kind:\n    case PoolKind.CXL:\n        pass",
        'match store:\n    case "dram" | "x":\n        pass',
        'if tier.store == "cxl":\n    pass',
        'size = 1 if name != "dram" else 2',
        "kind = PoolKind(tier.store)",
        "kind = PoolKind[name.upper()]",
        "LOCAL = {PoolKind.DRAM, PoolKind.CXL}",
        "COST = {PoolKind.CXL: 3}",
        'KIND_OF = {"cxl": PoolKind.CXL}',
        "for kind in (PoolKind.DRAM, PoolKind.CXL):\n    pass",
        "from maru_common.storage_types import PoolKind as PK\nif k == PK.CXL:\n    pass",
        "CXL = PoolKind.CXL\nif info.kind == CXL:\n    pass",
    ],
)
def test_a_new_kind_branch_fails_the_guard(source):
    found = find_branches(f"def f():\n{_indent(source)}", NEW)
    assert len(found) == 1 and found[0].scope == "f"
    assert unlisted(found) == found


@pytest.mark.parametrize(
    "source",
    [
        "if location.kind != info.kind:\n    pass",
        "if POOL_KINDS[info.kind].mappable:\n    pass",
        'info = PoolInfo("p", PoolKind.CXL, "n", 1, 4096, 4096)',
        'logger.info("dram tier: %s", tier.describe())',
        'if name == "dram_pool":\n    pass',
        'kinds = ["dram", "cxl"]',
    ],
)
def test_what_is_not_a_kind_branch(source):
    assert find_branches(source, NEW) == []


def test_a_listed_function_takes_only_its_listed_count():
    path = "maru_server/node_registry.py"
    checks = [
        "if info.kind == PoolKind.DRAM:\n    pass",
        "if info.kind == PoolKind.CXL:\n    pass",
    ]

    def method(n: int) -> str:
        body = "\n".join(checks[:n])
        return f"class NodeRegistry:\n    def _pool_error(self):\n{_indent(body, 8)}"

    assert unlisted(find_branches(method(1), path)) == []
    grown = find_branches(method(2), path)
    assert len(grown) == 2 and unlisted(grown) == grown
    elsewhere = find_branches(method(1), NEW)
    assert unlisted(elsewhere) == elsewhere


def test_the_drivers_may_branch():
    found = find_branches("if kind == PoolKind.CXL:\n    pass", FREE[0] + "new.py")
    assert found and unlisted(found) == []


def test_only_the_kind_table_of_storage_types_may_list_kinds():
    path = TABLE[0]
    assert find_branches("POOL_KINDS = {PoolKind.CXL: 1}", path) == []
    found = find_branches(
        'OTHER = {PoolKind.CXL: 1}\ndef f(value):\n    if value == "dram":\n        pass',
        path,
    )
    assert len(found) == 2 and unlisted(found) == found


def _indent(source: str, by: int = 4) -> str:
    return "\n".join(" " * by + line for line in source.splitlines())
