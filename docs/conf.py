import os
import sys
from unittest.mock import MagicMock

import sphinxcontrib.mermaid as _mm

sys.path.insert(0, os.path.abspath(".."))


# Mock maru_shm (C extension) so autodoc can import maru_handler/maru_common.
# MagicMock supports __or__ for `X | None` type hints in dataclasses.
class _MockModule(MagicMock):
    class MaruHandle:
        """Mock MaruHandle for type hint evaluation."""

        def __class_getitem__(cls, item):
            return cls

        def __or__(self, other):
            return self

        def __ror__(self, other):
            return self


_mock = _MockModule()
sys.modules["maru_shm"] = _mock
sys.modules["maru_shm.types"] = _mock


project = "Maru"
copyright = "© 2026 XCENA Inc. | All Rights Reserved"
author = "XCENA"

extensions = [
    "myst_parser",
    "sphinxcontrib.mermaid",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

# Force mermaid to always render with light theme (bypass dark mode detection)
_mm._MERMAID_JS = _mm._MERMAID_JS.replace("isDarkTheme()", "false")
# Also inject matchMedia override so mermaid internals always see light scheme
_mm._MERMAID_JS = _mm._MERMAID_JS.replace(
    'import mermaid from "{{ mermaid_js_url }}";',
    'import mermaid from "{{ mermaid_js_url }}";\n'
    "const _origMatchMedia = window.matchMedia;\n"
    "window.matchMedia = (q) => q.includes('prefers-color-scheme')\n"
    "    ? { matches: false, media: q, addEventListener(){}, removeEventListener(){} }\n"
    "    : _origMatchMedia(q);\n",
)

html_theme = "shibuya"
html_title = "Maru"
html_theme_options = {
    "github_url": "https://github.com/xcena-dev/maru",
}

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}
exclude_patterns = ["_build"]
myst_enable_extensions = ["colon_fence", "deflist"]
myst_fence_as_directive = ["mermaid"]

html_static_path = ["_static"]
html_css_files = ["custom.css"]

toc_object_entries_show_parents = "hide"

# -- autodoc -----------------------------------------------------------------
autodoc_mock_imports = ["numpy"]
autodoc_member_order = "bysource"
autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "show-inheritance": True,
}
