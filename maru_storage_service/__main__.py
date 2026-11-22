# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""``python -m maru_storage_service`` runs the same entry as ``maru-storage``."""

import sys

from .cli import main

sys.exit(main())
