"""Pytest configuration: make the consumer's main module importable."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
