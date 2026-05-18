"""Pytest config — adds source dirs to sys.path so tests can import them directly."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for sub in ("ingestion",):
    sys.path.insert(0, str(ROOT / sub))
