"""Pytest bootstrap for local-source imports.

Ensures tests can import project packages (e.g. ``memory``) when running
``pytest`` directly from the repository root without extra env vars.
"""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
root_str = str(ROOT)
if root_str not in sys.path:
    sys.path.insert(0, root_str)

