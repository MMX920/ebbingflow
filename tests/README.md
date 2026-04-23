# Tests Guide

This directory contains pytest-style regression tests only: `test_*.py`.

Recommended release check:

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe -m pip install -r requirements-dev.txt
pytest -q
```
