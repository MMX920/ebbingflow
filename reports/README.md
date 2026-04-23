# Reports Guide

Generated verification reports for open-source release checks.

Key files:

1. `release_closure_report.md`: human-readable summary
2. `release_closure_report.json`: machine-readable summary
3. `m2_quality_metrics.json`: quality metrics (`mismerge_rate`, `miss_recall_rate`)

Generate/update reports with:

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe scripts\release_closure_check.py --full
```

