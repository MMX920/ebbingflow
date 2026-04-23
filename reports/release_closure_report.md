# Release Closure Report

- Generated at: `2026-04-23T16:15:28.792618Z`
- Mode: `offline`

## Closure Status

| Item | Status |
|---|---|
| Functional Offline | PASS |
| Functional Real | SKIPPED (run with --full) |
| Quality Gates (regression stability) | PASS |
| Usability (README one command) | PASS |
| Reproducibility (fixed seed + fixed dataset) | PASS |

**Overall:** PASS

## Step Results

### Functional Offline (Core Regression)
- Command: `C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe -m pytest -q tests/test_profile_field_contract.py tests/test_temporal_goldens.py tests/test_ws_auth.py tests/test_chat_engine_role_label.py`
- Exit: `0`
- Status: `PASS`
- Stdout tail:
```text
...............                                                          [100%]
============================== warnings summary ===============================
..\..\..\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\config\__init__.py:1434
  C:\Users\Administrator\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\config\__init__.py:1434: PytestConfigWarning: Unknown config option: asyncio_mode
  
    self._warn_or_fail_if_strict(f"Unknown config option: {key}\n")

..\..\..\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\cacheprovider.py:475
  C:\Users\Administrator\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\cacheprovider.py:475: PytestCacheWarning: could not create cache path C:\Users\Administrator\Documents\Myprojects\ebbingflow\.pytest_cache\v\cache\nodeids: [WinError 5] 拒绝访问。: 'C:\\Users\\Administrator\\Documents\\Myprojects\\ebbingflow\\pytest-cache-files-94hfqnqe'
    config.cache.set("cache/nodeids", sorted(self.cached_nodeids))

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
15 passed, 2 warnings in 9.61s
```

### Quality Gates (Regression Stability)
- Command: `C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe -m pytest -q tests/test_conflict_arbitration.py tests/test_confidence_tuning.py tests/test_unit_currency_normalization.py`
- Exit: `0`
- Status: `PASS`
- Stdout tail:
```text
.....                                                                    [100%]
============================== warnings summary ===============================
..\..\..\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\config\__init__.py:1434
  C:\Users\Administrator\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\config\__init__.py:1434: PytestConfigWarning: Unknown config option: asyncio_mode
  
    self._warn_or_fail_if_strict(f"Unknown config option: {key}\n")

..\..\..\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\cacheprovider.py:475
  C:\Users\Administrator\AppData\Local\Programs\Python\Python312\Lib\site-packages\_pytest\cacheprovider.py:475: PytestCacheWarning: could not create cache path C:\Users\Administrator\Documents\Myprojects\ebbingflow\.pytest_cache\v\cache\nodeids: [WinError 5] 拒绝访问。: 'C:\\Users\\Administrator\\Documents\\Myprojects\\ebbingflow\\pytest-cache-files-gfawjx0h'
    config.cache.set("cache/nodeids", sorted(self.cached_nodeids))

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html
5 passed, 2 warnings in 1.42s
```
