# Contributing to EbbingFlow

Thank you for your interest in contributing.

## 1. Scope

This project accepts:

- bug fixes
- tests and verification improvements
- documentation improvements
- memory/retrieval/persona pipeline enhancements

## 2. Development Setup

```powershell
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements-dev.txt
Copy-Item .env.example .env
```

Required services for full validation:

- Neo4j 5.x
- an OpenAI-compatible LLM endpoint

## 3. Before You Open a PR

Run the release closure check:

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe scripts\release_closure_check.py --full
```

At minimum, your change should not regress:

- functional offline checks
- functional real chain checks
- quality metrics (`mismerge_rate`, `miss_recall_rate`)

## 4. Pull Request Guidelines

- keep PRs focused and small
- include a clear problem statement and solution summary
- include test evidence (commands + key output)
- avoid unrelated refactors in the same PR
- keep backward compatibility when possible

## 5. Coding Guidelines

- prefer ASCII in source files unless non-ASCII is necessary
- keep comments short and intentional
- do not commit secrets (`.env`, API keys, private data)
- do not remove evidence-chain behavior without replacement

## 6. Commit Message Suggestions

Conventional style is recommended:

- `feat: ...`
- `fix: ...`
- `refactor: ...`
- `test: ...`
- `docs: ...`
- `chore: ...`

## 7. Security

If you find a security issue, please do not post exploit details publicly.
Open a private disclosure channel with maintainers first.
