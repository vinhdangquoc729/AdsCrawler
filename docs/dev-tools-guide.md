# Developer Tools Guide: Ruff, pre-commit, and pytest

This guide explains the code-quality tools used in this project and how to run them locally.

---

## Table of Contents

1. [Ruff — Python linter & formatter](#1-ruff--python-linter--formatter)
2. [pre-commit — automatic checks before every commit](#2-pre-commit--automatic-checks-before-every-commit)
3. [pytest — running the test suite](#3-pytest--running-the-test-suite)
4. [Typical daily workflow](#4-typical-daily-workflow)

---

## 1. Ruff — Python linter & formatter

### What it is

Ruff is a fast Python linter written in Rust. It replaces tools like `flake8` and `isort`, running roughly 100× faster. It catches style issues, unused imports, undefined variables, and other common problems.

### How this project configures it

Configuration lives in [ruff.toml](../ruff.toml):

```toml
[lint]
select = ["E", "F", "W"]      # pycodestyle errors, pyflakes, pycodestyle warnings
ignore = ["E501", "E701", "E741"]  # long lines, multiple statements, ambiguous names

exclude = [
    "airflow/logs",
    "airflow/plugins",
    ".ivy2",
    "sample_data",
]
```

### How to run

```bash
# Install
pip install ruff

# Check all files — shows every issue with file + line number
ruff check .

# Auto-fix everything that can be fixed automatically
ruff check . --fix

# Check a single file
ruff check ingest/facebook/mock.py

# Format code (like black)
ruff format .
```

### Reading the output

```
ingest/facebook/mock.py:12:1: F401 `os` imported but unused
```

- `F401` — rule code (F = pyflakes)
- The message tells you exactly what is wrong
- Run `ruff check . --fix` and most issues disappear automatically

---

## 2. pre-commit — automatic checks before every commit

### What it is

pre-commit is a framework that runs checks automatically every time you run `git commit`. If any check fails, the commit is blocked and you see the error. This prevents broken or poorly formatted code from ever entering the repository.

### How this project configures it

Configuration lives in [.pre-commit-config.yaml](../.pre-commit-config.yaml):

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.4.4
    hooks:
      - id: ruff
        args: [--fix]
```

Every `git commit` runs Ruff with `--fix`. If Ruff changes any files, the commit is blocked so you can review and re-stage the fixed files.

### Setup

```bash
pip install pre-commit
pre-commit install
```

That's it. The hook is now wired into `.git/hooks/pre-commit`.

### How it behaves during a commit

```
$ git commit -m "feat: add new campaign field"

ruff....................................................................Failed
- hook id: ruff
- files were modified by this hook

ingest/facebook/mock.py   ← ruff auto-fixed this file
```

When this happens:

```bash
git add ingest/facebook/mock.py   # stage the fixed file
git commit -m "feat: add new campaign field"   # commit again — will pass now
```

### Running pre-commit manually

```bash
# Run all hooks on every file in the repo
pre-commit run --all-files

# Run only the ruff hook
pre-commit run ruff --all-files

# Run only on staged files (what the next commit would check)
pre-commit run
```

---

## 3. pytest — running the test suite

### What it is

pytest is Python's standard testing framework. It discovers test files automatically, runs them, and gives clear output showing what passed and what failed.

### How to run

```bash
# Install
pip install pytest

# Run all tests
pytest tests/

# Run with verbose output — shows each test name
pytest tests/ -v

# Run a single file
pytest tests/test_mock_generator.py -v

# Run a single test function
pytest tests/test_mock_generator.py::test_deterministic_id_co_prefix -v

# Stop after the first failure
pytest tests/ -x

# Show print() output (normally suppressed)
pytest tests/ -s
```

### Reading the output

```
tests/test_mock_generator.py::test_deterministic_id_co_prefix PASSED
tests/test_mock_generator.py::test_bias_phu_nu PASSED
tests/test_mock_generator.py::test_sum_metrics_cong_dung FAILED

FAILURES
========
test_sum_metrics_cong_dung
  AssertionError: assert 100.0 == 150.0
```

- `PASSED` — test ran and the assertion was true
- `FAILED` — an assertion was false; pytest shows the exact values
- `ERROR` — the test itself crashed (import error, missing fixture, etc.)

### How to write a new test

Every test is a plain function whose name starts with `test_`. No class required.

```python
# tests/test_my_module.py

from my_module import calculate_cpc

def test_calculate_cpc_basic():
    result = calculate_cpc(budget=1_000_000, clicks=500)
    assert result == 2000.0

def test_calculate_cpc_zero_clicks_raises():
    import pytest
    with pytest.raises(ZeroDivisionError):
        calculate_cpc(budget=1_000_000, clicks=0)
```

**Naming rules:**
- File: `test_<module_name>.py`
- Function: `test_<what_you_are_checking>`

Keep each test focused on one behaviour. If a test name is hard to write, the test is probably testing too much.

---

## 4. Typical daily workflow

```bash
# 1. Start from a clean, up-to-date branch
git checkout main
git pull origin main
git checkout -b feature/your-name-or-feature

# 2. Write code, then check locally before committing
ruff check . --fix          # fix lint issues
pytest tests/ -v            # make sure tests pass

# 3. Commit — pre-commit runs Ruff automatically
git add <changed files>
git commit -m "feat: short description of what you did"
# If pre-commit modified files, git add them again and re-commit

# 4. Push and open a Pull Request
git push origin feature/your-name-or-feature
# Go to GitHub → New Pull Request → wait for CI (green = good, red = read the logs)
```

### Quick reference

| Task | Command |
|---|---|
| Check lint issues | `ruff check .` |
| Auto-fix lint issues | `ruff check . --fix` |
| Run all tests | `pytest tests/ -v` |
| Run pre-commit manually | `pre-commit run --all-files` |
| Install pre-commit hook | `pre-commit install` |
