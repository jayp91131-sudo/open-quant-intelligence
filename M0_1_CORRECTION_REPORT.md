# Open Quant Intelligence V2 — M0.1 Correction Report

**Date:** 2026-11-13  
**Milestone:** M0.1 Corrective Patch  
**Branch:** `fix/m0-1-contract-validation`  
**Base Branch:** `main`  
**Technical Authority:** ChatGPT  
**Author:** Antigravity Senior Python & QA Engineer  
**Pull Request URL:** https://github.com/jayp91131-sudo/open-quant-intelligence/pull/1  
**Commit Hash:** `07e8d70` (`07e8d7031121d51a6dbdbad85e33d077c9c045b1`)  
**Status:** Pull Request Opened — Awaiting Technical Lead Review (Not Merged)  

---

## 1. Root Cause of Corrected Defects

### Defect 1: Timeframe Case Normalization Bypassing Monthly Check
- **Location:** `src/open_quant_intelligence/data/validation.py`
- **Root Cause:** In `validate_candle_stream`, the raw timeframe string was lowercased (`tf = tf_raw.strip().lower()`) prior to calling `parse_timeframe(expected_timeframe)`. In exchange conventions, uppercase `'M'` denotes monthly intervals, which are intentionally unsupported in M0. The premature lowercasing converted `'1M'` into `'1m'` (one minute), bypassing the explicit monthly rejection check in `parse_timeframe()`.
- **Correction:** The raw timeframe string is passed directly into `parse_timeframe(tf_raw)` without pre-lowercasing. If the timeframe contains uppercase `'M'`, `parse_timeframe` raises `InvalidTimeframeError`, which is captured as a structured `UNSUPPORTED_TIMEFRAME` validation issue. Accepted timeframes (such as `'1m'` and `'1H'`) are stored in their canonical `SupportedTimeframe.value` form (e.g. `'1m'`, `'1h'`).
- **Regression Tests Added:**
  - `test_timeframe_1m_accepted_1M_rejected_in_csv`
  - `test_timeframe_1H_normalizes_to_1h_in_csv`
  - `test_parse_timeframe_direct_preservation`

### Defect 2: Market Symbol Validation Bypass in CSV Ingestion & Domain
- **Location:** `src/open_quant_intelligence/data/validation.py` and `src/open_quant_intelligence/domain/candle.py`
- **Root Cause:** 
  1. In CSV ingestion, row validation only checked that `symbol` was a non-empty string, but did not enforce the `MarketIdentity` contract rules (requiring valid pair separators like `/`, `-`, `_` and non-empty base/quote assets). Malformed symbols like `BTCUSDT`, `BTC/`, or `/USDT` were able to pass CSV validation.
  2. In `Candle.__post_init__`, symbol validation also checked only string presence, allowing invalid symbols to be constructed in-memory.
- **Correction:**
  1. `MarketIdentity` validation is integrated directly into the `Candle.__post_init__` lifecycle, ensuring that no `Candle` domain model can be instantiated with a malformed market identity.
  2. `validate_candle_stream` validates row `MarketIdentity(exchange_id=exch_raw, market_type=mtype_raw, symbol=sym_raw)` before candle creation, recording structured `INVALID_MARKET_IDENTITY` validation issues instead of throwing uncaught exceptions.
  3. Valid symbols like `BTC/USDT`, `ETH-USDT`, and `SOL_USDC` pass smoothly.
- **Regression Tests Added:**
  - `test_csv_validation_rejects_malformed_symbol_btcusdt`
  - `test_csv_validation_rejects_malformed_symbol_missing_parts`
  - `test_csv_validation_accepts_valid_symbols`
  - `test_candle_domain_model_enforces_market_identity`
  - `test_in_memory_validate_candles_with_valid_and_invalid_symbols`

---

## 2. Documentation Synchronization

The following project documents were audited and updated to synchronize test counts and cloud CI references:
- **`README.md`**: Updated test count to 104; added verified link to GitHub Actions cloud workflow run [43405143643](https://github.com/jayp91131-sudo/open-quant-intelligence/actions/runs/43405143643); removed outdated statements regarding pending cloud runs.
- **`M0_FINAL_AUDIT_REPORT.md`**: Updated Section 10 to reflect the verified passing GitHub Actions cloud CI runs and historical test evolution (80 -> 96 -> 104 tests).
- **`M0_REMEDIATION_REPORT.md`**: Synchronized GitHub Actions CI status with cloud execution.
- **`docs/ARCHITECTURE.md`**: Documented strict symbol pair separator requirements and case-sensitive timeframe rules in the canonical CSV specification.

---

## 3. Files Changed

| File Path | Description of Changes |
|---|---|
| `src/open_quant_intelligence/data/validation.py` | Integrated `MarketIdentity` contract checking; removed pre-lowercasing of timeframe string to preserve monthly `'M'` check. |
| `src/open_quant_intelligence/domain/candle.py` | Enforced `MarketIdentity` validation in `Candle.__post_init__`. |
| `src/open_quant_intelligence/domain/market.py` | Formatted exception message under Ruff line length limits. |
| `tests/unit/test_m0_1_corrections.py` | Added 8 dedicated regression tests covering timeframe case sensitivity and market symbol contracts. |
| `tests/unit/test_remediation.py` | Formatted multi-line string under Ruff formatting guidelines. |
| `README.md` | Synchronized test count (104) and added GitHub Actions cloud CI reference. |
| `M0_FINAL_AUDIT_REPORT.md` | Synchronized GitHub Actions cloud execution reference. |
| `M0_REMEDIATION_REPORT.md` | Synchronized GitHub Actions cloud execution reference. |
| `docs/ARCHITECTURE.md` | Synchronized symbol format and timeframe case rules. |
| `M0_1_CORRECTION_REPORT.md` | Created this comprehensive M0.1 corrective patch report. |

---

## 4. Test & Verification Results

### Runtimes Verified:
- **Python 3.11.15** (CPython, Windows x86_64)
- **Python 3.12.13** (CPython, Windows x86_64)

### Verification Commands & Results:
1. **Editable Installation:**
   - Command: `python -m pip install -e ".[dev]"`
   - Exit Code: `0` (Both 3.11 and 3.12)
2. **Ruff Linter:**
   - Command: `python -m ruff check .`
   - Exit Code: `0` (`All checks passed!`)
3. **Ruff Formatting Check:**
   - Command: `python -m ruff format --check .`
   - Exit Code: `0` (`23 files already formatted`)
4. **Bytecode Compilation:**
   - Command: `python -m compileall -q src tests`
   - Exit Code: `0` (Clean compilation on both runtimes)
5. **Pytest Suite:**
   - Command: `python -m pytest -v`
   - Results: **104 passed** in 0.17s (3.11) and 0.19s (3.12)
   - Breakdown:
     - `tests/unit/test_candle.py`: 20 passed
     - `tests/unit/test_cli.py`: 10 passed
     - `tests/unit/test_m0_1_corrections.py`: 8 passed (new M0.1 tests)
     - `tests/unit/test_market.py`: 12 passed
     - `tests/unit/test_remediation.py`: 16 passed
     - `tests/unit/test_timeframe.py`: 16 passed
     - `tests/unit/test_validation.py`: 22 passed
6. **CLI Entrypoints & All 11 Scenarios:**
   - Both `open-quant validate <path>` and `open-quant-validate <path>` verified.
   - All 11 integration test scenarios passed with exact exit codes.

---

## 5. Scope Boundary Compliance

- **V1.1 Protection:** The `crypto-trading-bot` repository and kickoff archive remain 100% untouched.
- **No Milestone Leakage:** Milestone M1 (CCXT, Binance REST API, DuckDB, Parquet) was NOT started.
- **No Live Trading / Execution:** No credentials, trading strategies, or order execution added.
- **No Unsolicited License:** No license file was added, preserving repository owner governance.

---

## 6. Remaining Limitations of Milestone M0 / M0.1

- Public data ingestion adapters (CCXT Binance public data) are scheduled for Milestone M1.
- Columnar persistence (DuckDB catalog and Parquet storage) is scheduled for Milestone M1.
- Only spot markets are supported; derivatives and margin are out of scope.
