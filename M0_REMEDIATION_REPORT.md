# Open Quant Intelligence V2 — M0 Remediation Report

**Date:** 2026-11-13  
**Milestone:** M0 (Project Bootstrap & Domain Contracts)  
**Author:** Antigravity Senior QA & Core Engineer  
**Status:** M0 Remediation Complete — Ready for Technical Lead Review  

---

## 1. Executive Summary

Following the independent technical review (`M0_INDEPENDENT_REVIEW.md`), six verified defect findings (M0-01 through M0-06) and recommended contract hardening points have been fully resolved. All fixes include dedicated regression tests reproducing the failure and confirming the resolution. 

The test suite expanded from 80 to **96 tests**, with 100% passing across clean Python 3.11 and Python 3.12 environments. All 11 CLI integration scenarios pass with expected exit codes, Ruff passes with zero errors, and bytecode compiles cleanly.

---

## 2. Remediated Defects & Invariant Hardening

### Defect M0-01: Explicit candle `close_time` can contradict its timeframe
- **Issue:** An explicitly passed `close_time` was only checked to be greater than `open_time`, allowing arbitrary close times (e.g., `close_time=10:30` on a `1h` candle opened at `10:00`).
- **Remediation:** In `Candle.__post_init__`, compute `expected_close_time = compute_close_time(utc_open_time, parsed_tf)`. If `self.close_time` is supplied, it must strictly match `expected_close_time`. Contradictory values raise `InvalidCandleError`.
- **Regression Tests:** `test_m0_01_reproduce_contradictory_close_time_rejected` and `test_m0_01_exact_matching_close_time_accepted`.

### Defect M0-02: In-memory validator crashes when first item is not a Candle
- **Issue:** `validate_candles` dereferenced `candles[0].exchange_id` before verifying if `candles[0]` was an instance of `Candle`, raising unhandled `AttributeError` on `validate_candles([object()])`.
- **Remediation:** `validate_candles` inspects the sequence to locate the first valid `Candle` to establish series identity. If no elements are valid `Candle` instances, it records `INVALID_OBJECT_TYPE` issues for all items and returns a clean `ValidationResult(is_valid=False)` without crashing.
- **Regression Tests:** `test_m0_02_reproduce_first_element_not_a_candle` and `test_m0_02_mixed_invalid_elements_sequence`.

### Defect M0-03: Short / malformed CSV rows cause unhandled exception
- **Issue:** `csv.DictReader` assigns `None` to missing trailing fields. Calling `.lower()` on missing identity fields raised `AttributeError: 'NoneType' object has no attribute 'lower'`.
- **Remediation:** In `validate_candle_stream`, rows are checked for truncated columns (`None` values for required columns) and recorded as `MALFORMED_ROW`. All identity fields (`exchange_id`, `market_type`, `symbol`, `timeframe`) are verified to be non-empty strings before normalization. Missing values are recorded as `MISSING_IDENTITY_FIELD`.
- **Regression Tests:** `test_m0_03_reproduce_short_csv_row_no_attribute_error` and `test_m0_03_missing_identity_fields_controlled_error`.

### Defect M0-04: Extra CSV data fields silently discarded
- **Issue:** `csv.DictReader` collects unheaded row values under key `None`. The row dictionary comprehension previously dropped falsey keys, silently discarding extra unheaded data.
- **Remediation:** `validate_candle_stream` checks if key `None` is present in `raw_row` and contains values. If so, a structured `EXTRA_FIELDS` issue is appended with row number and unheaded values.
- **Regression Tests:** `test_m0_04_reproduce_extra_unheaded_csv_columns`.

### Defect M0-05: Non-interval-aligned candle timestamps accepted and misleading gap reporting
- **Issue:** In both `validate_candles` and `validate_candle_stream`, gap computation used `round(missing_seconds / interval_seconds)` without checking if `(actual_open - previous_open)` was an exact integer multiple of the timeframe interval. As a result, off-grid timestamps (e.g. 3h10m gap on a 1h series) were accepted in audit mode and reported as 2 missing candles.
- **Remediation:** Evaluates `remainder = diff_seconds % step_seconds`. If remainder is non-zero, appends a structured `MISALIGNED_INTERVAL` error and records zero gap intervals. Missing candle count is only computed when the delta is an exact grid multiple.
- **Regression Tests:** `test_m0_05_reproduce_misaligned_gap_calculation`, `test_m0_05_in_memory_misaligned_interval`, and `test_m0_05_exact_grid_gap_count`.

### Defect M0-06: README test count discrepancy
- **Issue:** README stated 75 tests while 80 tests were in the suite, creating documentation inconsistency.
- **Remediation:** Updated README and architecture documentation to accurately report 96 tests (80 original + 16 new regression/hardening tests).

---

## 3. Additional Contract Hardening

1. **`Candle.is_closed` Type Enforcement**: Explicitly enforces `isinstance(self.is_closed, bool)`. Strings such as `'false'` or numbers `0`/`1` raise `InvalidCandleError`.
2. **`Candle.trade_count` Validation**: Explicitly verifies `self.trade_count` is a non-negative `int` and not a `bool` (since `isinstance(True, int)` is True). Negative values and boolean flags raise `InvalidCandleError`.
3. **Boolean Price / Volume Rejection**: Checks `isinstance(price_val, bool)` and `isinstance(volume, bool)` prior to numeric conversion, rejecting `True`/`False` masquerading as prices or volumes.
4. **`MarketIdentity` Symbol Integrity**: Validates symbol contains a valid delimiter (`/`, `-`, `_`) with non-empty base and quote components. Rejects malformed symbols such as `BTCUSDT`, `BTC/`, or `/USDT`. Validates that explicitly passed `base_currency` and `quote_currency` agree with the symbol's constituent assets.
5. **Empty Dataset Policy**: Both in-memory sequence validator (`validate_candles([])`) and CSV stream validator (header-only CSV) explicitly return `ValidationResult(is_valid=False)` with structured issue `EMPTY_DATASET`.
6. **Gitignore Path Anchoring**: Anchored `/data/` to prevent repository directories like `src/.../data/` from being inadvertently excluded.

---

## 4. Verification Results

### A. Unit Tests (96 Tests)
```
collected 96 items
tests/unit/test_candle.py .................... [ 20%] (20 passed)
tests/unit/test_cli.py ..........              [ 31%] (10 passed)
tests/unit/test_market.py ............         [ 43%] (12 passed)
tests/unit/test_remediation.py ................ [ 60%] (16 passed)
tests/unit/test_timeframe.py ................  [ 77%] (16 passed)
tests/unit/test_validation.py ...................... [100%] (22 passed)

============================= 96 passed in 0.15s ==============================
```

### B. Python 3.11 & 3.12 Environments
- **Python 3.11.15**: Editable install successful; `compileall` exit code 0; `ruff check .` clean; 96 tests passed.
- **Python 3.12.13**: Editable install successful; `compileall` exit code 0; `ruff check .` clean; 96 tests passed.

### C. 11 CLI Integration Scenarios
| # | Scenario | Command / Input | Exit Code | Result |
|---|---|---|:---:|---|
| 1 | Valid CSV | `open-quant validate examples/sample_btc_1h.csv` | `0` | PASSED (5 valid candles, 0 errors, 0 gaps) |
| 2 | CSV with gaps (audit mode) | `open-quant validate examples/sample_btc_1h_with_gaps.csv` | `0` | PASSED (3 candles, 1 gap reported: 2 missing) |
| 3 | CSV with gaps (strict mode) | `open-quant validate --fail-on-gaps ...` | `1` | FAILED (Gaps rejected in strict mode) |
| 4 | Invalid prices | Price `< 0` | `1` | FAILED (`CANDLE_CONTRACT_VIOLATION`) |
| 5 | Naive timestamps | Timestamp without UTC timezone offset | `1` | FAILED (`TIMEZONE_NAIVE`) |
| 6 | Duplicate timestamps | Identical consecutive `open_time` | `1` | FAILED (`DUPLICATE_TIMESTAMP`) |
| 7 | Out-of-order timestamps | Decreasing timestamps | `1` | FAILED (`OUT_OF_ORDER_TIMESTAMP`) |
| 8 | Missing columns | Incomplete CSV header | `1` | FAILED (`MISSING_COLUMNS`) |
| 9 | Empty CSV | 0 bytes input | `1` | FAILED (`EMPTY_FILE`) |
| 10 | Nonexistent file | File path does not exist on disk | `1` | FAILED (`FILE_NOT_FOUND`) |
| 11 | Unheaded extra columns | 11 columns for 10 headers | `1` | FAILED (`EXTRA_FIELDS`) |

### D. Cloud CI Workflow
- Workflow matrix in `.github/workflows/ci.yml`:
  - Ubuntu / Python 3.11
  - Ubuntu / Python 3.12
  - Windows / Python 3.11
  - Windows / Python 3.12
- Local multi-OS emulation passed. Actual cloud runner execution is pending remote repository publication to GitHub.

---

## 5. Scope Boundary Compliance

- **V1.1 Protection:** The `crypto-trading-bot` directory and kickoff packages were untouched.
- **No Milestone Leakage:** Milestone M1 (CCXT, DuckDB, Parquet) was NOT started.
- **No Live Trading / Execution:** No exchange credentials, order execution, trading strategies, or AI/ML components were added.
