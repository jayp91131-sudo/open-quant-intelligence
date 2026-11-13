# Open Quant Intelligence (V2)

**Milestone M0: Project Bootstrap & Domain Contracts**

Open Quant Intelligence is a research-first quantitative cryptocurrency research platform designed for reproducible strategy evaluation, time-series integrity, and transparent data quality auditing.

> **CRITICAL DISCLAIMER**: This repository is research software in its foundational phase. It is **NOT** a live trading bot, does not place orders, does not contain exchange API credentials, does not execute trades, and does not provide financial or investment advice. Live order execution is explicitly out of scope.

## Milestone M0 Scope

Milestone M0 implements the core architectural domain contracts and validation engine:

- Clean Python package using standard `src/` layout.
- Immutable domain models: `Candle`, `MarketIdentity`, and `SupportedTimeframe`.
- Strict canonical OHLCV validation:
  - Finite strictly positive prices (`open`, `high`, `low`, `close` > 0).
  - Finite non-negative volume (`base_volume` >= 0).
  - High/low bounding invariants.
  - Strict timezone-aware timestamp enforcement (normalized to UTC; naive timestamps rejected).
  - Strict chronological order enforcement (no silent auto-sorting).
  - Explicit missing-interval gap detection (no forward-filling or data fabrication).
  - Single-symbol and single-timeframe series enforcement (mixed series rejected).
  - Only `spot` market type permitted in M0.
- Command-line interface (`open-quant validate`) for auditing local CSV datasets.
- 100% synthetic unit test suite without network dependencies or credentials.
- CI workflow targeting Python 3.11 and 3.12 across Windows and Linux.

## Time Semantics Contract

Candle intervals strictly use the **half-open interval convention** `[open_time, close_time)`:
- `open_time` is the **inclusive start instant** (e.g. `10:00:00Z`).
- `close_time` is the **exclusive upper boundary instant** (`11:00:00Z` for `1h`).
- All trades where `10:00:00Z <= t < 11:00:00Z` belong to this candle. A trade at exactly `11:00:00Z` belongs strictly to the subsequent `11:00` candle.
- In adjacent continuous series, `candle[i].close_time == candle[i+1].open_time`.

## Installation

### Prerequisites
- Python 3.11 or 3.12

### Editable Install (Development Mode)
```bash
python -m pip install -e ".[dev]"
```

## Running Verification Commands

```bash
# 1. Run unit tests
pytest -q

# 2. Run Ruff linter
ruff check .

# 3. Verify Python bytecode compilation
python -m compileall -q src tests

# 4. Run the CSV validator CLI
open-quant validate path/to/candles.csv
# or
python -m open_quant_intelligence.cli validate path/to/candles.csv
```

## Canonical CSV Specification

Required columns:
```csv
exchange_id,market_type,symbol,timeframe,open_time,open,high,low,close,base_volume
```

Example valid row:
```csv
binance,spot,BTC/USDT,1h,2026-01-01T00:00:00Z,50000.0,50500.0,49800.0,50200.0,123.45
```

## Roadmap

- **M0**: Project bootstrap and domain contracts (**Completed**)
- **M1**: Public data adapter and ingestion (CCXT Binance public data, Parquet storage, DuckDB catalog)
- **M2**: Strategy interface and baseline migration
- **M3**: Research simulation engine
- **M4**: Performance metrics and benchmarking
- **M5**: Robust validation (walk-forward, holdouts)
- **M6**: Market regimes and optional ML
- **M7**: Research dashboard
- **M8**: Live execution (Requires separate future engineering, security, and governance approvals)
