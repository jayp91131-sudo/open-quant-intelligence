# Open Quant Intelligence — Architecture Specification (V2 M0)

## Overview

Open Quant Intelligence is a research-first quantitative research platform for cryptocurrency markets.
Milestone M0 establishes the immutable domain models, strict data contracts, and local verification tools.

Live trading, API credentials, order execution, leverage, and AI/ML are explicitly excluded from M0.

## Core System Boundaries

```text
[ Market Data Source (CSV / Synthetic) ]
                   │
                   ▼
       [ OHLCV Data Validator ]
       (Strict Canonical Schema)
       - Strict UTC timezone normalization
       - Finite positive prices
       - Finite non-negative volume
       - High/low bounds checking
       - Chronological order & duplicate rejection
       - Deterministic gap detection (no forward-filling)
                   │
                   ▼
       [ Immutable Domain Models ]
       - Candle (@dataclass(frozen=True))
       - MarketIdentity (Strict 'spot' type)
       - SupportedTimeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)
                   │
                   ▼
     [ Downstream Research Pipelines ]
        (M1+ Ingestion / M2+ Strategy / M3+ Simulation)
```

## Canonical Data Contract

### CSV Column Specification

| Column | Type | Validation Invariants |
| :--- | :--- | :--- |
| `exchange_id` | String | Non-empty string (e.g. `binance`). Case-insensitive normalized to lowercase. |
| `market_type` | String | Strictly `'spot'`. Derivatives, futures, and margin are rejected in M0. |
| `symbol` | String | Canonical market symbol (e.g. `BTC/USDT`, `ETH-USDT`). Must contain a valid pair separator (`/`, `-`, `_`) and valid assets matching `MarketIdentity`. Single symbol per file. |
| `timeframe` | String | Must be a supported interval in `{'1m', '5m', '15m', '30m', '1h', '4h', '1d'}`. Case-sensitive (`'1m'` supported; `'1M'` monthly rejected). Single timeframe per file. |
| `open_time` | ISO-8601 | Timezone-aware timestamp. Naive timestamps are strictly rejected. Normalized to UTC. |
| `open` | Float | Finite, strictly positive (`open > 0`). |
| `high` | Float | Finite, strictly positive (`high >= max(open, close, low)`). |
| `low` | Float | Finite, strictly positive (`low <= min(open, close, high)`). |
| `close` | Float | Finite, strictly positive (`close > 0`). |
| `base_volume` | Float | Finite, non-negative (`base_volume >= 0`). |

### Strict Gap & Ordering Policy

1. **No Silent Auto-Sorting**: Input rows must already be in strictly increasing chronological order. If an out-of-order row is encountered, validation fails and pinpoints the offending row index and timestamps.
2. **No Silent Gap-Filling**: If an expected interval is missing, the gap is recorded with its expected open timestamp, actual open timestamp, and missing candle count. Gaps are never forward-filled or interpolated.
3. **No Duplicate Timestamps**: Duplicate `(exchange_id, market_type, symbol, timeframe, open_time)` keys are rejected.

## Candle Time Semantics & Boundary Policy

Across Open Quant Intelligence V2, candle intervals follow the standard mathematical **half-open interval convention**:

$$\text{Candle Interval} = [T_{\text{open}}, T_{\text{close}})$$

- **`open_time` ($T_{\text{open}}$)**: The **inclusive start instant** of the interval. All market activity occurring at or after $T_{\text{open}}$ belongs to this candle.
- **`close_time` ($T_{\text{close}}$)**: The **exclusive upper boundary instant**, calculated deterministically as:
  $$T_{\text{close}} = T_{\text{open}} + \Delta_{\text{timeframe}}$$
- **Concrete Example (1-Hour Candle at 10:00 UTC)**:
  - $T_{\text{open}} = \text{2026-01-01 10:00:00.000000 UTC}$ (inclusive)
  - $T_{\text{close}} = \text{2026-01-01 11:00:00.000000 UTC}$ (exclusive boundary)
  - **Market Coverage**: Captures every trade/tick with timestamp $t$ satisfying:
    $$\text{10:00:00.000Z} \le t < \text{11:00:00.000Z}$$
  - **Boundary Handoff**: A trade occurring at exactly $\text{11:00:00.000Z}$ belongs strictly to the next candle (opened at $\text{11:00:00.000Z}$).
  - **Completion Criterion**: The candle is finalized and eligible for strategy decision-making only when observation time $\tau \ge T_{\text{close}}$ ($\text{11:00:00.000Z}$).
- **Invariance Across System**:
  - In a continuous, gapless series: $\text{Candle}[i].\text{close\_time} == \text{Candle}[i+1].\text{open\_time}$.
  - Exchange adapters (such as Binance Kline REST endpoints returning inclusive milliseconds like `10:59:59.999Z`) must normalize this timestamp to the canonical exclusive boundary $T_{\text{open}} + \Delta$ upon ingestion in M1.

## Technology Stack

- **Python**: 3.11 and 3.12 support.
- **Packaging**: `pyproject.toml`, `src/` layout, editable install via `pip install -e ".[dev]"`.
- **Linter & Code Style**: Ruff.
- **Test Framework**: Pytest.
- **Zero Heavy Runtime Dependencies**: Core M0 data validation operates strictly using Python's robust standard library.
