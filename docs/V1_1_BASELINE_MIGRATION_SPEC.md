# OQI V2 — V1.1 Baseline Migration Specification

**Milestone:** M2 — Strategy Interface & Baseline Migration  
**Status:** Draft Specification (Pending Independent Review & Project Owner Approval)  
**Target Module (Deferred Pending Approval):** `src/open_quant_intelligence/strategy/baseline.py`  
**Target Parity Tests (Deferred Pending Approval):** `tests/unit/strategy/test_baseline_parity.py`  
**M2 Framework Reference:** `docs/M2_ARCHITECTURE_PROPOSAL.md` (merged to `main` via PR #6, `328351e`; generic framework merged via PR #7, `258f9c254c291a42f3e6e7363a11e4709f33309e`)

> [!IMPORTANT]
> This document is a **design and migration specification only**. No baseline strategy code has been implemented, and no M3 simulation or backtest execution logic is included. Implementation of `baseline.py` and `test_baseline_parity.py` remains blocked until the Project Owner explicitly approves this specification and resolves the open decisions in Section 11.

> [!NOTE]
> **Classification Legend:** Throughout this document, every requirement is explicitly tagged as:
> - **[Source-Verified]:** Directly established from the inspected V1.1 baseline specification (`binance_trading_bot_v1.1_audited.zip`).
> - **[Proposed V2 Mapping]:** Proposed adaptation to OQI V2's merged M2 generic strategy contracts.
> - **[Unresolved Decision]:** Behavior not fully determined by V1.1 source that requires explicit Project Owner approval before implementation.
> - **[Future M3 Work]:** Execution, exit, sizing, fee, or portfolio behavior explicitly excluded from M2 and deferred to M3.

---

## 1. Purpose and Scope

### 1.1 Purpose
This specification defines the exact mathematical, behavioral, and contract-integration requirements to migrate the legacy Binance Trading Bot V1.1 scanner strategy (`evaluate_symbol`) into the OQI V2 M2 strategy framework (`src/open_quant_intelligence/strategy/`).

### 1.2 In-Scope (M2)
- Exact mathematical reproduction of V1.1 EMA20, EMA50, Pandas EWM RSI(14), 20-candle prior average volume, relative volume, and `breakout_lookback` prior-high calculations.
- Exact preservation of V1.1 entry conditions, strict vs. inclusive comparison operators, warmup floor (`max(60, breakout_lookback + 22)`), and heuristic ranking score.
- Mapping of V1.1 scanner candidate signals into M2 `ResearchSignal` (`LONG` / `HOLD`), `StrategyConfig`, `StrategyCandle`, `StrategyContext`, and `StrategyOutput`.
- Deterministic golden parity test plan with hard-coded expected values.

### 1.3 Out-of-Scope
- V1.1 backtest execution logic (3% stop-loss, 6% take-profit, 1% cash risk sizing, 0.10% fees, 0.10% slippage, trend exits) — **[Future M3 Work]**.
- V1.1 universe selection (Binance Spot USDT pair filtering, quote-volume ranking, top-30 selection) — belongs outside per-symbol `Strategy.evaluate()`.
- V1.1 blind final-candle dropping — explicitly replaced by M0 `Candle.is_closed` and half-open interval semantics `[open_time, close_time)`.
- Live trading, exchange order placement, CCXT execution, or API credentials.
- Any claims of strategy profitability or statistical validity.

---

## 2. Source Provenance and Exact V1.1 Behavior

### 2.1 Provenance Status
- **[Source-Verified]:** The V1.1 source archive (`binance_trading_bot_v1.1_audited.zip`) was independently inspected to establish the authoritative V1.1 scanner behavior for `evaluate_symbol(symbol, candles, volume_multiple=2.0, breakout_lookback=20)`.
- **Local Workspace Note:** `binance_trading_bot_v1.1_audited.zip` is not present in the local repository workspace; this specification uses the independently inspected V1.1 behavior as the authoritative source reference and explicitly flags any edge cases (Section 11) where direct source verification or an owner decision is required prior to coding.

### 2.2 Original V1.1 Signature and Inputs **[Source-Verified]**
- Function: `evaluate_symbol(symbol, candles, volume_multiple=2.0, breakout_lookback=20)`
- Evaluates one symbol at a time.
- Required input columns: `open`, `high`, `low`, `close`, `volume`.
- Default parameters:
  - `volume_multiple = 2.0` (must be finite and `> 0`)
  - `breakout_lookback = 20`

---

## 3. Indicator Formulas and Initialization Details **[Source-Verified]**

All formulas below operate strictly on the historical prefix ending at the current completed candle $T$ (`history[0..T]`). No Pandas dependency is required in V2 runtime code; pure Python/math implementations must produce identical floating-point behavior to the Pandas definitions below.

### 3.1 EMA20 and EMA50
Original Pandas definitions:
- `ema20 = close.ewm(span=20, adjust=False).mean()`
- `ema50 = close.ewm(span=50, adjust=False).mean()`

Exact recurrence (`adjust=False`):
- Smoothing factor: $\alpha_N = \frac{2}{N + 1}$
  - For $N = 20$: $\alpha_{20} = \frac{2}{21}$
  - For $N = 50$: $\alpha_{50} = \frac{2}{51}$
- Seed at index $0$: $\text{EMA}_0 = \text{close}_0$
- Update for $t \ge 1$: $\text{EMA}_t = \alpha_N \cdot \text{close}_t + (1 - \alpha_N) \cdot \text{EMA}_{t-1}$

> [!IMPORTANT]
> Do **not** substitute an SMA seed over the first $N$ candles. `adjust=False` in Pandas seeds $\text{EMA}_0 = \text{close}_0$.

### 3.2 RSI (14-Period Pandas EWM, `adjust=False`)
Original Pandas definitions:
- `delta = close.diff()` (undefined/`NaN` at index $0$)
- `gain = delta.clip(lower=0)`
- `loss = -delta.clip(upper=0)`
- `avg_gain = gain.ewm(alpha=1/14, adjust=False, min_periods=14).mean()`
- `avg_loss = loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()`
- `RS = avg_gain / avg_loss.where(avg_loss != 0)`
- `RSI = 100 - 100 / (1 + RS)`

Exact recurrence and timing:
- At index $0$: `delta_0` is `NaN`; `gain_0` and `loss_0` are `NaN` (excluded from EWM initialization).
- First valid `delta`, `gain`, `loss` occur at index $1$ (`close_1 - close_0`).
- EWM with $\alpha = \frac{1}{14}$ and `adjust=False` seeds at index $1$:
  - $\text{avg\_gain}_1 = \text{gain}_1$, $\text{avg\_loss}_1 = \text{loss}_1$
  - For $t \ge 2$:
    $$\text{avg\_gain}_t = \frac{1}{14}\text{gain}_t + \frac{13}{14}\text{avg\_gain}_{t-1}$$
    $$\text{avg\_loss}_t = \frac{1}{14}\text{loss}_t + \frac{13}{14}\text{avg\_loss}_{t-1}$$
- `min_periods=14`: since valid `delta` values begin at index $1$, the 14th valid `delta` occurs at **index $14$** (the 15th candle, 1-indexed). Indices $0..13$ have `NaN` for `avg_gain` and `avg_loss`.
- Special cases when `avg_gain` and `avg_loss` are valid (`t >= 14`):
  - `avg_loss == 0` and `avg_gain > 0` $\implies \text{RSI} = 100.0$
  - `avg_gain == 0` and `avg_loss > 0` $\implies \text{RSI} = 0.0$
  - `avg_gain == 0` and `avg_loss == 0` $\implies \text{RSI} = 50.0$

> [!IMPORTANT]
> This is **not** classic Wilder RSI seeded with a 14-period SMA of the first 14 changes. It is Pandas `ewm(alpha=1/14, adjust=False, min_periods=14)` seeded on `gain_1` / `loss_1`. Preserve this behavior exactly.

### 3.3 Volume and Relative Volume
Original Pandas definitions (mapping V1.1 `volume` to M2 `StrategyCandle.base_volume`):
- `avg_volume_20 = volume.shift(1).rolling(20, min_periods=20).mean()`
  - At candle index $t \ge 20$, averages strictly the preceding 20 candles: `volume[t-20 : t]` (excludes `volume[t]`).
- `relative_volume = current_volume / avg_volume_20`
- Invalid/zero denominator rule:
  - If `avg_volume_20` is unavailable (`t < 20`) or `avg_volume_20 == 0.0`, `relative_volume` is **invalid / `NaN`**.
  - Do **not** silently convert invalid `relative_volume` to `0.0` or `inf`.

### 3.4 Prior-High Breakout
Original Pandas definition:
- `prior_high = high.shift(1).rolling(breakout_lookback, min_periods=breakout_lookback).max()`
  - At candle index $t \ge \text{breakout\_lookback}$, takes $\max(\text{high}[t-\text{breakout\_lookback} : t])$, strictly **excluding** `high[t]`.

---

## 4. Entry Condition Definitions and Threshold Boundaries **[Source-Verified]**

All four conditions must evaluate to `True` on the latest completed candle $T$:

| # | Condition | Exact Expression | Boundary Behavior |
|---|---|---|---|
| 1 | **Trend** | `close > ema50 and ema20 > ema50` | Strictly `>` on both comparisons; equality (`==`) fails. |
| 2 | **Momentum** | `50 <= RSI <= 75` | Inclusive on both bounds (`RSI == 50.0` and `RSI == 75.0` pass; `< 50` or `> 75` fail). |
| 3 | **Volume** | `relative_volume >= volume_multiple` | Inclusive (`relative_volume == volume_multiple` passes; invalid/`NaN` `relative_volume` fails). |
| 4 | **Breakout** | `close > prior_high` | Strictly `>` (`close == prior_high` does **not** qualify). |

Default parameters:
- `volume_multiple = 2.0` (validated: finite `float`, `> 0`)
- `breakout_lookback = 20` (validated: `int`, `>= 1`)

---

## 5. Warm-Up Behavior **[Source-Verified]**

- Minimum history requirement:
  $$\text{minimum\_history\_candles} = \max(60, \text{breakout\_lookback} + 22)$$
- For default `breakout_lookback = 20`:
  $$\max(60, 20 + 22) = 60 \text{ candles}$$
- At 59 candles: insufficient history; V1.1 returns no signal. In M2, `StrategyEvaluator` Stage B raises `InsufficientHistoryError` before invoking `Strategy.evaluate()` when `len(prefix_history) < strategy.minimum_history_candles`.
- At 60 candles: eligible for evaluation, subject to valid indicators (e.g., finite non-zero `avg_volume_20`).

---

## 6. Score Semantics **[Source-Verified]**

Original heuristic score formula:
$$\text{raw\_score} = \min(30, \max(0, (\text{relative\_volume} - 1) \times 10)) + \min(25, \max(0, \text{RSI} - 50)) + (25 \text{ if trend else } 0) + (20 \text{ if breakout else } 0)$$
$$\text{score} = \min(100.0, \text{raw\_score})$$

Original reason string when entry conditions pass:
- `"EMA trend + RSI momentum + relative-volume surge + close breakout"`

> [!WARNING]
> **Non-Probabilistic Metadata Only:** This score is an ad-hoc heuristic ranking value used in V1.1 to sort candidates. It is **not** a calibrated probability, confidence interval, expected return, or evidence of profitability. In M2, it must be stored strictly in `StrategyOutput.metadata` (when finite) and never interpreted as position sizing or trade confidence.

---

## 7. V1.1-to-M2 Signal Mapping **[Proposed V2 Mapping]**

| V1.1 Outcome | Proposed M2 `ResearchSignal` | Proposed M2 `ReasonCode` / `reason_code` | Rationale |
|---|---|---|---|
| All 4 entry conditions pass | `ResearchSignal.LONG` | `"EMA trend + RSI momentum + relative-volume surge + close breakout"` (or `ReasonCode.LONG_SIGNAL_GENERATED` — see Section 11) | V1.1 scanner emits a long candidate entry signal. |
| One or more entry conditions fail (including invalid/`NaN` `relative_volume`) | `ResearchSignal.HOLD` | `ReasonCode.NO_CONDITION_MET` (`"NO_CONDITION_MET"`) | In M2, `FLAT` means target zero exposure (exit), whereas `HOLD` means no state transition. V1.1 scanner has no exit logic. |

- `ResearchSignal.FLAT` is **never** emitted by this migrated baseline strategy.
- **No Profitability Claim:** Emitting `LONG` represents a deterministic rule match on historical market data, not a claim of positive expected return.

---

## 8. Causality and Candle Closure Requirements **[Source-Verified + Proposed V2 Mapping]**

1. **No Blind Final-Candle Drop:** V1.1 blindly dropped its last fetched candle. OQI V2 **rejects** that pattern. M2 relies on M0 `Candle.is_closed == True` and half-open intervals `[open_time, close_time)`. Stage B rejects any unclosed prefix candle with `IncompleteCandleError` before `Strategy.evaluate()` is called.
2. **Point-in-Time Causality:** For target candle $T$, `Strategy.evaluate()` receives only `history = StrategyCandle` tuple for `0..T`.
3. **Decision Timestamp:** `StrategyOutput.decision_time` must strictly equal `history[-1].close_time`.
4. **Exclusion of Current Candle from Lookbacks:** Both `avg_volume_20` and `prior_high` use `.shift(1)` semantics (indices `< T`), preventing intra-bar lookahead or self-comparison bias.

---

## 9. Golden Parity Test Matrix **[Proposed V2 Mapping]**

All expected values in `tests/unit/strategy/test_baseline_parity.py` **must be hard-coded** (computed independently and verified against Pandas reference values, never generated dynamically by the strategy under test).

### 9.1 Indicator Parity Cases
| Test ID | Case | Verification Target |
|---|---|---|
| `GP-IND-01` | EMA20 & EMA50 parity | Matches `close.ewm(span=20, adjust=False).mean()` and `span=50` at seed ($t=0$) and $t=59$ within `rel_tol=1e-9, abs_tol=1e-12`. |
| `GP-IND-02` | Pandas EWM RSI parity | Matches `gain/loss.ewm(alpha=1/14, adjust=False, min_periods=14).mean()` (distinct from SMA-seeded Wilder RSI). |
| `GP-IND-03` | RSI all-gain case | Strictly increasing closes $\implies \text{avg\_loss}=0, \text{avg\_gain}>0 \implies \text{RSI} == 100.0$. |
| `GP-IND-04` | RSI all-loss case | Strictly decreasing closes $\implies \text{avg\_gain}=0, \text{avg\_loss}>0 \implies \text{RSI} == 0.0$. |
| `GP-IND-05` | RSI flat-price case | Constant closes $\implies \text{avg\_gain}=0, \text{avg\_loss}=0 \implies \text{RSI} == 50.0$. |
| `GP-IND-06` | RSI first-valid timing | Undefined before index $14$; first valid at index $14$ (15th candle). |
| `GP-IND-07` | Prior 20-candle avg volume | Excludes `volume[T]`; equals mean of `volume[T-20:T]`. |
| `GP-IND-08` | Prior-high breakout window | Excludes `high[T]`; equals max of `high[T-breakout_lookback:T]`. |
| `GP-IND-09` | Zero/unavailable `avg_volume_20` | `avg_volume_20 == 0` yields invalid `relative_volume` (no division-by-zero crash, no `0.0`/`inf` substitution), entry fails $\implies$ `HOLD`. |

### 9.2 Signal Boundary Cases
| Test ID | Case | Expected Signal |
|---|---|---|
| `GP-BND-01` | `RSI == 50.0` (other conditions true) | `LONG` (inclusive lower bound) |
| `GP-BND-02` | `RSI == 75.0` (other conditions true) | `LONG` (inclusive upper bound) |
| `GP-BND-03` | `RSI < 50.0` (e.g., `49.999999`) | `HOLD` |
| `GP-BND-04` | `RSI > 75.0` (e.g., `75.000001`) | `HOLD` |
| `GP-BND-05` | `relative_volume == 2.0` (`volume_multiple=2.0`) | `LONG` (inclusive bound) |
| `GP-BND-06` | `relative_volume < 2.0` (e.g., `1.999999`) | `HOLD` |
| `GP-BND-07` | `close == prior_high` | `HOLD` (strict `>` required for breakout) |
| `GP-BND-08` | `close > prior_high` | `LONG` |
| `GP-BND-09` | `close == ema50` or `ema20 == ema50` | `HOLD` (strict `>` required for trend) |
| `GP-BND-10` | 59 candles (`breakout_lookback=20`) | `InsufficientHistoryError` raised by Stage B |
| `GP-BND-11` | 60 candles (`breakout_lookback=20`) | Evaluates cleanly (emits `LONG` or `HOLD` based on indicators) |

### 9.3 Causality, Integrity, and Provenance Cases
| Test ID | Case | Verification Target |
|---|---|---|
| `GP-CAU-01` | Future candle invariance | Appending or mutating candles strictly after $T$ leaves decision at $T$ 100% identical. |
| `GP-CAU-02` | Prefix-only evaluation | `StrategyEvaluator.evaluate_at()` passes only `0..T` `StrategyCandle` views. |
| `GP-CAU-03` | Invalid/unclosed input rejection | Unclosed prefix candle $\implies$ `IncompleteCandleError`; tampered OHLCV vs catalog $\implies$ `SnapshotConsistencyError`. |
| `GP-CAU-04` | Parameter isolation | Changing `volume_multiple` affects only volume condition & score; changing `breakout_lookback` updates warmup `max(60, breakout_lookback + 22)` and `prior_high` window. |
| `GP-CAU-05` | `StrategyOutput` & `StrategyDecision` contract parity | `decision_time == source_candle_close_time`, catalog `snapshot_id` & `dataset_content_fingerprint` attached. |

---

## 10. M2 Contract Integration Notes (Verified Against `main`)

Verified directly against `src/open_quant_intelligence/strategy/`:

1. **`StrategyConfig` (`contracts.py`):**
   - Baseline config (e.g., `V11BaselineConfig(StrategyConfig)`) must be a `@dataclass(frozen=True)` containing only primitive scalars (`volume_multiple: float = 2.0`, `breakout_lookback: int = 20`).
   - Validation in `__post_init__` must reject `bool`, `NaN`, `inf`, `volume_multiple <= 0`, and `breakout_lookback < 1`.
   - `compute_fingerprint()` automatically produces the canonical `sha256:...` config digest.

2. **`Strategy` Protocol (`contracts.py`):**
   - Baseline class must expose properties: `strategy_id -> str`, `version -> str`, `config -> StrategyConfig`, `minimum_history_candles -> int` (`max(60, config.breakout_lookback + 22)`), `gap_policy -> GapPolicy`, and `evaluate(history: Sequence[StrategyCandle], context: StrategyContext) -> StrategyOutput`.

3. **`StrategyCandle` (`contracts.py`):**
   - Exposes `open_time`, `close_time`, `open`, `high`, `low`, `close`, `base_volume`, `quote_volume`, `trade_count`.
   - V1.1 `volume` maps strictly to `StrategyCandle.base_volume`.
   - `source`, `fetched_at`, `exchange_id`, and `market_type` are absent.

4. **`StrategyContext` (`contracts.py`):**
   - Exposes `symbol: str`, `timeframe: SupportedTimeframe`, `prefix_quality_report: DataQualityReport`.
   - Note: While V1.1 was designed around `1h` candles, `StrategyContext.timeframe` is preserved on `StrategyOutput.timeframe` without assuming `1h` unless configured/restricted.

5. **`StrategyOutput` & `ValidatedStrategyOutput` (`signals.py`):**
   - `StrategyOutput.metadata` enforces `_validate_metadata()`, which accepts **only** `int | float | str | bool` with **finite** floats (`NaN`, `inf`, and `None` raise `ValueError` or `TypeError`).
   - **Critical Constraint:** When `avg_volume_20 == 0` (invalid/`NaN` `relative_volume`), `relative_volume` **must not** be written as `float("nan")` or `None` into `StrategyOutput.metadata`. Instead, omit `relative_volume` (or record `relative_volume_valid: False`) and ensure `score` only includes valid finite terms (or is omitted/`0.0` per owner approval).
   - `StrategyEvaluator` validates `strategy_id`, `strategy_version`, `config_fingerprint`, `symbol`, `timeframe`, `source_candle_open_time`, `source_candle_close_time`, and `decision_time` before issuing `ValidatedStrategyOutput` and assembling `StrategyDecision`.

---

## 11. Known Limitations and Unresolved Decisions

The following decisions are **not specified** by the V1.1 source and **require explicit Project Owner approval** before implementing `baseline.py`:

1. **[Unresolved Decision 1 — `GapPolicy` Selection]:**
   - M2 `GapPolicy` provides two options:
     - `GapPolicy.STRICT_GAP_FREE`: Rejects evaluation if *any* gap (including leading gaps) exists in `[requested_since_ms, T.close_time)`.
     - `GapPolicy.REJECT_INTERNAL_GAPS`: Permits leading gaps before the first available candle, but rejects any internal gap between candles in the evaluated prefix.
   - *Owner Decision Required:* Should the V1.1 baseline declare `STRICT_GAP_FREE` or `REJECT_INTERNAL_GAPS`? (Note: because EMA20, EMA50, RSI14, `avg_volume_20`, and `prior_high` assume contiguous bars, `STRICT_GAP_FREE` or `REJECT_INTERNAL_GAPS` both protect indicator continuity; `REJECT_INTERNAL_GAPS` allows evaluating a continuous series that starts after `requested_since`, whereas `STRICT_GAP_FREE` rejects any prefix with leading gaps.)

2. **[Unresolved Decision 2 — `reason_code` Exact String]:**
   - V1.1 emits `"EMA trend + RSI momentum + relative-volume surge + close breakout"` on entry.
   - M2 `signals.py` defines `ReasonCode.LONG_SIGNAL_GENERATED = "LONG_SIGNAL_GENERATED"` and `ReasonCode.NO_CONDITION_MET = "NO_CONDITION_MET"`.
   - *Owner Decision Required:* On `LONG`, should `reason_code` use the literal V1.1 string `"EMA trend + RSI momentum + relative-volume surge + close breakout"` (preserving exact text parity) or `ReasonCode.LONG_SIGNAL_GENERATED` (with the V1.1 text in `metadata`)?

3. **[Unresolved Decision 3 — Metadata & Score Representation When `avg_volume_20 == 0`]:**
   - When `avg_volume_20 == 0.0`, `relative_volume` is invalid/`NaN`. Because `StrategyOutput.metadata` forbids `NaN` and `None`, confirm the exact metadata convention: omit `relative_volume` from `metadata`, set `relative_volume_valid=False`, and treat the volume score component as `0.0` (so `score` remains finite) or omit `score`?

4. **[Unresolved Decision 4 — Timeframe Restriction]:**
   - V1.1 ran on `1h` candles. Should `baseline.py` accept any `SupportedTimeframe` supplied by `StrategyContext.timeframe` or explicitly restrict/warn on non-`1h` timeframes?

5. **Known M1 Limitation (Out of M2 Scope):**
   - M1's `DuckDBCatalog` stores `requested_since`/`requested_until` as timezone-naive `TIMESTAMP` in DuckDB, creating a cross-timezone portability limitation if a `.duckdb` file is moved across OS timezones. Tracked as a separate future M1 schema task; not modified in M2.

---

## 12. Explicit M3 Exclusions **[Future M3 Work]**

The following V1.1 backtest and scanner behaviors are **strictly excluded** from M2 (`baseline.py`) and reserved for M3+:
- 3% fixed stop-loss and 6% fixed take-profit rules.
- Trend-based exits or `FLAT` signal generation.
- Next-bar entry execution timing.
- 1% cash-risk position sizing and portfolio/account state tracking.
- 0.10% fee and 0.10% slippage assumptions.
- Trade accounting, equity curves, and performance metrics.
- Spot USDT quote-volume universe filtering and top-30 symbol ranking.

---

## 13. Acceptance Criteria for M2 Baseline Completion

M2 will be eligible for final acceptance only when all of the following are satisfied:
1. Project Owner approves this specification and resolves Decisions 1–4 in Section 11.
2. `src/open_quant_intelligence/strategy/baseline.py` is implemented strictly adhering to `Strategy` and `StrategyConfig` without modifying M0, M1, or M2 generic contracts.
3. `tests/unit/strategy/test_baseline_parity.py` implements all hard-coded golden parity, boundary, and causality cases in Section 9 (including `GP-IND-01..09`, `GP-BND-01..11`, `GP-CAU-01..05`).
4. Full `pytest` suite, Ruff lint/format, and `compileall` pass on Python 3.11 and 3.12 across Ubuntu and Windows CI.
5. Independent review confirms zero future-data leakage, zero M3 leakage, and exact parity with V1.1 indicator math.

---

## 14. Owner Approval Checklist

Before authorizing implementation of `src/open_quant_intelligence/strategy/baseline.py`, please confirm:

- [ ] **Indicator & Signal Parity Approved:** EMA20/EMA50 (`adjust=False`), Pandas EWM RSI(14) (`min_periods=14`, special cases `100`/`0`/`50`), `shift(1)` volume/breakout exclusions, warmup `max(60, breakout_lookback + 22)`, and `LONG` / `HOLD` mapping (no `FLAT`).
- [ ] **Decision 1 (`GapPolicy`):** Choose `GapPolicy.STRICT_GAP_FREE` or `GapPolicy.REJECT_INTERNAL_GAPS`.
- [ ] **Decision 2 (`reason_code`):** Choose literal V1.1 reason string vs. `ReasonCode.LONG_SIGNAL_GENERATED`.
- [ ] **Decision 3 (Zero-Volume Metadata):** Approve omitting `relative_volume` + `relative_volume_valid=False` + finite `score` (volume term = `0.0`) when `avg_volume_20 == 0`.
- [ ] **Decision 4 (Timeframe Scope):** Confirm whether `baseline.py` accepts any `SupportedTimeframe` or restricts to `1h`.
- [ ] **M3 Exclusions Confirmed:** Stop/target/sizing/fees/slippage/universe selection remain deferred to M3.
