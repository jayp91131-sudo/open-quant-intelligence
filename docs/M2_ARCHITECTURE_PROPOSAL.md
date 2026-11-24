# Open Quant Intelligence V2 — Milestone M2 Architecture Proposal
## Strategy Interface & Baseline Migration (Draft Candidate — Awaiting Technical Review)

**Document Reference:** `docs/M2_ARCHITECTURE_PROPOSAL.md`  
**Milestone:** M2 — Strategy Interface & Baseline Migration  
**Base Commit on `main`:** `0781ce2`  
**Branch:** `docs/m2-strategy-interface-proposal`  
**Author:** AntiGravity (Implementation Engineer)  
**Technical Authority & Reviewer:** ChatGPT  
**Final Decision Maker:** Project Owner  
**Status:** DRAFT CANDIDATE — AWAITING TECHNICAL REVIEW & OWNER APPROVAL (Implementation is strictly NOT authorized)  

---

## Table of Contents

1. [Executive Summary & Objectives](#1-executive-summary--objectives)
2. [Current Architecture & M0/M1 Compatibility](#2-current-architecture--m0m1-compatibility)
3. [Strategy Interface & Data Contracts](#3-strategy-interface--data-contracts)
4. [Input Validation & Data-Quality Policy](#4-input-validation--data-quality-policy)
5. [Signal Semantics & Timing Rules](#5-signal-semantics--timing-rules)
6. [Baseline Migration Plan & Unresolved Dependency (BLOCKER)](#6-baseline-migration-plan--unresolved-dependency-blocker)
7. [Proposed Package Structure](#7-proposed-package-structure)
8. [Determinism & Reproducibility](#8-determinism--reproducibility)
9. [Testing & Acceptance Criteria](#9-testing--acceptance-criteria)
10. [Strict Milestone Exclusions](#10-strict-milestone-exclusions)
11. [Risks, Failure Modes, and Open Decisions](#11-risks-failure-modes-and-open-decisions)
12. [M2 Completion Criteria](#12-m2-completion-criteria)

---

## 1. Executive Summary & Objectives

Milestone M2 transitions Open Quant Intelligence V2 from a robust, crash-resilient data ingestion and storage foundation (established in M0 and M1) into the quantitative research domain. The objective of M2 is to define a minimal, strictly typed, deterministic strategy interface protocol and establish a verifiable baseline migration framework.

In quantitative finance, strategies often fail in production or backtesting due to hidden state mutation, lookahead leakage, loose timestamp conventions, unhandled missing data intervals, or implicit coupling to portfolio simulation engines. M2 eliminates these vulnerabilities by enforcing a **pure functional paradigm**:

$$\text{StrategyDecision} = f(\text{Immutable Strategy Config}, \text{Validated Historical Candles})$$

### Core Objectives of M2:

1. **Explicit, Minimal Strategy Protocol:** Define a formal `Strategy` interface contract with an immutable configuration identity, semantic versioning, declared minimum lookback history, and explicit gap handling policy.
2. **Deterministic Point-in-Time Evaluation:** Ensure evaluation functions as a pure mathematical transformation of finalized historical candles $[T_{\text{open}}, T_{\text{close}})$. Evaluation is strictly isolated from side effects: zero file I/O, zero network calls, zero database queries, zero system clock access, and zero pseudo-randomness.
3. **Structured Research Intentions (Signals):** Standardize research signal vocabulary into three unambiguous states: `LONG`, `FLAT`, and `HOLD`. These express directional research intention, completely detached from exchange order execution, position sizing, margin, or leverage.
4. **Rigorous Data Quality Re-use:** Enforce that strategies evaluate only fully closed candles (`is_closed=True`) validated against M0 schema invariants and M1 gap detection (`DataQualityReport`). Data is never silently sorted, interpolated, or forward-filled.
5. **Baseline Migration with Parity Verification:** Provide a formal golden-test migration methodology for the project's baseline strategy while explicitly documenting the current absence of legacy strategy code as an **unresolved owner-input dependency blocker**.
6. **Strict Milestone Boundaries:** Confine M2 strictly to decision evaluation. All execution loops, fill assumptions, slippage, commission modeling, portfolio state tracking, and performance metrics are quarantined to Milestone M3.

---

## 2. Current Architecture & M0/M1 Compatibility

Milestone M2 operates strictly as a consumer of M0 domain contracts and M1 dataset artifacts. It introduces zero alterations to existing M0/M1 responsibilities.

```text
┌────────────────────────────────────────────────────────────────────────┐
│ M0 — Domain Models & Local Verification                                │
│   - Candle (Immutable dataclass, frozen=True)                         │
│   - MarketIdentity (Strict 'spot' market_type)                         │
│   - SupportedTimeframe (1m, 5m, 15m, 30m, 1h, 4h, 1d)                 │
│   - Half-Open Intervals: [T_open, T_close) where T_close = T_open + Δ  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ consumes
┌───────────────────────────────────▼────────────────────────────────────┐
│ M1 — Ingestion, Storage & Catalog Foundation                           │
│   - Public Binance Vision Ingestion (allowlisted Spot endpoints)       │
│   - Canonical Parquet Datasets (Snappy compression, immutable)         │
│   - DuckDB Snapshot Catalog (CatalogDatasetRecord, dataset_gaps)       │
│   - Content Fingerprints: sha256:[0-9a-f]{64}                          │
│   - Quality Analysis: DataQualityReport, QualityGapEvent               │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ consumes
┌───────────────────────────────────▼────────────────────────────────────┐
│ M2 — Strategy Interface & Evaluation (THIS PROPOSAL)                   │
│   - Strategy Protocol & StrategyConfig (Immutable, frozen)             │
│   - ResearchSignal: LONG, FLAT, HOLD (Intentions, not orders)          │
│   - StrategyDecision: Point-in-time signal at T_close with reason_code │
│   - StrategyEvaluator: Strict M0/M1 validation & lookback verification │
│   - Baseline Strategy Migration: Golden parity test suite              │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ produces decisions for
┌───────────────────────────────────▼────────────────────────────────────┐
│ M3 — Simulation & Backtesting Engine (FUTURE MILESTONE)                │
│   - Event-driven / vectorized simulation loops                         │
│   - Portfolio state, cash accounting, position tracking                │
│   - Order generation, fills (at T_close or next T_open), slippage, fee │
└────────────────────────────────────────────────────────────────────────┘
```

### Preserved System Invariants

1. **Immutable Domain Models:** `Candle`, `MarketIdentity`, and `SupportedTimeframe` remain frozen. Strategy evaluation accepts `Sequence[Candle]` and cannot alter any candle attribute.
2. **Half-Open Intervals $[T_{\text{open}}, T_{\text{close}})$:**
   - $T_{\text{open}}$ is the inclusive start boundary.
   - $T_{\text{close}} = T_{\text{open}} + \Delta_{\text{timeframe}}$ is the exclusive upper boundary.
   - A candle finalized at $T_{\text{close}}$ represents all trading activity during $T_{\text{open}} \le t < T_{\text{close}}$.
3. **Strict Chronological Ordering & Uniqueness:** Candle inputs must arrive in strictly ascending order ($T_{\text{open}, i} < T_{\text{open}, i+1}$) without duplicates. Out-of-order candles are rejected with structured errors; auto-sorting is prohibited.
4. **No Synthetic Data Generation:** Zero forward-filling, zero linear interpolation, and zero hallucinated candles. Missing data intervals are surfaced explicitly via M1 quality metadata.
5. **Spot-Only Market Support:** All evaluated candles must adhere to `market_type == "spot"`. Short selling, perpetual funding rates, and margin borrows are mathematically excluded from the domain.
6. **Platform & Runtime Compatibility:** Pure Python 3.11 and 3.12 compatibility with deterministic, identical results across Linux and Windows.

---

## 3. Strategy Interface & Data Contracts

### 3.1 Research Signal Vocabulary

In OQI V2, strategy outputs are **research intentions**, not execution orders. The signal vocabulary is intentionally restricted to three discrete states:

```python
from enum import Enum


class ResearchSignal(str, Enum):
    """Directional research signal emitted by a strategy for the latest completed candle.
    
    These express quantitative intent, NOT exchange orders:
    - LONG: Target state is exposure to the underlying base asset.
    - FLAT: Target state is zero exposure (100% quote currency / cash).
    - HOLD: Maintain existing stance without initiating a state transition.
    """
    LONG = "LONG"
    FLAT = "FLAT"
    HOLD = "HOLD"
```

*Architectural Boundary Note:* Signals do not specify order type (LIMIT/MARKET), price, quantity, leverage, or execution timing. Those concerns belong strictly to M3 simulation and order management.

---

### 3.2 Strategy Decision Contract

Every strategy evaluation produces a single, fully auditable, immutable `StrategyDecision` representing the quantitative conclusion for the latest closed candle:

```python
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    """Immutable record of a deterministic strategy evaluation result."""

    # Identity & Provenance
    strategy_id: str
    strategy_version: str
    config_fingerprint: str
    dataset_content_fingerprint: str

    # Market & Time Coordinates
    symbol: str
    timeframe: str
    source_candle_open_time: datetime
    source_candle_close_time: datetime
    decision_time: datetime

    # Decision Output
    signal: ResearchSignal
    reason_code: str
    metadata: Mapping[str, Any]

    def __post_init__(self) -> None:
        if self.decision_time != self.source_candle_close_time:
            raise ValueError(
                f"Causality violation: decision_time ({self.decision_time.isoformat()}) "
                f"must strictly equal source_candle_close_time ({self.source_candle_close_time.isoformat()})."
            )
        if self.source_candle_close_time <= self.source_candle_open_time:
            raise ValueError("source_candle_close_time must be strictly after source_candle_open_time.")
        if not self.strategy_id.strip():
            raise ValueError("strategy_id cannot be empty.")
        if not self.reason_code.strip():
            raise ValueError("reason_code cannot be empty.")
```

#### Fields Description:
- `strategy_id`: Unique identifier of the strategy (e.g. `baseline-momentum`).
- `strategy_version`: Semantic version string of the strategy logic (e.g. `1.0.0`).
- `config_fingerprint`: Canonical SHA-256 digest (`sha256:[0-9a-f]{64}`) of the strategy configuration.
- `dataset_content_fingerprint`: Canonical SHA-256 digest (`sha256:[0-9a-f]{64}`) of the evaluated candle series (matching M1).
- `source_candle_open_time`: Inclusive start timestamp of the candle that triggered the evaluation.
- `source_candle_close_time`: Exclusive close timestamp $T_{\text{close}}$ of the evaluated candle.
- `decision_time`: Identical to `source_candle_close_time`, anchoring the decision at the exact instant the candle closed.
- `signal`: `LONG`, `FLAT`, or `HOLD`.
- `reason_code`: Machine-readable token explaining the decision (e.g., `MOMENTUM_THRESHOLD_EXCEEDED`, `WARMUP_INSUFFICIENT`, `EXIT_CRITERIA_MET`).
- `metadata`: Read-only dictionary of computed indicators, thresholds, or intermediate metrics (e.g., `{"fast_ma": 42150.2, "slow_ma": 41980.5}`) enabling full post-hoc verification.

---

### 3.3 Strategy Configuration Contract

Strategy parameters must be encapsulated in an immutable, serializable configuration model capable of deterministic SHA-256 fingerprinting:

```python
import hashlib
import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class StrategyConfig:
    """Base class for immutable strategy configuration parameters."""

    def compute_fingerprint(self) -> str:
        """Compute canonical SHA-256 fingerprint of configuration attributes."""
        raw = asdict(self)
        payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"
```

---

### 3.4 Gap Policy Specification

Each strategy must explicitly state its operational policy when confronting missing intervals:

```python
class GapPolicy(str, Enum):
    """Explicit policy defining how a strategy handles historical data gaps.
    
    Gaps are never silently patched or forward-filled.
    """
    STRICT_REJECT = "STRICT_REJECT"
    """Reject evaluation if ANY gap exists in the provided candle history."""

    ABORT_ON_INTERNAL = "ABORT_ON_INTERNAL"
    """Reject evaluation if internal gaps exist, but allow leading/trailing window gaps outside lookback."""

    ALLOW_UNALIGNED_GAP_EXPLICIT = "ALLOW_UNALIGNED_GAP_EXPLICIT"
    """Proceed with evaluation only if the strategy's mathematical model explicitly supports
    irregular intervals, passing the M1 QualityGapEvent metadata to the strategy decision."""
```

---

### 3.5 The Strategy Protocol

The core strategy contract is defined as a Python `Protocol`:

```python
from typing import Protocol, Sequence, runtime_checkable
from open_quant_intelligence.domain.candle import Candle


@runtime_checkable
class Strategy(Protocol):
    """Strict, allowlisted strategy protocol for deterministic research evaluation."""

    @property
    def strategy_id(self) -> str:
        """Stable, unique machine-readable identifier for the strategy."""
        ...

    @property
    def version(self) -> str:
        """Semantic version string of the strategy implementation."""
        ...

    @property
    def config(self) -> StrategyConfig:
        """Immutable configuration parameters."""
        ...

    @property
    def minimum_history_candles(self) -> int:
        """Strictly positive integer defining the minimum completed candles required to evaluate."""
        ...

    @property
    def gap_policy(self) -> GapPolicy:
        """Declared gap tolerance policy."""
        ...

    def evaluate(
        self,
        history: Sequence[Candle],
        dataset_content_fingerprint: str = "",
    ) -> StrategyDecision:
        """Evaluate history ending at history[-1] and return a deterministic StrategyDecision.
        
        Invariants:
        1. Pure function: No I/O, no network, no database, no system clock, no state mutation.
        2. history is strictly ordered, non-empty, and satisfies minimum_history_candles.
        3. Decision corresponds strictly to the close boundary of history[-1].
        """
        ...
```

---

## 4. Input Validation & Data-Quality Policy

### 4.1 Re-use of M0 and M1 Foundations

M2 does NOT re-implement data validation or gap detection from scratch. Instead, it reuses existing, audited foundation components:

1. **M0 Validation (`validate_candle_sequence`):**
   - Validates that every candle in `history` has valid, finite, positive OHLC prices ($H \ge \max(O, C, L)$, $L \le \min(O, C, H)$) and non-negative volume.
   - Rejects unparseable or naive timestamps.
   - Enforces strict chronological progression: $T_{\text{open}, i} < T_{\text{open}, i+1}$.
   - Rejects duplicate timestamps.
2. **M1 Quality Analysis (`analyze_dataset_quality`):**
   - Audits candle grid alignment: $(T_{\text{open}} - T_{\text{since}}) \pmod{\Delta} == 0$.
   - Quantifies total expected intervals vs actual record count: $missing\_intervals = expected - count$.
   - Classifies gap events into `LEADING`, `INTERNAL`, and `TRAILING` with exact timestamps and missing counts via `QualityGapEvent`.

---

### 4.2 Evaluator Pre-Flight Verification Rules

The `StrategyEvaluator` sits between dataset storage and the `Strategy.evaluate()` method. Before calling `strategy.evaluate()`, the evaluator performs the following pre-flight checks:

```text
Input: Sequence[Candle]
   │
   ├─► Check 1: Non-Empty & Closed Status
   │     - len(history) > 0
   │     - All candles must have is_closed == True (unfinalized candles rejected)
   │
   ├─► Check 2: Market & Timeframe Homogeneity
   │     - Every candle must share identical exchange_id, market_type, symbol, and timeframe
   │
   ├─► Check 3: Minimum Lookback / History Check
   │     - len(history) >= strategy.minimum_history_candles
   │     - If insufficient: raise InsufficientHistoryError or emit structured warmup decision
   │
   ├─► Check 4: Strict Chronological Progression
   │     - history[i].open_time < history[i+1].open_time for all i
   │     - In a continuous series: history[i].close_time == history[i+1].open_time
   │
   ├─► Check 5: Gap Policy Enforcement
   │     - Evaluate history with analyze_dataset_quality()
   │     - If gaps detected: enforce strategy.gap_policy
   │     - Gaps are NEVER silently filled, interpolated, or averaged
   │
   ▼
Passes Pre-Flight -> Execute strategy.evaluate(history)
```

#### Structured Error Hierarchy:
```python
class StrategyEvaluationError(Exception):
    """Base exception for all strategy evaluation failures."""


class InsufficientHistoryError(StrategyEvaluationError):
    """Raised when available candle history is less than minimum_history_candles."""


class IncompleteCandleError(StrategyEvaluationError):
    """Raised when an unclosed candle (is_closed=False) is supplied."""


class HeterogeneousSeriesError(StrategyEvaluationError):
    """Raised when candles contain mixed symbols, timeframes, or markets."""


class ChronologicalOrderError(StrategyEvaluationError):
    """Raised when candles are out of order or contain duplicate timestamps."""


class StrategyDataGapError(StrategyEvaluationError):
    """Raised when input candles violate the strategy's declared GapPolicy."""
```

---

## 5. Signal Semantics & Timing Rules

### 5.1 Point-in-Time Causality & Boundary Alignment

A central flaw in quantitative strategy design is lookahead bias caused by timestamp ambiguity (e.g. using bar open time to execute trades that depend on bar close prices). M2 resolves this mathematically:

$$\text{Candle } C_i = [T_{\text{open}, i}, T_{\text{close}, i})$$

1. **Observation Time Horizon:** Information concerning candle $C_i$ (specifically its high, low, close, and volume) does not exist until $T_{\text{close}, i}$.
2. **Decision Point:** The earliest instant a strategy can evaluate candle $C_i$ is at $t = T_{\text{close}, i}$.
3. **Causality Guarantee:** 
   $$\text{Decision Timestamp} \equiv T_{\text{close}, i}$$
   No information, tick, or candle with timestamp $t \ge T_{\text{close}, i}$ may be accessed or considered during the evaluation of $C_i$.

```text
Timeline (1h Timeframe):
10:00 UTC                 11:00 UTC                 12:00 UTC
────┼─────────────────────────┼─────────────────────────┼────►
    │       Candle C_0        │       Candle C_1        │
    │  [10:00, 11:00 UTC)     │  [11:00, 12:00 UTC)     │
    │                         │                         │
    └────────────────────────►│                         │
         Market Activity      │                         │
                              ▼                         │
                      At exactly 11:00:00Z:             │
                      1. C_0 is finalized               │
                      2. Strategy evaluates C_0         │
                      3. Emits Decision(T_decision=11:00)
                              │                         │
                              └────────────────────────►│
                                   Market Activity      │
                                                        ▼
                                                At exactly 12:00:00Z:
                                                1. C_1 is finalized
                                                2. Strategy evaluates [C_0, C_1]
                                                3. Emits Decision(T_decision=12:00)
```

---

### 5.2 State Transitions & The `HOLD` Signal

`ResearchSignal` states represent quantitative intentions:

- **`LONG`**: Strategy determines that market conditions warrant long asset exposure.
- **`FLAT`**: Strategy determines that market conditions warrant exiting or remaining in 100% cash / quote currency.
- **`HOLD`**: Strategy determines that no transition trigger occurred; maintain whatever exposure stance previously existed.

```text
State Transition Diagram (Research Stance):

              ┌─────────────┐
              │    FLAT     │◄─────────────────┐
              │ (100% Cash) │                  │
              └──────┬──────┘                  │
                     │                         │
                Signal=LONG               Signal=FLAT
                     │                         │
                     ▼                         │
              ┌─────────────┐                  │
         ┌───►│    LONG     ├──────────────────┘
         │    │  (Exposed)  │
         │    └─────────────┘
    Signal=HOLD      ▲
    (No Change)      │ Signal=HOLD (No Change)
         └───────────┘
```

*Note on Strategy Statefulness:* Strategies in M2 are designed to be stateless or deterministically state-foldable. When evaluating a slice `history`, the decision for the final candle must depend only on the historical series provided, not on mutable in-memory variables preserved from prior calls.

---

### 5.3 Clear Separation from Milestone M3

M2 explicitly stops at decision generation. The following responsibilities belong to M3 and are prohibited in M2:

| Concern | Handled in M2? | Handled in M3? | Reason for Boundary |
|---|:---:|:---:|---|
| Directional research intent (`LONG`/`FLAT`) | **YES** | Consumes M2 | Strategy logic defines intention. |
| Reason code & indicator metadata | **YES** | Consumes M2 | Explains why strategy signaled. |
| Portfolio cash balance tracking | **NO** | **YES** | Strategies do not manage wallets. |
| Position sizing & leverage | **NO** | **YES** | Risk & capital allocation concern. |
| Execution timing (e.g. Next Bar Open fill) | **NO** | **YES** | Backtest simulation model concern. |
| Slippage, spread & fee calculation | **NO** | **YES** | Execution realism model concern. |
| PnL, Sharpe, Sortino, Drawdown | **NO** | **YES** | Portfolio performance analytics concern. |

---

## 6. Baseline Migration Plan & Unresolved Dependency (BLOCKER)

### 6.1 Unresolved Dependency Statement (CRITICAL BLOCKER)

> [!CAUTION]
> **IMPLEMENTATION BLOCKER — UNRESOLVED OWNER INPUT REQUIRED**  
> A comprehensive code and repository audit of `main` at commit `0781ce2` confirms that **no legacy strategy implementation exists** within the repository.
> 
> The implementation engineer is strictly prohibited from inventing, assuming, or guessing the baseline strategy algorithm (such as a generic SMA crossover, EMA MACD, RSI threshold, or Buy-and-Hold).
> 
> Until the Project Owner provides the definitive legacy baseline specification, implementation of the baseline strategy module cannot proceed.

---

### 6.2 Required Owner Input Deliverables

Before baseline migration code can be authored in M2, the Project Owner must supply or formally approve the following specification:

1. **Algorithm Definition & Source Logic:**
   - Exact mathematical formula or legacy Python/pseudocode source for all indicators and decision rules.
   - Exact tie-breaking logic (e.g., if price equals indicator threshold).
2. **Parameter Defaults & Configuration:**
   - Names, types, and default values for all strategy parameters.
   - Explicit minimum history / warm-up length (e.g., number of candles required before the first valid signal can be emitted).
3. **Signal Semantics & Warmup Output:**
   - Explicit specification of what the strategy emits during warm-up periods (e.g., `FLAT` with `reason_code="WARMUP"`, or `HOLD`).
   - Exact triggering conditions for transitioning between `FLAT`, `LONG`, and `HOLD`.
4. **Reference Inputs & Expected Outputs (Golden Master Dataset):**
   - A reference historical candle dataset (CSV or Parquet) covering at least 500 completed candles across known market regimes (trending, ranging, volatile).
   - The verified, authoritative expected sequence of `StrategyDecision` outputs (signal, decision timestamp, reason code, and indicator values) generated by the legacy system.

---

### 6.3 Golden Parity Migration Methodology

Once the Project Owner provides the reference specification and golden fixtures, the migration will follow this rigorous 4-step protocol:

```text
Step 1: Ingest Reference Dataset
  └─► Load golden test candles into canonical OQI Candle domain objects.

Step 2: Implement Migrated Baseline
  └─► Author src/open_quant_intelligence/strategy/baseline.py strictly against Strategy protocol.

Step 3: Point-by-Point Parity Suite
  └─► Execute baseline over rolling evaluation windows.
  └─► Assert Bit-for-Bit Identity:
        - Output signal == Expected golden signal
        - Decision timestamp == Expected golden timestamp
        - Reason code == Expected golden reason code
        - Indicator float values match within 1e-12 relative tolerance.

Step 4: Regression Lock
  └─► Commit golden reference fixtures into tests/fixtures/baseline_golden/
  └─► Lock parity suite into CI matrix.
```

---

## 7. Proposed Package Structure

To ensure clean separation of concerns and maintainability without over-engineering, M2 recommends the following package structure under `src/open_quant_intelligence/strategy/`:

```text
src/open_quant_intelligence/strategy/
├── __init__.py          # Public exports: Strategy, StrategyConfig, StrategyDecision, ResearchSignal
├── contracts.py         # Protocols, StrategyConfig, GapPolicy, exception hierarchy
├── signals.py           # ResearchSignal enum, StrategyDecision, ReasonCode constants
├── evaluator.py         # StrategyEvaluator: M0/M1 validation wrapper & causality enforcement
└── baseline.py          # Migrated baseline strategy implementation (BLOCKED ON OWNER INPUT)
```

### Module Responsibilities:

1. **`contracts.py`:**
   - Defines `Strategy` protocol via `typing.Protocol`.
   - Defines `StrategyConfig` frozen base class with canonical SHA-256 fingerprinting.
   - Defines `GapPolicy` enum.
   - Defines clean exception hierarchy (`StrategyEvaluationError`, `InsufficientHistoryError`, etc.).
2. **`signals.py`:**
   - Defines `ResearchSignal` (`LONG`, `FLAT`, `HOLD`).
   - Defines `StrategyDecision` frozen dataclass with post-init causality checks.
   - Defines standard `ReasonCode` constants (`WARMUP_INSUFFICIENT`, `LONG_SIGNAL_GENERATED`, `FLAT_SIGNAL_GENERATED`, `NO_CONDITION_MET`).
3. **`evaluator.py`:**
   - Defines `StrategyEvaluator`.
   - Encapsulates pre-flight verification: checks candle completeness (`is_closed`), homogeneity, chronological sequence, and gap policy.
   - Pure orchestrator ensuring strategies never encounter unvetted or corrupt inputs.
4. **`baseline.py`:**
   - Implements the concrete baseline strategy adhering strictly to `Strategy`.
   - Contains zero dependencies outside standard library and OQI domain models.
   - Implementation is deferred until owner supplies the legacy specification.

### Architecture Simplification Justifications:
- **No Heavy Indicator Libraries:** Do not introduce TA-Lib, pandas-ta, or external C-extensions. Any required baseline math will be implemented in pure, transparent Python or NumPy, ensuring seamless cross-platform builds on Windows and Linux.
- **No Dynamic Plugin Loaders:** Strategies will be standard Python classes imported explicitly, avoiding fragile reflection, runtime code execution, or dynamic import hooks.
- **No Internal Database Access:** The strategy module does not query DuckDB. The calling workflow (e.g., CLI or research script) queries the catalog, retrieves candles, and supplies them to the evaluator.

---

## 8. Determinism & Reproducibility

Reproducibility is a fundamental pillar of quantitative research. Milestone M2 enforces reproducibility through cryptographic provenance:

```text
┌────────────────────────────────────────────────────────┐
│ Input Artifact Provenance                              │
│   - M1 Dataset Content Fingerprint: sha256:[0-9a-f]{64}│
│   - Candle Count & Window Bounds                       │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ Strategy Specification Provenance                      │
│   - Strategy Identifier (e.g. "baseline-trend")         │
│   - Semantic Version (e.g. "1.0.0")                    │
│   - StrategyConfig SHA-256 Fingerprint                 │
└───────────────────────────┬────────────────────────────┘
                            │
                            ▼
               [ Pure Function: evaluate() ]
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ Deterministic Output Decision                          │
│   - Identical Signal, Reason Code, and Metadata        │
│   - 100% Bit-for-Bit Replicable Across Environments    │
└────────────────────────────────────────────────────────┘
```

### Mathematical Determinism Invariants:
1. **Purity:** Given identical inputs `(config, history, fingerprint)`, `strategy.evaluate()` will produce identical decisions on Python 3.11 and 3.12, Windows and Ubuntu, today or ten years from now.
2. **Floating-Point Stability:** Mathematical calculations avoid architecture-dependent approximations. Sums and multiplications adhere to standard IEEE 754 precision.
3. **Immutability:** Strategy parameters, input candles, and emitted decisions cannot be modified after instantiation (`frozen=True`, `slots=True`).

---

## 9. Testing & Acceptance Criteria

When implementation is authorized, M2 will deliver a comprehensive, offline, deterministic test suite under `tests/unit/strategy/`:

### 9.1 Unit Test Coverage Areas

1. **Protocol & Contract Enforcement (`test_strategy_contracts.py`):**
   - Verify `StrategyDecision` is frozen and rejects unequal `decision_time` vs `source_candle_close_time`.
   - Verify `StrategyConfig` calculates deterministic SHA-256 fingerprints across Python dictionaries.
   - Verify `Strategy` protocol enforcement via `isinstance()` checks.
2. **Evaluator Pre-Flight & Safety (`test_strategy_evaluator.py`):**
   - Rejection of empty candle sequences.
   - Rejection of sequences shorter than `minimum_history_candles` (`InsufficientHistoryError`).
   - Rejection of unclosed candles (`is_closed=False`).
   - Rejection of mixed symbols (e.g., mixing `BTC/USDT` with `ETH/USDT`).
   - Rejection of mixed timeframes (e.g., mixing `1h` with `15m`).
   - Rejection of out-of-order or duplicate timestamps.
3. **Data Quality & Gap Policy Enforcement (`test_strategy_gap_policy.py`):**
   - Behavior under `STRICT_REJECT`: raises `StrategyDataGapError` on any missing interval.
   - Behavior under `ABORT_ON_INTERNAL`: permits valid history with leading gaps, but rejects internal gaps.
   - Verification that no forward-filling or interpolation ever takes place.
4. **Causality & Lookahead Protection (`test_strategy_causality.py`):**
   - Provide an evaluation window ending at candle $T$. Mutate subsequent candle $T+1$ in the input. Prove that decision at $T$ is mathematically independent of $T+1$.
5. **Idempotency & Isolation (`test_strategy_determinism.py`):**
   - Run `evaluate()` 1,000 times consecutively on identical inputs. Prove output decisions, timestamps, reason codes, and indicator floats are bit-for-bit identical.
   - Run parallel evaluations across multiple threads/processes. Assert zero cross-contamination.
6. **Baseline Migration Golden Tests (`test_baseline_parity.py`):**
   - Execute baseline strategy against the owner-provided golden reference dataset.
   - Assert 100% parity across all emitted decisions.

---

## 10. Strict Milestone Exclusions

To maintain focus and avoid architectural bloat, the following capabilities are explicitly excluded from M2:

- **Simulation & Backtesting Loops:** Processing sequences of signals over time with trade accounting belongs to M3.
- **Portfolio & Wallet Management:** Tracking cash, margin, capital allocation, exposure percentages, and position sizing belongs to M3/M4.
- **Execution & Exchange Integration:** Interacting with exchanges, submitting orders, or routing trades belongs to M4+.
- **Realistic Execution Models:** Simulating fills, maker/taker fees, slippage, orderbook depth, and latency belongs to M3.
- **Performance Analytics:** Calculating Sharpe ratio, Sortino ratio, max drawdown, CAGR, and win rates belongs to M3.
- **Optimization & Hyperparameter Tuning:** Grid searches, Bayesian optimization, and walk-forward analysis belong to M5.
- **Machine Learning & Feature Stores:** Predictive models, neural networks, and regime clustering belong to M6.
- **Live Trading & Dashboards:** Streaming data, WebSockets, telegram alerts, and UI dashboards are out of scope.

---

## 11. Risks, Failure Modes, and Open Decisions

### 11.1 Key Risks & Mitigation

| Risk | Impact | Mitigation Strategy |
|---|---|---|
| **Lack of Legacy Baseline Specification** | High (Blocks M2 completion) | Documented clearly as an unresolved owner dependency. Implementation cannot start until owner supplies reference specification and golden fixtures. |
| **Lookahead Bias Leakage** | High (Invalidates research results) | Mathematically locked causality: `decision_time == source_candle_close_time`. Pre-flight evaluator enforces strict sequential timestamps. |
| **Silent Gap Corruption** | Medium (Flawed indicator math) | M1 `DataQualityReport` reused; `GapPolicy` strictly enforced; silent forward-filling prohibited. |
| **Float Discrepancies Across Platforms** | Low (Minor parity mismatch) | IEEE 754 standard operations only; unit tests enforce tolerances on indicator float metadata. |

---

### 11.2 Open Decisions for Technical Authority & Project Owner

The following items are presented for review and approval:

1. **Open Decision 1: Owner Supply of Baseline Specification (BLOCKER)**
   - *Question:* When and how will the legacy baseline algorithm source, parameter defaults, and golden reference fixtures be provided?
   - *Options:*
     - A: Project Owner commits a specification document and reference CSV fixture to the repository.
     - B: Project Owner designates an approved new baseline strategy (e.g. canonical SMA crossover) as the new reference standard.
2. **Open Decision 2: Single-Candle vs Sequence Evaluation Interface**
   - *Question:* Should `Strategy.evaluate()` operate strictly on the final candle of a history slice (point-in-time), or should the interface also offer an optimized vectorized batch method (`evaluate_series()`) for backtest acceleration in M3?
   - *Recommendation:* Keep `Strategy.evaluate(history)` as the primary canonical single-candle contract in M2. Batch evaluation can be implemented cleanly in M3 as a vectorized loop over the canonical evaluator.
3. **Open Decision 3: Handling Insufficient Warmup History**
   - *Question:* When `len(history) < minimum_history_candles`, should the evaluator raise an `InsufficientHistoryError`, or should the strategy return `StrategyDecision(signal=HOLD, reason_code="WARMUP_INSUFFICIENT")`?
   - *Recommendation:* Support both: raise `InsufficientHistoryError` in strict evaluation mode, but provide an evaluator option to emit structured warmup decisions with `reason_code="WARMUP_INSUFFICIENT"` for simulation pipelines.

---

## 12. M2 Completion Criteria

Milestone M2 will be considered complete and ready for merge into `main` when:

1. The `Strategy` protocol, `StrategyConfig`, `ResearchSignal`, and `StrategyDecision` contracts are implemented under `src/open_quant_intelligence/strategy/`.
2. The `StrategyEvaluator` is implemented, enforcing all M0/M1 validation checks, gap policies, and point-in-time causality.
3. The Project Owner supplies the baseline specification and reference fixtures.
4. The baseline strategy is implemented and verified against the golden reference test suite with 100% signal parity.
5. All offline unit tests pass cleanly across Python 3.11 and 3.12 on both Ubuntu and Windows CI runners.
6. `ruff check` and `ruff format` report zero errors.
7. Technical Authority (ChatGPT) and Project Owner grant formal written approval.
8. No M3 simulation, execution, or portfolio code is introduced.
