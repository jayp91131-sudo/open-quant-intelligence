# Open Quant Intelligence V2 — Milestone M2 Architecture Proposal
## Strategy Interface & Baseline Migration (Revised Architecture Proposal)

**Document Reference:** `docs/M2_ARCHITECTURE_PROPOSAL.md`  
**Milestone:** M2 — Strategy Interface & Baseline Migration  
**Base Commit on `main`:** `0781ce2`  
**Branch:** `docs/m2-strategy-interface-proposal`  
**Author:** AntiGravity (Implementation Engineer)  
**Technical Authority & Reviewer:** ChatGPT  
**Final Decision Maker:** Project Owner  
**Status:** DRAFT CANDIDATE — REVISED PROPOSAL (Implementation is strictly NOT authorized)  

---

## Table of Contents

1. [Executive Summary & Objectives](#1-executive-summary--objectives)
2. [Current Architecture & M0/M1 Compatibility](#2-current-architecture--m0m1-compatibility)
3. [Strategy Interface & Data Contracts](#3-strategy-interface--data-contracts)
   - 3.1 [Research Signal Vocabulary & HOLD Semantics](#31-research-signal-vocabulary--hold-semantics)
   - 3.2 [Evaluation Context Contract](#32-evaluation-context-contract)
   - 3.3 [Strategy Decision Contract & Deep Immutability](#33-strategy-decision-contract--deep-immutability)
   - 3.4 [Strategy Configuration & Deterministic Fingerprinting](#34-strategy-configuration--deterministic-fingerprinting)
   - 3.5 [Gap Policy Specification](#35-gap-policy-specification)
   - 3.6 [The Strategy Protocol](#36-the-strategy-protocol)
4. [Input Validation & Data-Quality Policy](#4-input-validation--data-quality-policy)
   - 4.1 [Re-use of M0 and M1 Foundations](#41-re-use-of-m0-and-m1-foundations)
   - 4.2 [Evaluator Pre-Flight Verification Rules](#42-evaluator-pre-flight-verification-rules)
   - 4.3 [Structured Error Hierarchy](#43-structured-error-hierarchy)
5. [Signal Semantics, Causality & Timing Rules](#5-signal-semantics-causality--timing-rules)
   - 5.1 [Point-in-Time Causality & Availability at $T_{\text{close}}$](#51-point-in-time-causality--availability-at-t_textclose)
   - 5.2 [Execution Decoupling & Fill Conventions (M3 Scope)](#52-execution-decoupling--fill-conventions-m3-scope)
   - 5.3 [Stateless Intention & Transition Semantics](#53-stateless-intention--transition-semantics)
   - 5.4 [Clear Separation from Milestone M3](#54-clear-separation-from-milestone-m3)
6. [Baseline Migration Plan & Unresolved Dependency (BLOCKER)](#6-baseline-migration-plan--unresolved-dependency-blocker)
   - 6.1 [Unresolved Dependency Statement (CRITICAL BLOCKER)](#61-unresolved-dependency-statement-critical-blocker)
   - 6.2 [Required Owner Input Deliverables](#62-required-owner-input-deliverables)
   - 6.3 [Golden Parity Migration Methodology & Tolerances](#63-golden-parity-migration-methodology--tolerances)
7. [Proposed Package Structure](#7-proposed-package-structure)
8. [Determinism, Provenance & Parity Guarantees](#8-determinism-provenance--parity-guarantees)
9. [Testing & Acceptance Criteria](#9-testing--acceptance-criteria)
10. [Strict Milestone Exclusions](#10-strict-milestone-exclusions)
11. [Risks, Failure Modes, and Open Decisions](#11-risks-failure-modes-and-open-decisions)
12. [M2 Completion Criteria](#12-m2-completion-criteria)

---

## 1. Executive Summary & Objectives

Milestone M2 transitions Open Quant Intelligence V2 from a robust, crash-resilient data ingestion and storage foundation (established in M0 and M1) into the quantitative research domain. The primary objective of M2 is to define a minimal, strictly typed, deterministic strategy interface protocol and establish a verifiable baseline migration framework.

In quantitative finance, strategies frequently suffer from subtle production-backtest divergence caused by hidden state mutation, lookahead leakage, loose timestamp conventions, unhandled missing data intervals, or implicit coupling to portfolio simulation engines. M2 eliminates these vulnerabilities by enforcing a **pure functional paradigm**:

$$\text{StrategyDecision} = f(\text{Immutable Strategy Config}, \text{Validated Historical Candles}, \text{Immutable Evaluation Context})$$

### Core Objectives of M2:

1. **Explicit, Minimal Strategy Protocol:** Define a formal `Strategy` interface contract with an immutable configuration identity, semantic versioning, declared minimum lookback history, and explicit gap handling policy.
2. **Authoritative Evaluation Context:** Decouple data retrieval from strategy logic by supplying an immutable `EvaluationContext` that contains dataset provenance, content fingerprints, requested analysis window bounds, and M1 `DataQualityReport` metadata.
3. **Deterministic Point-in-Time Evaluation:** Ensure evaluation functions as a pure mathematical transformation of finalized historical candles $[T_{\text{open}}, T_{\text{close}})$. Evaluation is strictly isolated from side effects: zero file I/O, zero network calls, zero database queries, zero system clock access, and zero pseudo-randomness.
4. **Structured Research Intentions (Signals):** Standardize research signal vocabulary into three unambiguous states: `LONG`, `FLAT`, and `HOLD`. These express directional research intention, completely detached from exchange order execution, position sizing, margin, or leverage.
5. **Rigorous Data Quality Re-use:** Enforce that strategies evaluate only fully closed candles (`is_closed=True`) validated against M0 schema invariants (`validate_candles`) and M1 gap detection (`analyze_dataset_quality`). Unaligned candle timestamps are strictly rejected. Data is never silently sorted, interpolated, or forward-filled.
6. **Canonical Warmup Handling:** Enforce a single canonical behavior for insufficient history: raise `InsufficientHistoryError` whenever available history is less than `strategy.minimum_history_candles`. No dummy or fabricated decisions are emitted without an eligible source candle.
7. **Baseline Migration with Parity Verification:** Provide a formal golden-test migration methodology for the project's baseline strategy while explicitly documenting the current absence of legacy strategy code as an **unresolved owner-input dependency blocker**.
8. **Strict Milestone Boundaries:** Confine M2 strictly to decision evaluation. All execution loops, fill assumptions, slippage, commission modeling, portfolio state tracking, and performance metrics are quarantined to Milestone M3.

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
│   - Validation API: validate_candles()                                │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ consumes
┌───────────────────────────────────▼────────────────────────────────────┐
│ M1 — Ingestion, Storage & Catalog Foundation                           │
│   - Public Binance Vision Ingestion (allowlisted Spot endpoints)       │
│   - Canonical Parquet Datasets (Snappy compression, immutable)         │
│   - DuckDB Snapshot Catalog (CatalogDatasetRecord, dataset_gaps)       │
│   - Content Fingerprints: sha256:[0-9a-f]{64}                          │
│   - Quality Analysis: analyze_dataset_quality(), DataQualityReport     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ consumes
┌───────────────────────────────────▼────────────────────────────────────┐
│ M2 — Strategy Interface & Evaluation (THIS PROPOSAL)                   │
│   - Strategy Protocol & StrategyConfig (Immutable, frozen)             │
│   - EvaluationContext: dataset provenance & M1 DataQualityReport       │
│   - ResearchSignal: LONG, FLAT, HOLD (Stateless research intent)       │
│   - StrategyDecision: Point-in-time signal at T_close with reason_code │
│   - StrategyEvaluator: Strict M0/M1 validation & lookback verification │
│   - Baseline Strategy Migration: Golden parity test suite              │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ produces decisions for
┌───────────────────────────────────▼────────────────────────────────────┐
│ M3 — Simulation & Backtesting Engine (FUTURE MILESTONE)                │
│   - Event-driven / vectorized simulation loops                         │
│   - Portfolio state, cash accounting, position tracking                │
│   - Fill conventions (e.g. Next-Bar Open), slippage, fee modeling      │
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

### 3.1 Research Signal Vocabulary & HOLD Semantics

In OQI V2, strategy outputs are **research intentions**, not execution orders. The signal vocabulary is restricted to three discrete states:

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

#### Detailed Semantics of `HOLD`:
- **Stateless Intention:** A strategy interface is strictly stateless and does not receive portfolio state, current account balances, or existing trade positions.
- **Explicit No-Transition:** Emitting `HOLD` means the strategy has evaluated all market conditions up to the current candle close and determined that **no state transition** should occur.
- **Simulation Engine Ownership:** The consuming M3 simulation engine maintains the current portfolio state, trade history, and previous stance. When M3 receives `HOLD`, it preserves its active stance (e.g., remains Long if already Long, or remains Flat if already Flat).
- **No Position Accounting in M2:** Portfolio state and position accounting are explicitly forbidden within M2 contracts and strategy logic.

---

### 3.2 Evaluation Context Contract

To keep database access strictly outside strategy implementations while ensuring complete auditability and data-quality awareness, M2 introduces the immutable `EvaluationContext`.

The caller (e.g., backtest runner, CLI, or test harness) queries the catalog, validates Parquet snapshot integrity, computes or retrieves the authoritative M1 `DataQualityReport`, and packages them into `EvaluationContext` before invoking evaluation:

```python
import re
from dataclasses import dataclass
from open_quant_intelligence.domain.timeframe import SupportedTimeframe
from open_quant_intelligence.data.quality import DataQualityReport


@dataclass(frozen=True, slots=True)
class EvaluationContext:
    """Authoritative dataset provenance and data-quality metadata supplied to strategy evaluation.
    
    Database access remains strictly external to strategies. The caller retrieves data,
    verifies snapshot integrity, computes the M1 DataQualityReport, and constructs this context.
    """

    dataset_identity: str
    content_fingerprint: str
    requested_since_ms: int
    requested_until_ms: int
    timeframe: SupportedTimeframe
    quality_report: DataQualityReport

    def __post_init__(self) -> None:
        if not re.match(r"^sha256:[0-9a-f]{64}$", self.content_fingerprint):
            raise ValueError(
                f"Invalid content_fingerprint format: '{self.content_fingerprint}'. "
                "Must match ^sha256:[0-9a-f]{64}$."
            )
        if self.requested_until_ms <= self.requested_since_ms:
            raise ValueError(
                f"requested_until_ms ({self.requested_until_ms}) must be strictly greater "
                f"than requested_since_ms ({self.requested_since_ms})."
            )
        if not self.dataset_identity.strip():
            raise ValueError("dataset_identity cannot be empty.")
```

---

### 3.3 Strategy Decision Contract & Deep Immutability

Every strategy evaluation produces a single, fully auditable, deeply immutable `StrategyDecision` representing the quantitative conclusion for the latest closed candle:

```python
import types
from dataclasses import dataclass
from datetime import datetime
from typing import Mapping
from open_quant_intelligence.domain.timeframe import SupportedTimeframe


# Permitted primitive scalar types for decision metadata
MetadataScalar = int | float | str | bool


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
    timeframe: SupportedTimeframe
    source_candle_open_time: datetime
    source_candle_close_time: datetime
    decision_time: datetime

    # Decision Output
    signal: ResearchSignal
    reason_code: str
    metadata: Mapping[str, MetadataScalar]

    def __post_init__(self) -> None:
        # Strict Point-in-Time Causality Check
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
        if not re.match(r"^sha256:[0-9a-f]{64}$", self.config_fingerprint):
            raise ValueError(f"Invalid config_fingerprint format: '{self.config_fingerprint}'.")
        if not re.match(r"^sha256:[0-9a-f]{64}$", self.dataset_content_fingerprint):
            raise ValueError(f"Invalid dataset_content_fingerprint format: '{self.dataset_content_fingerprint}'.")

        # Enforce Deep Immutability and Type Restriction on Metadata
        for k, v in self.metadata.items():
            if not isinstance(k, str):
                raise TypeError(f"Metadata key must be str, got {type(k).__name__}: {k!r}")
            if not isinstance(v, (int, float, str, bool)) or isinstance(v, bool) and not isinstance(v, bool):
                raise TypeError(
                    f"Metadata value for key '{k}' must be primitive scalar (int, float, str, bool), "
                    f"got {type(v).__name__}: {v!r}"
                )
            if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")):
                raise ValueError(f"Metadata float value for key '{k}' must be finite, got: {v}")

        # Wrap in read-only mapping proxy to prevent in-place mutation
        object.__setattr__(self, "metadata", types.MappingProxyType(dict(self.metadata)))
```

---

### 3.4 Strategy Configuration & Deterministic Fingerprinting

Strategy parameters are encapsulated in an immutable configuration model capable of deterministic SHA-256 fingerprinting.

#### Supported Data Types & Validation:
- Permitted configuration values are strictly limited to primitive JSON scalars: `int`, `float`, `str`, `bool`, and tuples of these scalars.
- Arbitrary Python objects, callable functions, dictionaries, sets, and nested dataclasses are disallowed to guarantee universal cross-platform serialization.
- Special floating-point values (`NaN`, positive infinity, negative infinity) are strictly rejected.
- Fingerprint generation utilizes canonical JSON serialization: UTF-8 encoding, keys sorted lexicographically, and compact delimiters `separators=(',', ':')`.

```python
import hashlib
import json
import math
from dataclasses import asdict, dataclass
from typing import Any


def _validate_config_value(val: Any) -> None:
    if isinstance(val, (int, str, bool)):
        return
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            raise ValueError(f"Non-finite float value not permitted in configuration: {val}")
        return
    if isinstance(val, (tuple, list)):
        for item in val:
            _validate_config_value(item)
        return
    raise TypeError(
        f"Unsupported configuration value type: {type(val).__name__}. "
        "Only int, float, str, bool, and tuples thereof are permitted."
    )


@dataclass(frozen=True)
class StrategyConfig:
    """Base class for immutable strategy configuration parameters."""

    def __post_init__(self) -> None:
        for k, v in asdict(self).items():
            _validate_config_value(v)

    def compute_fingerprint(self) -> str:
        """Compute canonical SHA-256 fingerprint of configuration attributes."""
        raw = asdict(self)
        payload = json.dumps(
            raw,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return f"sha256:{hashlib.sha256(payload).hexdigest()}"
```

---

### 3.5 Gap Policy Specification

Each strategy must explicitly state its operational policy when confronting missing intervals. In accordance with OQI V2 data invariants, unaligned timestamps are **always invalid** and can never be accepted by any gap policy.

The supported gap policies govern missing intervals between aligned candles:

```python
from enum import Enum


class GapPolicy(str, Enum):
    """Explicit policy defining how a strategy handles historical missing intervals.
    
    Unaligned candle timestamps are ALWAYS invalid and rejected prior to policy evaluation.
    Missing intervals are NEVER silently patched, forward-filled, or interpolated.
    """
    STRICT_GAP_FREE = "STRICT_GAP_FREE"
    """Strict initial policy: Reject evaluation if ANY gap event exists in the evaluated window
    or candle history (gap_count > 0)."""

    REJECT_INTERNAL_GAPS = "REJECT_INTERNAL_GAPS"
    """Reject evaluation if any INTERNAL gap exists between candles in the series.
    Leading or trailing gaps (where dataset boundaries do not cover the entire requested window)
    are permitted provided the evaluated history itself is continuous."""
```

> [!NOTE]
> The previously contemplated `ALLOW_UNALIGNED_GAP_EXPLICIT` has been permanently removed. M0 and M1 enforce rigid timeframe grid alignment; unaligned candles represent data corruption and cannot be accepted under any quantitative evaluation policy.

---

### 3.6 The Strategy Protocol

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
        """Semantic version string of the strategy implementation (e.g. '1.0.0')."""
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
        context: EvaluationContext,
    ) -> StrategyDecision:
        """Evaluate history ending at history[-1] and return a deterministic StrategyDecision.
        
        Invariants:
        1. Pure function: No I/O, no network, no database, no system clock, no state mutation.
        2. history is strictly ordered, non-empty, and satisfies len(history) >= minimum_history_candles.
        3. context supplies valid dataset provenance matching ^sha256:[0-9a-f]{64}$ and M1 quality metadata.
        4. Decision corresponds strictly to the close boundary of history[-1].
        """
        ...
```

---

## 4. Input Validation & Data-Quality Policy

### 4.1 Re-use of M0 and M1 Foundations

M2 does NOT re-implement data validation or gap detection from scratch. Instead, it directly re-uses existing, audited foundation components:

1. **M0 Validation (`open_quant_intelligence.data.validation.validate_candles`):**
   - The authoritative public function `validate_candles(candles, fail_on_gaps=False, expected_timeframe=...)` is invoked to verify that every candle adheres to domain schema rules.
   - Validates positive prices, finite numbers ($H \ge \max(O, C, L)$, $L \le \min(O, C, H)$), and non-negative volume.
   - Rejects unparseable or naive timestamps.
   - Enforces strict ascending chronological order: $T_{\text{open}, i} < T_{\text{open}, i+1}$.
   - Rejects duplicate timestamps.
2. **M1 Quality Analysis (`open_quant_intelligence.data.quality.analyze_dataset_quality`):**
   - Authoritative function `analyze_dataset_quality(candles, since_ms, until_ms, timeframe)` produces the `DataQualityReport`.
   - Audits candle grid alignment: $(T_{\text{open}} - T_{\text{since}}) \pmod{\Delta} == 0$. Raises `UnalignedCandleError` if any candle is off-grid.
   - Quantifies exact missing intervals: $missing\_intervals = expected - count$.
   - Classifies gap events into `LEADING`, `INTERNAL`, `TRAILING`, and `FULL_WINDOW` via `QualityGapEvent`.

#### Critical Distinction: Unaligned vs Missing Intervals:
- **Unaligned Candle Timestamps:** Represents malformed or corrupt data violating the discrete timeframe lattice. **Always invalid**; rejected immediately with `StrategyEvaluationError`.
- **Missing Intervals (Gaps):** Validly aligned candles where one or more expected intermediate lattice steps are absent. Evaluated against the strategy's declared `GapPolicy`.

---

### 4.2 Evaluator Pre-Flight Verification Rules

The `StrategyEvaluator` sits between dataset storage and the `Strategy.evaluate()` method. Before calling `strategy.evaluate()`, the evaluator performs the following pre-flight checks:

```text
Input: Sequence[Candle], EvaluationContext
   │
   ├─► Check 1: Provenance & Fingerprint Validation
   │     - Validate context.content_fingerprint matches ^sha256:[0-9a-f]{64}$
   │     - Validate context.dataset_identity is non-empty
   │
   ├─► Check 2: Non-Empty & Closed Status
   │     - len(history) > 0
   │     - All candles must have is_closed == True (unfinalized candles rejected)
   │
   ├─► Check 3: Market & Timeframe Homogeneity
   │     - Every candle must share identical exchange_id, market_type ("spot"), symbol, and timeframe
   │     - Candle timeframe must match context.timeframe
   │
   ├─► Check 4: M0 Schema & Chronological Validation
   │     - Execute validate_candles(history, fail_on_gaps=False, expected_timeframe=context.timeframe)
   │     - Any validation error raises ChronologicalOrderError or StrategyEvaluationError
   │
   ├─► Check 5: Canonical Minimum Lookback / History Check
   │     - Check: len(history) >= strategy.minimum_history_candles
   │     - If len(history) < minimum_history_candles:
   │         RAISE InsufficientHistoryError (Canonical behavior; no dummy decisions)
   │
   ├─► Check 6: Grid Alignment Check
   │     - Confirm zero unaligned candles. Unaligned candles raise StrategyEvaluationError immediately.
   │
   ├─► Check 7: Gap Policy Enforcement
   │     - Inspect context.quality_report
   │     - If strategy.gap_policy == STRICT_GAP_FREE:
   │         Reject if context.quality_report.gap_count > 0
   │     - If strategy.gap_policy == REJECT_INTERNAL_GAPS:
   │         Reject if any gap event has gap_type == GapType.INTERNAL
   │
   ▼
Passes Pre-Flight -> Execute strategy.evaluate(history, context)
```

---

### 4.3 Structured Error Hierarchy

All evaluation errors inherit from `StrategyEvaluationError` to enable predictable, structured handling by consumers:

```python
class StrategyEvaluationError(Exception):
    """Base exception for all strategy evaluation failures."""


class InsufficientHistoryError(StrategyEvaluationError):
    """Raised when available candle history is strictly less than minimum_history_candles."""


class IncompleteCandleError(StrategyEvaluationError):
    """Raised when an unclosed candle (is_closed=False) is supplied to evaluation."""


class HeterogeneousSeriesError(StrategyEvaluationError):
    """Raised when candles contain mixed symbols, timeframes, or non-spot markets."""


class ChronologicalOrderError(StrategyEvaluationError):
    """Raised when candles are out of order or contain duplicate timestamps."""


class StrategyDataGapError(StrategyEvaluationError):
    """Raised when input candles violate the strategy's declared GapPolicy."""
```

---

## 5. Signal Semantics, Causality & Timing Rules

### 5.1 Point-in-Time Causality & Availability at $T_{\text{close}}$

A central flaw in quantitative strategy design is lookahead bias caused by timestamp ambiguity (e.g. using bar open time to execute trades that depend on bar close prices). M2 resolves this mathematically:

$$\text{Candle } C_i = [T_{\text{open}, i}, T_{\text{close}, i})$$

1. **Information Finalization:** The information concerning candle $C_i$ (its high, low, close, and volume) does not exist until the exclusive close timestamp $T_{\text{close}, i}$.
2. **Decision Availability:** The earliest instant a strategy can evaluate candle $C_i$ is at $t = T_{\text{close}, i}$.
3. **Causality Invariant:**
   $$\text{decision\_time} \equiv T_{\text{close}, i}$$
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

### 5.2 Execution Decoupling & Fill Conventions (M3 Scope)

A common mistake in strategy interface specifications is assuming that because a decision is timestamped at $T_{\text{close}, i}$, the backtest simulator can "fill" an order at the close price $C_i.\text{close}$.

**M2 explicitly rejects this assumption:**
- Observing $C_i.\text{close}$ is a prerequisite for generating the decision at $T_{\text{close}, i}$.
- In live execution, an order transmitted after $T_{\text{close}, i}$ cannot retrospectively execute at $C_i.\text{close}$.
- Fills require price observations that become available **strictly after** the decision is emitted.
- While backtest engines (M3) frequently use the **Next-Bar Open** convention (filling at $C_{i+1}.\text{open}$ or an execution model price), this is an M3 simulation modeling choice, not a strategy interface assumption.
- **Strict Separation:** All fill modeling, slippage, commission, order routing, and execution price determination belong exclusively to M3. M2 outputs only the research intention.

---

### 5.3 Stateless Intention & Transition Semantics

`ResearchSignal` states represent directional intent:

- **`LONG`**: Quantitative criteria indicate positive directional exposure to the base asset.
- **`FLAT`**: Quantitative criteria indicate zero exposure (100% cash / quote currency).
- **`HOLD`**: Quantitative criteria indicate no state transition; maintain whatever exposure stance previously existed.

```text
State Transition Diagram (Research Stance Managed by M3 Simulator):

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

*Prefix Invariance Property:* Strategies must be stateless pure functions. When evaluating a historical series $C_0, \dots, C_T$, the decision produced for $C_T$ must be completely invariant to whether previous evaluation calls were made on earlier sub-slices.

---

### 5.4 Clear Separation from Milestone M3

M2 explicitly stops at decision generation. The following responsibilities belong to M3 and are prohibited in M2:

| Concern | Handled in M2? | Handled in M3? | Architectural Boundary |
|---|:---:|:---:|---|
| Directional research intent (`LONG`/`FLAT`/`HOLD`) | **YES** | Consumes M2 | Strategy logic defines intention. |
| Reason code & indicator metadata | **YES** | Consumes M2 | Explains why strategy signaled. |
| Evaluation context & provenance metadata | **YES** | Supplies M2 | Caller verifies M1 Parquet snapshot. |
| Pre-flight quality & lookback enforcement | **YES** | Pre-condition | Evaluator verifies series validity. |
| Portfolio cash & coin balance tracking | **NO** | **YES** | Strategies do not manage accounts. |
| Position sizing, leverage & margin | **NO** | **YES** | Risk & capital allocation concern. |
| Order execution modeling (Next-Bar Open fills) | **NO** | **YES** | Simulation engine fill model concern. |
| Slippage, spread & fee calculation | **NO** | **YES** | Execution realism model concern. |
| PnL, Sharpe, Sortino, Drawdown | **NO** | **YES** | Performance analytics concern. |

---

## 6. Baseline Migration Plan & Unresolved Dependency (BLOCKER)

### 6.1 Unresolved Dependency Statement (CRITICAL BLOCKER)

> [!CAUTION]
> **IMPLEMENTATION BLOCKER — UNRESOLVED OWNER INPUT REQUIRED**  
> A comprehensive code and repository audit of `main` at commit `0781ce2` confirms that **no legacy strategy implementation exists** within the repository.
> 
> The implementation engineer is strictly prohibited from inventing, assuming, or guessing the baseline strategy algorithm (such as an arbitrary SMA crossover, EMA MACD, RSI threshold, or Buy-and-Hold).
> 
> Until the Project Owner provides the definitive legacy baseline specification, implementation of the baseline strategy module cannot proceed.

---

### 6.2 Required Owner Input Deliverables

Before baseline migration code can be authored in M2, the Project Owner must supply or formally approve the following specification:

1. **Algorithm Definition & Mathematical Rules:**
   - Exact mathematical formulation or legacy Python/pseudocode source for all indicators and decision rules.
   - Exact tie-breaking logic (e.g., condition when indicator value exactly equals threshold).
2. **Parameter Defaults & Configuration:**
   - Names, types, and default values for all strategy parameters.
   - Declared minimum lookback history length (`minimum_history_candles`).
3. **Transition Triggers & Reason Codes:**
   - Exact conditions governing transitions between `LONG`, `FLAT`, and `HOLD`.
   - Exact machine-readable `reason_code` strings associated with each decision.
4. **Authoritative Golden Fixture Specification:**
   - Golden test datasets must be structured to cover:
     - Warmup history boundary cases ($N = \text{lookback} - 1$ and $N = \text{lookback}$).
     - Decision boundary transitions (`FLAT` $\to$ `LONG`, `LONG` $\to$ `FLAT`, `LONG` $\to$ `HOLD`, `FLAT` $\to$ `HOLD`).
     - Numerical tie conditions.
     - Gap policy scenarios (continuous series vs missing intervals).
   - Expected outputs must include exact signal strings, reason codes, decision timestamps, and reference indicator values.

---

### 6.3 Golden Parity Migration Methodology & Tolerances

Once the Project Owner provides the reference specification and golden fixtures, the migration will follow this rigorous 4-step protocol:

```text
Step 1: Ingest Reference Dataset
  └─► Load golden test candles into canonical OQI Candle domain objects.
  └─► Compute M1 quality report and construct EvaluationContext.

Step 2: Implement Migrated Baseline
  └─► Author src/open_quant_intelligence/strategy/baseline.py strictly against Strategy protocol.

Step 3: Point-by-Point Parity Verification
  └─► Execute baseline over rolling evaluation windows.
  └─► Discrete Exact Equality (==):
        - decision.signal == expected.signal
        - decision.decision_time == expected.decision_time
        - decision.reason_code == expected.reason_code
        - decision.strategy_id == expected.strategy_id
        - decision.strategy_version == expected.strategy_version
        - decision.config_fingerprint == expected.config_fingerprint
  └─► Floating-Point Tolerance Verification:
        - For each float indicator in decision.metadata:
          math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-12)

Step 4: Regression Lock
  └─► Commit golden reference fixtures into tests/fixtures/baseline_golden/
  └─► Lock parity suite into CI matrix.
```

---

## 7. Proposed Package Structure

To ensure clean separation of concerns and maintainability without over-engineering, M2 establishes the following package structure under `src/open_quant_intelligence/strategy/`:

```text
src/open_quant_intelligence/strategy/
├── __init__.py          # Public exports: Strategy, StrategyConfig, EvaluationContext, StrategyDecision, ResearchSignal
├── contracts.py         # Protocols, StrategyConfig, EvaluationContext, GapPolicy, exception hierarchy
├── signals.py           # ResearchSignal enum, StrategyDecision, ReasonCode constants
├── evaluator.py         # StrategyEvaluator: M0/M1 validation wrapper, causality & lookback enforcement
└── baseline.py          # Migrated baseline strategy implementation (BLOCKED ON OWNER INPUT)
```

### Module Responsibilities:

1. **`contracts.py`:**
   - Defines `Strategy` protocol via `typing.Protocol`.
   - Defines `EvaluationContext` immutable dataclass with fingerprint validation and M1 quality report.
   - Defines `StrategyConfig` frozen base class with canonical SHA-256 fingerprinting.
   - Defines `GapPolicy` enum (`STRICT_GAP_FREE`, `REJECT_INTERNAL_GAPS`).
   - Defines clean exception hierarchy (`StrategyEvaluationError`, `InsufficientHistoryError`, etc.).
2. **`signals.py`:**
   - Defines `ResearchSignal` (`LONG`, `FLAT`, `HOLD`).
   - Defines `StrategyDecision` frozen dataclass with post-init causality checks and deeply immutable `metadata`.
   - Defines standard `ReasonCode` constants (`WARMUP_INSUFFICIENT`, `LONG_SIGNAL_GENERATED`, `FLAT_SIGNAL_GENERATED`, `NO_CONDITION_MET`).
3. **`evaluator.py`:**
   - Defines `StrategyEvaluator`.
   - Encapsulates pre-flight verification: checks candle completeness (`is_closed`), homogeneity, chronological sequence (`validate_candles`), lookback (`InsufficientHistoryError`), grid alignment, and gap policy.
   - Pure orchestrator ensuring strategies never encounter unvetted or corrupt inputs.
4. **`baseline.py`:**
   - Implements the concrete baseline strategy adhering strictly to `Strategy`.
   - Contains zero dependencies outside standard library and OQI domain models.
   - Implementation is deferred until owner supplies the legacy specification.

### Architecture Simplification Justifications:
- **No Heavy Indicator Libraries:** Do not introduce TA-Lib, pandas-ta, or external C-extensions. Any required baseline math will be implemented in pure, transparent Python or NumPy, ensuring seamless cross-platform builds on Windows and Linux.
- **No Dynamic Plugin Loaders:** Strategies will be standard Python classes imported explicitly, avoiding fragile reflection, runtime code execution, or dynamic import hooks.
- **No Internal Database Access:** The strategy module does not query DuckDB. The calling workflow (e.g., CLI or research script) queries the catalog, verifies Parquet snapshot integrity, and supplies data and `EvaluationContext` to the evaluator.

---

## 8. Determinism, Provenance & Parity Guarantees

Reproducibility is a fundamental pillar of quantitative research. Milestone M2 enforces reproducibility through cryptographic provenance:

```text
┌────────────────────────────────────────────────────────┐
│ Input Artifact Provenance                              │
│   - M1 Dataset Content Fingerprint: sha256:[0-9a-f]{64}│
│   - Candle Count & Requested Window Bounds             │
│   - M1 DataQualityReport                               │
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
│   - Identical Signal, Reason Code, and Provenance      │
│   - Floating-Point Indicators Verified Within Tolerance│
└────────────────────────────────────────────────────────┘
```

### Determinism & Parity Rules:

1. **Exact Parity on Discrete Fields:**
   For any evaluation, the following attributes must match using exact equality (`==`):
   - `signal` (`ResearchSignal`)
   - `reason_code` (`str`)
   - `strategy_id` (`str`)
   - `strategy_version` (`str`)
   - `config_fingerprint` (`str`)
   - `dataset_content_fingerprint` (`str`)
   - `symbol` (`str`)
   - `timeframe` (`SupportedTimeframe`)
   - `source_candle_open_time` (`datetime`)
   - `source_candle_close_time` (`datetime`)
   - `decision_time` (`datetime`)
2. **Floating-Point Tolerances on Metadata:**
   Universal bit-for-bit identity across disparate CPU architectures and compiler optimizations is mathematically unattainable for general transcendental floating-point operations. Numeric indicator values stored in `metadata` are evaluated using documented tolerances:
   $$\text{rel\_tol} = 10^{-9}, \quad \text{abs\_tol} = 10^{-12}$$
   ```python
   math.isclose(actual_val, golden_val, rel_tol=1e-9, abs_tol=1e-12)
   ```
3. **Deep Immutability:**
   Strategy parameters, input candles, and emitted decisions cannot be modified after instantiation (`frozen=True`, `slots=True`, `MappingProxyType`).

---

## 9. Testing & Acceptance Criteria

When implementation is authorized, M2 will deliver a comprehensive, offline, deterministic test suite under `tests/unit/strategy/`:

### 9.1 Unit Test Coverage Areas

1. **Protocol & Contract Enforcement (`test_strategy_contracts.py`):**
   - Verify `StrategyDecision` is frozen and rejects unequal `decision_time` vs `source_candle_close_time`.
   - Verify `StrategyDecision` rejects mutable or non-scalar metadata and wraps metadata in `MappingProxyType`.
   - Verify `StrategyConfig` calculates deterministic SHA-256 fingerprints across Python dictionaries and rejects non-finite floats (`NaN`, `inf`).
   - Verify `EvaluationContext` validates content fingerprint format (`^sha256:[0-9a-f]{64}$`) and requested bounds.
   - Verify `Strategy` protocol enforcement via `isinstance()` checks.
2. **Evaluator Pre-Flight & Safety (`test_strategy_evaluator.py`):**
   - Rejection of empty candle sequences.
   - Rejection of sequences shorter than `minimum_history_candles` with canonical `InsufficientHistoryError`.
   - Rejection of unclosed candles (`is_closed=False`).
   - Rejection of mixed symbols (e.g., mixing `BTC/USDT` with `ETH/USDT`).
   - Rejection of mixed timeframes (e.g., mixing `1h` with `15m`).
   - Rejection of out-of-order or duplicate timestamps via M0 `validate_candles()`.
   - Rejection of unaligned candle timestamps.
3. **Data Quality & Gap Policy Enforcement (`test_strategy_gap_policy.py`):**
   - Behavior under `STRICT_GAP_FREE`: raises `StrategyDataGapError` on any missing interval.
   - Behavior under `REJECT_INTERNAL_GAPS`: permits valid history with leading/trailing gaps outside evaluated sequence, but strictly rejects internal gaps.
   - Verification that no forward-filling or interpolation ever takes place.
4. **Causality & Prefix-Invariance (`test_strategy_causality.py`):**
   - **Prefix-Invariance Test:** For history sequence $C_0, \dots, C_T$ followed by future candles $C_{T+1}, \dots, C_{T+k}$, prove that `evaluate(history[:T+1], context)` produces the exact same decision whether future candles exist, are truncated, or have modified prices.
   - Prove that decision at $T$ is mathematically independent of all information where $t \ge T_{\text{close}}$.
5. **Idempotency & Isolation (`test_strategy_determinism.py`):**
   - Run `evaluate()` 1,000 times consecutively on identical inputs. Prove output decisions, timestamps, reason codes, and indicator floats match perfectly within defined tolerances.
   - Run parallel evaluations across multiple threads/processes. Assert zero cross-contamination.
6. **Baseline Migration Golden Tests (`test_baseline_parity.py`):**
   - Execute baseline strategy against the owner-provided golden reference dataset.
   - Assert 100% discrete parity and floating-point tolerance conformity across all emitted decisions.

---

## 10. Strict Milestone Exclusions

To maintain focus and avoid architectural bloat, the following capabilities are explicitly excluded from M2:

- **Simulation & Backtesting Loops:** Processing sequences of signals over time with trade accounting belongs to M3.
- **Portfolio & Wallet Management:** Tracking cash, margin, capital allocation, exposure percentages, and position sizing belongs to M3/M4.
- **Execution & Exchange Integration:** Interacting with exchanges, submitting orders, or routing trades belongs to M4+.
- **Realistic Execution Models:** Simulating fills (e.g. Next-Bar Open), maker/taker fees, slippage, orderbook depth, and latency belongs to M3.
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
| **Lookahead Bias Leakage** | High (Invalidates research results) | Mathematically locked causality: `decision_time == source_candle_close_time`. Pre-flight evaluator enforces strict sequential timestamps and prefix invariance. |
| **Silent Gap Corruption** | Medium (Flawed indicator math) | M1 `DataQualityReport` reused; `GapPolicy` strictly enforced; unaligned candles rejected; silent forward-filling prohibited. |
| **Float Discrepancies Across Platforms** | Low (Minor parity mismatch) | Exact equality on discrete fields; documented relative/absolute tolerances (`rel_tol=1e-9`, `abs_tol=1e-12`) on floating-point metadata. |

---

### 11.2 Open Decisions for Technical Authority & Project Owner

The following items are presented for review and approval:

1. **Open Decision 1: Owner Supply of Baseline Specification (CRITICAL BLOCKER)**
   - *Question:* When and how will the legacy baseline algorithm source, parameter defaults, and golden reference fixtures be provided?
   - *Options:*
     - A: Project Owner commits a specification document and reference CSV fixture to the repository.
     - B: Project Owner designates an approved new baseline strategy (e.g. canonical SMA crossover) as the new reference standard.
2. **Open Decision 2: Batch Vectorized Evaluation in Milestone M3**
   - *Question:* Should M3 introduce an optimized vectorized evaluation method (`evaluate_series()`) that computes rolling indicators over NumPy arrays for fast backtesting?
   - *Recommendation:* Keep `Strategy.evaluate(history, context)` as the single canonical single-candle contract in M2. Batch vectorization should be explored in M3 as an optimization, verified for 100% parity against the M2 point-in-time evaluator.

*(Note: Previous Open Decision 3 regarding warmup behavior has been resolved: canonical behavior is established as raising `InsufficientHistoryError`.)*

---

## 12. M2 Completion Criteria

Milestone M2 will be considered complete and ready for merge into `main` when:

1. The `Strategy` protocol, `StrategyConfig`, `EvaluationContext`, `ResearchSignal`, and `StrategyDecision` contracts are implemented under `src/open_quant_intelligence/strategy/`.
2. The `StrategyEvaluator` is implemented, enforcing M0 `validate_candles()`, M1 quality reports, gap policies, and point-in-time causality.
3. The Project Owner supplies the baseline specification and reference fixtures.
4. The baseline strategy is implemented and verified against the golden reference test suite with 100% discrete parity and indicator tolerance conformity.
5. All offline unit tests pass cleanly across Python 3.11 and 3.12 on both Ubuntu and Windows CI runners.
6. `ruff check` and `ruff format` report zero errors.
7. Technical Authority (ChatGPT) and Project Owner grant formal written approval.
8. No M3 simulation, execution, or portfolio code is introduced.
