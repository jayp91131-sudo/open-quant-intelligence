# Open Quant Intelligence V2 — Milestone M1 Architecture Proposal
## Public Market Data Ingestion and Canonical Storage Engine (Final Candidate — Awaiting Technical Approval)

**Document Reference:** `docs/M1_ARCHITECTURE_PROPOSAL.md`  
**Milestone:** M1 — Public Market Data Ingestion & Storage  
**Base Commit on `main`:** `02b7bcad43a2947c7ed2977e143e959559f5873d`  
**Branch:** `docs/m1-architecture-proposal`  
**Author:** AntiGravity (Implementation Engineer)  
**Technical Authority:** ChatGPT  
**Final Decision Maker:** Project Owner  
**Status:** FINAL CANDIDATE — AWAITING TECHNICAL APPROVAL (Implementation is strictly NOT authorized)  

---

## 1. Executive Summary & Purpose

Milestone M1 establishes the foundation for reproducible quantitative research in Open Quant Intelligence (OQI) by delivering an automated, deterministic historical market data ingestion and storage pipeline.

The pipeline will:
1. Validate Spot market tradability, listing status, base asset, and quote asset against Binance exchange metadata via a dedicated allowlisted method `fetch_spot_symbol_metadata(symbol)` before initiating ingestion, immediately rejecting unsupported, delisted, halted, or non-tradable pairs.
2. Enforce an unextrapolated exchange server-time preflight check requiring that the requested window upper boundary satisfies $until\_ms \le fresh\_server\_time\_ms$. If $until\_ms$ lies in the future, ingestion aborts immediately with `FutureWindowError` prior to issuing any Kline requests, preventing premature classification of future intervals as missing data.
3. Collect public spot OHLCV data from Binance's dedicated public market data cluster (`https://data-api.binance.vision`) using CCXT without authentication or credentials, mediated through a strict, allowlisted public adapter `PublicExchangeAdapter` that encapsulates CCXT privately, captures response headers for rate-limit monitoring, and explicitly purges all private, SAPI, futures, options, portfolio-margin, and WebSocket endpoint groups.
4. Execute strict pagination ordering: inspect raw timestamp types and reject unparseable or non-integer timestamps; discard valid rows where $open\_time \ge until\_ms$ or $open\_time < since\_ms$ before field validation; and ensure every non-terminal iteration achieves strictly positive cursor advancement or raises a structured error.
5. Prevent permanent omission of incomplete in-range tail candles: discard the original forming OHLCV payload without validating prices or volume, preserve the exact cursor, query a genuinely fresh exchange-time sample on every retry, wait for finalization, and explicitly re-fetch fresh OHLCV from that cursor. Apply full validation exclusively to the newly fetched row. If retries exhaust, abort safely without committing a truncated dataset.
6. Provide full requested-window coverage analysis: detect leading gaps (before first candle), internal gaps (between candles), and trailing gaps (after last candle), enforcing that every admitted candle aligns strictly to the requested timeframe grid: $(candle\_open\_ms - since\_ms) \pmod{\Delta} == 0$. Compute $missing\_intervals = expected\_records - record\_count$.
7. Provide an explicit zero-candle branch for legitimate EMPTY ranges that validates market identity and window boundaries, computes a canonical header-only content SHA-256 digest, and records an immutable committed EMPTY catalog entry with nullable artifact fields, without routing empty sequences into M0's sequence validator (which expects at least one candle).
8. Store validated datasets in Apache Parquet files using Snappy compression via a complete, 8-state crash-recovery protocol (`INITIALIZING → STAGING → STAGED → PUBLISHING → PUBLISHED → COMMITTING_CATALOG → COMMITTED → CLEANUP`) adhering strictly to cross-platform no-clobber invariants. Pre-existing target files with mismatched hashes are never automatically quarantined, moved, or deleted; collisions raise structured `RecoveryConflictError` / `DatasetPathCollisionError` and preserve the destination byte-for-byte unchanged.
9. Maintain an immutable dataset snapshot catalog in DuckDB accompanied by a dedicated `dataset_verification_history` audit table, cleanly separating immutable snapshot identities from verification audit events.
10. Provide user-facing CLI commands for downloading, cataloging, inspecting, and re-validating historical datasets.

---

## 2. Invariant Contracts & Strict Exclusions

### A. Preserved M0 & M0.1 Contracts
- **Python Compatibility:** Python 3.11 and 3.12 exclusively.
- **Repository Structure:** Standard `src/` layout under `src/open_quant_intelligence/`.
- **Domain Invariants:** Immutable `Candle`, `MarketIdentity`, and `SupportedTimeframe` classes remain the single source of truth.
- **Time Semantics:** Half-open interval convention `[open_time, close_time)` where `close_time = open_time + duration`.
- **Supported Timeframes:** Strictly `SupportedTimeframe.M1` ("1m"), `M5` ("5m"), `M15` ("15m"), `M30` ("30m"), `H1` ("1h"), `H4` ("4h"), and `D1` ("1d").
- **Data Integrity:** No silent auto-sorting, no forward-filling of gaps, and no row fabrication.
- **Test Integrity:** The existing 104-test M0/M0.1 unit and CLI test suite must continue passing without modification.

### B. Strict Milestone Exclusions
- **No Private API Keys or Credentials:** Ingestion uses public market data endpoints exclusively.
- **No Raw CCXT Exposure:** Raw CCXT exchange objects are never leaked outside the internal adapter boundary; no generic method forwarding is permitted.
- **No Extraneous Endpoints:** SAPI, futures (fapi), delivery (dapi), options (eapi), portfolio-margin (papi), and WebSockets are completely purged.
- **No Order Execution or Trading:** Zero routing, balancing, order placement, or wallet manipulation.
- **No Derivatives:** Strictly spot markets (`market_type == "spot"`). Futures, options, and perpetuals are out of scope.
- **No Strategy Logic or Backtesting:** Strategy evaluation, alpha generation, and portfolio simulation belong to M2/M3.
- **No AI/ML:** Model inference and feature training belong to M6.
- **No Real-Time Streaming:** WebSockets and orderbook tickers are excluded. M1 is historical batch OHLCV only.

---

## 3. Technology Stack & Dependency Verification

### A. Evaluation of Dependencies

| Component | Library Choice | Justification |
|---|---|---|
| **Market Data Adapter** | `ccxt>=4.4.52, <5.0.0` | Industry-standard exchange abstraction. Encapsulated behind `PublicExchangeAdapter`. Deep-merged endpoint URLs are explicitly sanitized to retain only the required Vision Spot public operations. |
| **Analytical Catalog & Query** | `duckdb>=1.1.0, <2.0.0` | Embedded in-process analytical SQL engine. Handles metadata registration, analytical queries across local Parquet files, and relational catalog queries without requiring a client-server database daemon. |
| **Parquet Storage Engine** | Native DuckDB Parquet Engine (Primary) | DuckDB compiles native, highly performant Parquet reading and writing (`COPY ... TO ... (FORMAT PARQUET, COMPRESSION SNAPPY)`). Using DuckDB for Parquet eliminates heavy runtime coupling to PyArrow for ingestion. |
| **Optional Arrow Interop** | `pyarrow>=15.0.0, <19.0.0` (Optional `[dev]`) | Maintained as an optional development dependency for zero-copy memory testing and schema inspection, but **NOT** required for the production runtime pipeline. |

### B. CI Target Versions vs. Supported Bounds
The existing 104 tests in the repository verify M0 domain and CLI contracts; they do not exercise CCXT or DuckDB because M1 dependencies are not yet installed in the current environment. When M1 implementation begins, CI will explicitly test against pinned target versions while declaring semantic upper and lower bounds:

| Dependency | Supported Version Bounds in `pyproject.toml` | Target Pinned Version in M1 Implementation CI | Role |
|---|---|---|---|
| `ccxt` | `>=4.4.52, <5.0.0` | `4.4.52` | Production runtime |
| `duckdb` | `>=1.1.0, <2.0.0` | `1.1.3` | Production runtime |
| `pytest` | `>=8.0.0` | `8.3.3` | Test framework (`[dev]`) |
| `ruff` | `>=0.4.0` | `0.7.3` | Linter / formatter (`[dev]`) |
| `pyarrow` | `>=15.0.0, <19.0.0` | `18.0.0` | Optional verification (`[dev]`) |

```toml
[project]
dependencies = [
    "ccxt>=4.4.52,<5.0.0",
    "duckdb>=1.1.0,<2.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "ruff>=0.4.0",
    "pyarrow>=15.0.0,<19.0.0",
]
```

---

## 4. System Architecture & Module Boundaries

### A. Modular Structure
```text
src/open_quant_intelligence/
├── domain/                      # [M0] Immutable contracts (Candle, MarketIdentity, SupportedTimeframe)
│   ├── candle.py
│   ├── market.py
│   └── timeframe.py
├── data/                        # [M0] Validation engine
│   ├── validation.py            # [M0] validate_candles() canonical sequence & Candle validator
│   └── quality.py               # [M1 NEW] Gap analysis & data quality reporting
├── ingestion/                   # [M1 NEW] Public ingestion pipeline
│   ├── __init__.py
│   ├── adapter.py               # Strict allowlisted Binance public vision adapter (encapsulates CCXT)
│   ├── preflight.py             # Pre-flight Spot market metadata & future-window verification
│   ├── pagination.py            # Strict paginator with malformed-row rejection and tail re-fetch
│   ├── clock.py                 # Fresh exchange server time observation & ServerTimeObserver protocol
│   └── pipeline.py              # Orchestrator: Pre-Flight -> Fetch -> Edge Gap Analysis -> Store
├── storage/                     # [M1 NEW] Persistence & Catalog
│   ├── __init__.py
│   ├── manifest.py              # Durable manifest manager, 8-state machine transitions, active leases
│   ├── parquet_store.py         # Staging flush, Win32/POSIX sync barriers, atomic no-clobber publication
│   ├── fingerprint.py           # Canonical binary content hashing (data & empty sets) & artifact SHA-256
│   └── catalog.py               # DuckDB immutable snapshot catalog, edge gap recording & verification history
└── cli.py                       # CLI entrypoints (validate, ingest, datasets, inspect, verify)
```

### B. Mermaid Architecture Diagram
```mermaid
flowchart TD
    subgraph Binance_Vision["Binance Public Vision Cluster"]
        ExchangeInfo["GET /api/v3/exchangeInfo (Market Metadata & Rate Limits)"]
        TimeAPI["GET /api/v3/time (Fresh Server Time Samples)"]
        API["GET /api/v3/klines (Public Spot Kline Batches)"]
    end

    subgraph Ingestion_Adapter["Adapter Boundary (src/open_quant_intelligence/ingestion/)"]
        Adapter["PublicExchangeAdapter (Strict Interface, Sanitized CCXT 4.4.52)"]
        MarketPreflight["Market Metadata Preflight (fetch_spot_symbol_metadata)"]
        WindowPreflight["Future Window Preflight (until_ms <= server_time_ms)"]
        ClockSync["Server Time Observer (Fresh Sample Protocol)"]
        Paginator["Strict Paginator (Timestamp Inspection, In-Range Filtering)"]
        TailRefetch["Incomplete Tail Guard (Preserve Cursor, Wait Finalization, Re-fetch Fresh OHLCV)"]
    end

    subgraph Coverage_Validation["Coverage & Domain Validation (src/open_quant_intelligence/data/)"]
        EdgeGapAnalysis["Edge & Full Window Coverage Analysis (Leading, Internal, Trailing Gaps)"]
        GridAlignmentCheck["Grid Alignment Check: (open_ms - since_ms) % step_ms == 0"]
        Decision{"Any Admitted Candles?"}
        
        subgraph Zero_Branch["Zero-Candle Branch (Legitimate EMPTY)"]
            EmptyDigest["Compute Canonical Header-Only Content SHA-256"]
            EmptyCatalog["Record Committed EMPTY Snapshot (file_path=NULL)"]
        end
        
        subgraph Non_Zero_Branch["Non-Zero Candle Branch"]
            M0Validator["M0 validate_candles() Domain & Sequence Validation"]
            QualityCheck["Verify: record_count + missing_intervals == expected_records"]
            DataDigest["Compute Canonical Binary Content SHA-256"]
            Staging["Parquet Staging Write + Row Count Check (rows == record_count)"]
            Publication["Atomic No-Clobber Publication + Platform Sync Barriers"]
            DataCatalog["Record Committed Snapshot + Gaps in DuckDB"]
        end
    end

    subgraph Storage_Catalog["Storage & Catalog Layer (src/open_quant_intelligence/storage/)"]
        ManifestManager["Durable Manifest 8-State Transitions (.ingest_manifest.json)"]
        DataFiles["Parquet Datasets: data/datasets/.../*.parquet"]
        DuckDBCatalog["DuckDB Catalog (data/catalog.duckdb: datasets, dataset_gaps, verification_history)"]
        Quarantine["Quarantine Directory: data/quarantine/ (Positive-proof / Owner approved only)"]
    end

    ExchangeInfo -->|Rate Limits & Symbols| Adapter
    Adapter --> MarketPreflight
    MarketPreflight -->|Non-Tradable / Delisted| HaltPre["Abort with MarketStatusError"]
    MarketPreflight -->|Valid Spot Symbol| WindowPreflight
    TimeAPI -->|Server Time Samples| Adapter
    Adapter --> ClockSync
    ClockSync --> WindowPreflight
    WindowPreflight -->|until_ms > server_time_ms| HaltFuture["Abort with FutureWindowError (0 Kline requests)"]
    WindowPreflight -->|Historical Window Valid| Paginator
    API -->|Raw Kline Batches| Adapter
    Adapter -->|Raw Pages| Paginator
    Paginator -->|Malformed / Regression| HaltPag["Abort with MalformedPageError / PaginationRegressionError"]
    Paginator --> TailRefetch
    TailRefetch -->|Re-fetch Fresh OHLCV from Cursor| Adapter
    TailRefetch -->|Retries Exhausted| AbortTail["Abort with IncompleteTailCandleError"]
    TailRefetch -->|Finalized Raw Candles| EdgeGapAnalysis
    EdgeGapAnalysis --> GridAlignmentCheck
    GridAlignmentCheck --> Decision
    
    Decision -->|Zero Candles| EmptyDigest
    EmptyDigest --> EmptyCatalog
    EmptyCatalog --> DuckDBCatalog
    
    Decision -->|>= 1 Candles| M0Validator
    M0Validator --> QualityCheck
    QualityCheck --> DataDigest
    DataDigest --> ManifestManager
    ManifestManager --> Staging
    Staging --> Publication
    Publication -->|Target Collision| HaltCollision["Abort with RecoveryConflictError (Target Unmodified)"]
    Publication --> DataFiles
    Publication --> DataCatalog
    DataCatalog --> DuckDBCatalog
```

---

## 5. PublicExchangeAdapter Contract & Binance Vision Configuration

### A. Strict Allowlisted Adapter Interface
To prevent architectural leaks, raw CCXT objects and arbitrary methods are strictly encapsulated. The ingestion engine interacts solely through a narrow, allowlisted interface:

```python
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class SpotSymbolMetadata:
    """Standardized metadata for Spot market validation."""

    symbol: str
    status: str
    is_spot_trading_allowed: bool
    permissions: tuple[str, ...]
    base_asset: str
    quote_asset: str


class RawKlineRow(list):
    """Raw Kline tuple [open_time, open, high, low, close, volume]."""


class PublicExchangeAdapter(Protocol):
    """Strict allowlisted public market data interface encapsulating CCXT."""

    def fetch_spot_symbol_metadata(self, symbol: str) -> SpotSymbolMetadata:
        """Fetch Spot market metadata from Binance exchangeInfo.

        Args:
            symbol: Normalized market symbol (e.g. 'BTC/USDT').

        Returns:
            SpotSymbolMetadata with symbol status, spot eligibility, base, and quote assets.

        Raises:
            MarketNotFoundError: If symbol does not exist in exchangeInfo.
            PublicAdapterNetworkError: On network or transport failures.
        """
        ...

    def fetch_ohlcv(self, symbol: str, timeframe: str, since_ms: int, limit: int = 1000) -> list[RawKlineRow]:
        """Fetch raw public OHLCV candles from Binance vision cluster.

        Captures response headers internally (X-MBX-USED-WEIGHT-1M, Retry-After).
        """
        ...

    def fetch_time(self) -> int:
        """Fetch public Binance server timestamp in milliseconds."""
        ...

    def get_rate_limit_weight_limit(self) -> int:
        """Return maximum IP request weight per minute from exchangeInfo."""
        ...

    def get_last_used_weight_1m(self) -> int:
        """Return the latest recorded X-MBX-USED-WEIGHT-1M header value."""
        ...
```

### B. CCXT 4.4.52 URL Sanitization & Header Capture
1. **URL Sanitization:** Upon initialization, the adapter deletes or purges `sapi`, `fapi`, `dapi`, `eapi`, `papi`, `ws`, and the obsolete `v1` path (which maps to `/api/v1` in CCXT 4.4.52, unnecessary for M1). It sets:
   ```python
   self._exchange.urls["api"] = {
       "public": "https://data-api.binance.vision/api/v3",
   }
   ```
2. **Internal Response Header Capture:**
   CCXT records response headers on the exchange instance (`self._exchange.last_response_headers`). After each HTTP request, the adapter captures:
   - `X-MBX-USED-WEIGHT-1M`: Parsed as an integer to update the current 1-minute window weight usage.
   - `Retry-After`: Parsed as an integer (seconds) when HTTP 429 is encountered.
3. **No Generic Method Forwarding:**
   The adapter defines explicit methods only. No `__getattr__` or `__getattribute__` forwarding exists; client code cannot reach private or unapproved CCXT methods.

### C. Rate-Limit Recovery & Backoff Policy
1. **Dynamic Limit Retrieval:** At initialization, the adapter parses `exchangeInfo.rateLimits` for `rateLimitType == "REQUEST_WEIGHT"` and `interval == "MINUTE"`. The `limit` (e.g. 1,200 or 6,000) is cached. `fetch_exchange_status()` is **not** used.
2. **Proactive Throttling via `X-MBX-USED-WEIGHT-1M`:**
   - Used weight $> 70\%$: Injects adaptive sleep $\text{sleep\_ms} = (\text{used} / \text{limit})^2 \times 500\,\text{ms}$.
   - Used weight $> 90\%$: Pauses until the 60-second window elapses.
3. **HTTP 429 Rate Limit Handling:**
   - Reads `Retry-After` header. If present, sleeps for the specified seconds $+ 500\,\text{ms}$ jitter.
   - If `Retry-After` is absent, falls back to bounded exponential backoff: $T = \min(60, 2^{\text{attempt}} \times 1.5) + \text{jitter}$.
   - Maximum 3 retries. If exhausted, aborts immediately with `RateLimitExhaustedError`. Ingestion terminates safely; pagination does not continue.
4. **HTTP 418 IP Ban:**
   - If HTTP 418 is received, **HALT IMMEDIATELY WITH ZERO RETRIES**. Log a `CRITICAL` alert and write an abort record in the manifest.

---

## 6. Pre-Flight Window Validation & Future Window Rejection

### A. Strict Request Window Validation
M1 ingests completed historical data only. Before making any Kline requests, the pipeline validates window ordering, timeframe grid alignment, and rejects future boundaries:

```python
def validate_request_window_preflight(
    since_ms: int,
    until_ms: int,
    timeframe: SupportedTimeframe,
    adapter: PublicExchangeAdapter,
) -> int:
    """Validate window ordering, grid alignment, and historical completion.
    
    Raises:
        InvalidWindowError: If until_ms <= since_ms.
        UnalignedBoundaryError: If boundaries are not grid-aligned.
        FutureWindowError: If until_ms is in the future relative to server time.
    """
    if until_ms <= since_ms:
        raise InvalidWindowError(
            f"Invalid query window: until_ms ({until_ms}) must be strictly greater than since_ms ({since_ms})."
        )
    
    step_ms = timeframe_to_duration_ms(timeframe)
    if since_ms % step_ms != 0:
        raise UnalignedBoundaryError(
            f"Requested since_ms ({since_ms}) is not aligned to {timeframe.value} grid ({step_ms}ms)."
        )
    if until_ms % step_ms != 0:
        raise UnalignedBoundaryError(
            f"Requested until_ms ({until_ms}) is not aligned to {timeframe.value} grid ({step_ms}ms)."
        )
    
    # Fresh server time preflight check
    fresh_server_time_ms = adapter.fetch_time()
    if until_ms > fresh_server_time_ms:
        raise FutureWindowError(
            f"Requested until boundary ({until_ms}ms) is in the future relative to "
            f"exchange server time ({fresh_server_time_ms}ms). M1 ingests completed historical data only."
        )
        
    return (until_ms - since_ms) // step_ms
```

### B. Prevention of Misleading Missing Candles
Rejecting future windows prior to pagination ensures the pipeline never requests incomplete future intervals, never misclassifies future intervals as missing data, and never commits misleading `GAP_CONTAINING` snapshots for time windows that have not yet elapsed on the exchange. Zero Kline requests are issued if the future check fails.

---

## 7. Complete Pagination Algorithm & Incomplete Tail Re-Fetch Policy

### A. ServerTimeObserver Protocol
The observation layer defines a strict protocol ensuring fresh server time is fetched without stale cached assumptions during retries:

```python
class ServerTimeObserver(Protocol):
    """Protocol for observing unextrapolated Binance exchange server time."""

    def get_fresh_server_time_ms(self, force_refresh: bool = False) -> int:
        """Return the exchange server time in milliseconds.
        
        Args:
            force_refresh: If True, bypasses any short-term cache and issues
                           an actual GET /api/v3/time HTTP request immediately.
        """
        ...
```

### B. Complete Helper Behavior: `validate_raw_kline_fields`
```python
def validate_raw_kline_fields(row: RawKlineRow) -> None:
    """Validate numeric, price, and volume invariants of an admitted Kline row.
    
    Raises:
        MalformedKlineError: If fields are non-numeric, non-finite, or violate bounds.
    """
    if not isinstance(row, (list, tuple)) or len(row) != 6:
        raise MalformedKlineError(f"Row is not a 6-element sequence: {row!r}")
    t, o, h, l, c, v = row
    if not isinstance(t, int) or isinstance(t, bool) or t < 0:
        raise MalformedKlineError(f"Row timestamp {t!r} is not a valid epoch millisecond integer.")
    for val_name, val in [("open", o), ("high", h), ("low", l), ("close", c), ("volume", v)]:
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            raise MalformedKlineError(f"Field {val_name} ({val!r}) must be numeric int or float.")
        if not math.isfinite(val):
            raise MalformedKlineError(f"Field {val_name} ({val!r}) must be finite.")
    if min(o, h, l, c) <= 0:
        raise MalformedKlineError(f"Prices must be strictly positive: open={o}, high={h}, low={l}, close={c}")
    if v < 0:
        raise MalformedKlineError(f"Volume must be non-negative: volume={v}")
```

### C. Complete Pagination Algorithm Specification
```python
def paginate_ohlcv(
    adapter: PublicExchangeAdapter,
    symbol: str,
    timeframe: SupportedTimeframe,
    since_ms: int,
    until_ms: int,
    clock: ServerTimeObserver,
) -> list[RawKlineRow]:
    """Deterministically paginate historical OHLCV from Binance vision public endpoints."""
    step_ms = timeframe_to_duration_ms(timeframe)
    expected_count = (until_ms - since_ms) // step_ms
    max_pages = max(50, ((expected_count + 999) // 1000) * 3)

    current_since_ms = since_ms
    admitted_candles: list[RawKlineRow] = []
    seen_timestamps: set[int] = set()
    page_count = 0

    while current_since_ms < until_ms:
        page_count += 1
        if page_count > max_pages:
            raise PaginationBudgetExceededError(f"Pagination exceeded safety budget of {max_pages} requests.")

        # Request batch from exchange (max 1000 klines)
        page = adapter.fetch_ohlcv(symbol, timeframe.value, since_ms=current_since_ms, limit=1000)
        
        # 1. Empty page: Exchange has no further data in range
        if not page:
            break

        # 2. Strict Raw Row Structure & Timestamp Validation
        # Do not silently skip malformed rows. Every row MUST have a valid integer timestamp.
        page_timestamps: list[int] = []
        for i, row in enumerate(page):
            if not isinstance(row, (list, tuple)) or len(row) < 1:
                raise MalformedPageError(f"Row {i} is not a valid sequence: {row!r}")
            t = row[0]
            if not isinstance(t, int) or isinstance(t, bool) or t < 0:
                raise MalformedPageError(f"Row {i} timestamp {t!r} is not a valid integer millisecond timestamp.")
            if page_timestamps and t <= page_timestamps[-1]:
                raise MalformedPageError(f"Page timestamps not strictly increasing: {page_timestamps[-1]} -> {t}")
            page_timestamps.append(t)

        first_raw_t = page_timestamps[0]
        max_raw_t = page_timestamps[-1]

        # 3. Non-Advancing & Regression Checks
        if max_raw_t < current_since_ms:
            raise PaginationRegressionError(
                f"Page regression: max timestamp {max_raw_t}ms is before cursor {current_since_ms}ms"
            )

        # 4. Extract In-Range Rows
        # Out-of-range rows (t < since_ms or t >= until_ms) are discarded without validating OHLCV fields.
        in_range_rows: list[RawKlineRow] = [row for row in page if since_ms <= row[0] < until_ms]

        # Terminal condition: All returned rows are beyond requested range
        if not in_range_rows and first_raw_t >= until_ms:
            break

        # If page had valid timestamps but none in range (e.g. gap spanning past cursor):
        if not in_range_rows:
            next_cursor = max_raw_t + 1
            if next_cursor <= current_since_ms:
                raise PaginationRegressionError(f"Pagination stalled with no forward progress at {current_since_ms}ms")
            current_since_ms = next_cursor
            continue

        # 5. Process In-Range Rows and Guard Incomplete Tail
        obs_server_time_ms = clock.get_fresh_server_time_ms(force_refresh=False)

        for raw_kline in in_range_rows:
            open_ms = raw_kline[0]
            close_ms = open_ms + step_ms

            if open_ms in seen_timestamps:
                raise PageOverlapError(f"Timestamp {open_ms} was already admitted in a previous page.")

            # Check if candle is incomplete / forming at observation time
            if close_ms > obs_server_time_ms:
                # Incomplete tail candle detected!
                # Discard original incomplete OHLCV values WITHOUT validating them.
                # Preserve cursor: do not advance past it.
                finalized = False
                for attempt in range(3):  # MAX_TAIL_RETRIES = 3
                    time.sleep(2)
                    # Genuinely fresh server-time request on every retry
                    fresh_server_time_ms = clock.get_fresh_server_time_ms(force_refresh=True)
                    if close_ms <= fresh_server_time_ms:
                        # Re-fetch fresh OHLCV from the exact cursor to obtain finalized values
                        refetched = adapter.fetch_ohlcv(symbol, timeframe.value, since_ms=open_ms, limit=1)
                        if not refetched or refetched[0][0] != open_ms:
                            raise IncompleteTailCandleError(
                                f"Re-fetched candle at {open_ms}ms returned missing or mismatched timestamp."
                            )
                        refetched_row = refetched[0]
                        # Fully validate newly fetched row
                        validate_raw_kline_fields(refetched_row)
                        admitted_candles.append(refetched_row)
                        seen_timestamps.add(open_ms)
                        current_since_ms = open_ms + 1
                        finalized = True
                        break

                if not finalized:
                    raise IncompleteTailCandleError(
                        f"Candle at open_time {open_ms}ms remained forming after retries. Aborting ingestion."
                    )
            else:
                # Candle is fully closed: validate numeric OHLCV fields and admit
                validate_raw_kline_fields(raw_kline)
                admitted_candles.append(raw_kline)
                seen_timestamps.add(open_ms)
                current_since_ms = open_ms + 1

        # Advance cursor past page maximum if all rows in page were processed
        next_cursor = max_raw_t + 1
        if next_cursor > current_since_ms:
            current_since_ms = next_cursor

    return admitted_candles
```

---

## 8. Full Requested-Window Coverage & Edge-Gap Analysis

### A. Leading, Internal, and Trailing Gap Accounting
M0's `validate_candles()` verifies internal continuity between consecutive candles. M1 adds comprehensive **full requested-window coverage analysis** to detect edge gaps:

1. **Requested Window Boundaries:** $[\text{since\_ms}, \text{until\_ms})$ with interval step $\Delta_{\text{ms}}$.
2. **Grid Alignment Invariant:** For every admitted candle $i$:
   $$(\text{candle\_open\_ms}[i] - \text{since\_ms}) \pmod{\Delta_{\text{ms}}} == 0$$
   If any candle is not aligned to the requested grid, ingestion aborts with `UnalignedCandleError`.
3. **Leading Gap Detection:**
   If $\text{first\_candle\_open\_ms} > \text{since\_ms}$:
   - A leading gap exists from `since_ms` to `first_candle_open_ms`.
   - Missing leading intervals: $N_{\text{leading}} = (\text{first\_candle\_open\_ms} - \text{since\_ms}) // \Delta_{\text{ms}}$.
4. **Internal Gap Detection:**
   Between consecutive candles $i$ and $i+1$:
   - If $\text{open}[i+1] > \text{close}[i]$, an internal gap exists with missing count $(\text{open}[i+1] - \text{close}[i]) // \Delta_{\text{ms}}$.
5. **Trailing Gap Detection:**
   If $\text{last\_candle\_close\_ms} < \text{until\_ms}$:
   - A trailing gap exists from `last_candle_close_ms` to `until_ms`.
   - Missing trailing intervals: $N_{\text{trailing}} = (\text{until\_ms} - \text{last\_candle\_close\_ms}) // \Delta_{\text{ms}}$.
6. **Coverage Mathematical Invariant:**
   $$\text{missing\_intervals} = N_{\text{leading}} + \sum N_{\text{internal}} + N_{\text{trailing}} = \text{expected\_records} - \text{record\_count}$$
   If `record_count + missing_intervals != expected_records`, ingestion aborts with `InconsistentGapStatisticsError`.

### B. Revised `dataset_gaps` Schema Supporting Trailing Gaps
To naturally represent trailing gaps where no subsequent candle exists, `subsequent_open` is made nullable:

```sql
CREATE TABLE IF NOT EXISTS dataset_gaps (
    gap_id VARCHAR PRIMARY KEY,
    snapshot_id VARCHAR REFERENCES datasets(snapshot_id),
    gap_type VARCHAR NOT NULL,                   -- 'LEADING', 'INTERNAL', 'TRAILING'
    expected_open TIMESTAMP WITH TIME ZONE NOT NULL,
    subsequent_open TIMESTAMP WITH TIME ZONE,    -- NULL for TRAILING gaps
    missing_count BIGINT NOT NULL
);
```

### C. Distinction Between EMPTY and Non-Empty Datasets with Gaps
- **EMPTY Datasets:** Record count is exactly 0. No Parquet file is produced. Missing intervals equals `expected_records`. One comprehensive gap event of type `'FULL_WINDOW'` is recorded.
- **Non-Empty Datasets with Gaps (`GAP_CONTAINING`):** Record count is $> 0$. A valid physical Parquet file is published containing exactly `record_count` rows. Distinct `LEADING`, `INTERNAL`, and `TRAILING` gap events are cataloged.

---

## 9. Canonical Binary Content Fingerprinting & Negative-Zero Normalization

To guarantee deterministic data integrity across platforms, compilers, and DuckDB versions, M1 establishes **exactly one canonical serialization contract**.

### A. Negative-Zero Normalization
In IEEE-754 arithmetic, $-0.0$ and $+0.0$ have different bit representations (`0x8000000000000000` vs `0x0000000000000000`). All float values are normalized before serialization:
```python
def normalize_float(val: float) -> float:
    return 0.0 if val == 0.0 else val
```

### B. Canonical Binary Serialization Specification
```text
[Header Block]
  - Magic Bytes:          ASCII "OQICCF" (6 bytes)
  - Schema Version:       UINT16_LE (2 bytes, value = 1)
  - Exchange ID:          UINT16_LE length + UTF-8 bytes (e.g. 7, "binance")
  - Market Type:          UINT16_LE length + UTF-8 bytes (e.g. 4, "spot")
  - Symbol:               UINT16_LE length + UTF-8 bytes (e.g. 8, "BTC/USDT")
  - Timeframe:            UINT16_LE length + UTF-8 bytes (e.g. 2, "1h")
  - Requested Since ms:   INT64_LE (8 bytes)
  - Requested Until ms:   INT64_LE (8 bytes)
  - Candle Count:         UINT64_LE (8 bytes, 0 for EMPTY datasets)

[Candle Record Block] (Included if Candle Count > 0, ordered strictly by open_time ascending)
  - Open Time Epoch ms:   INT64_LE (8 bytes)
  - Close Time Epoch ms:  INT64_LE (8 bytes)
  - Open Price:           Normalized IEEE-754 binary64 DOUBLE_LE (8 bytes)
  - High Price:           Normalized IEEE-754 binary64 DOUBLE_LE (8 bytes)
  - Low Price:            Normalized IEEE-754 binary64 DOUBLE_LE (8 bytes)
  - Close Price:          Normalized IEEE-754 binary64 DOUBLE_LE (8 bytes)
  - Base Volume:          Normalized IEEE-754 binary64 DOUBLE_LE (8 bytes)
  - Quote Volume Present: UINT8 (1 byte: 0 = NULL, 1 = Present)
    [If Present]:         Normalized IEEE-754 binary64 DOUBLE_LE (8 bytes)
  - Trade Count Present:  UINT8 (1 byte: 0 = NULL, 1 = Present)
    [If Present]:         INT64_LE (8 bytes)
```

For EMPTY datasets, the digest is computed over `[Header Block]` with `Candle Count = 0`. Different empty identities produce distinct hashes; identical empty identities produce stable hashes.

---

## 10. Complete Storage Durability, Win32/POSIX APIs & Crash Recovery

### A. Windows Storage Durability Protocol (Win32 API Contracts)
1. **Staging File Creation & Flush:**
   - Staging files are written to `<target_path>.tmp_<uuid>`.
   - Flushed via Python `f.flush()` followed by `os.fsync(f.fileno())` (which invokes Win32 `FlushFileBuffers(handle)` internally) to commit data to non-volatile disk blocks.
2. **Same-Volume No-Clobber Publication:**
   - Staging and target reside on the same filesystem volume (`data/datasets/`).
   - Publication uses Win32 `MoveFileExW(staging_path, target_path, MOVEFILE_WRITE_THROUGH)`.
   - **Crucial Invariant:** `MOVEFILE_REPLACE_EXISTING` is **OMITTED**. If `target_path` already exists, `MoveFileExW` fails atomically with `GetLastError() == ERROR_ALREADY_EXISTS`, raising `FileExistsError` in Python without modifying the destination.
3. **Manifest Temporary Replacement:**
   - Manifest transitions write to `.ingest_manifest_<uuid>.json.tmp`, flushed via `FlushFileBuffers`.
   - Replaces manifest atomically: `MoveFileExW(tmp_path, manifest_path, MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`.
4. **Hardware Limitations:** Documented that consumer disk write caches may delay physical media writes unless battery-backed.

### B. POSIX Storage Durability Protocol
1. Staging file is flushed and committed via `os.fsync(fd)`.
2. Manifest updates write to `.tmp`, execute `os.fsync(fd)`, atomically replace via `os.replace()`, and call `os.fsync(dir_fd)` on the parent directory.
3. Publication executes `os.link(staging, target)` followed by `os.unlink(staging)`. If target exists, `os.link()` raises `FileExistsError` atomically.
4. Directory synchronization executes `os.fsync(dir_fd)` on the dataset parent directory.

### C. Removal of Unsafe Automatic Quarantine & Conflict Handling
- **Pre-Existing Target Files:** Pre-existing target files must **never** be automatically overwritten, deleted, replaced, or moved to quarantine merely because their hash differs from the current manifest.
- **Recovery Conflict Resolution:** If an existing target file is discovered during recovery:
  1. The reconciler checks whether the target file matches the manifest `file_sha256`.
  2. If the hash matches, the artifact was published by the current ingestion; reconciliation proceeds to commit the catalog.
  3. If the hash mismatches, it indicates an **unrelated pre-existing target file**. Reconciliation aborts immediately with structured `RecoveryConflictError("Pre-existing target artifact hash collision detected.")`.
  4. The pre-existing target file remains **byte-for-byte untouched**. Quarantine is permitted only with positive proof of ownership or via an explicit, owner-authorized CLI command (`open-quant datasets quarantine <id>`).

### D. Comprehensive Crash-Recovery Transition Table

$$\text{INITIALIZING} \to \text{STAGING} \to \text{STAGED} \to \text{PUBLISHING} \to \text{PUBLISHED} \to \text{COMMITTING\_CATALOG} \to \text{COMMITTED} \to \text{CLEANUP}$$

| Crash Point | Filesystem State | Catalog State | Manifest State | Startup Reconciliation Recovery Action |
|---|---|---|---|---|
| **1. Before initial manifest is durable** | Partial `.tmp` or no files | No record | No manifest | Orphan `.tmp` without manifest: checked against active process lockfile and age threshold ($>1\,\text{hour}$); if unowned and expired, deleted as garbage. |
| **2. After manifest temp flush, before replacement** | `.tmp` manifest exists; no main manifest | No record | No main manifest | Reconciler cleans up orphaned `.tmp` manifest and associated staging file. |
| **3. During staging write** | Incomplete staging `.tmp` file | No record | `INITIALIZING` or `STAGING` | Reconciler inspects manifest lease: if PID dead and lease expired, deletes staging `.tmp` and unlinks manifest. |
| **4. After staging verified, before publication** | Staging `.tmp` verified; target does not exist | No record | `STAGED` | Reconciler verifies staging `.tmp` hash. If valid, resumes publication $\to$ advances manifest $\to$ commits catalog. If invalid, deletes `.tmp` and manifest. |
| **5. Crash after publication link, before staging unlink** | Both staging `.tmp` and target `.parquet` exist | No record | `PUBLISHING` | Reconciler checks target `.parquet`: if hash matches manifest, unlinks redundant staging `.tmp`, advances manifest to `PUBLISHED`, and proceeds to catalog commit. If target hash mismatches, aborts with `RecoveryConflictError` without touching target. |
| **6. Crash after publication, before catalog commit** | Target `.parquet` exists; staging unlinked | No record | `PUBLISHED` or `COMMITTING_CATALOG` | Reconciler verifies target `.parquet` against manifest hash: if valid, executes DuckDB commit transaction $\to$ advances manifest to `COMMITTED`. If target hash mismatches, aborts with `RecoveryConflictError` (target preserved). |
| **7. After DuckDB commit, before manifest unlink** | Target `.parquet` exists | Committed record exists | `COMMITTED` or `COMMITTING_CATALOG` | Reconciler observes transaction already committed in DuckDB for `ingestion_uuid`; safely unlinks orphaned manifest. |
| **8. During cleanup (staging/manifest unlink)** | Target `.parquet` exists | Committed record exists | Residual manifest or `.tmp` | Reconciler verifies catalog commit, removes residual manifest or `.tmp`. Idempotent. |

---

## 11. Immutable Dataset Snapshots & Verification History

### A. Immutable Snapshot Catalog Schema (DDL)
```sql
CREATE TABLE IF NOT EXISTS datasets (
    snapshot_id VARCHAR PRIMARY KEY,             -- Unique snapshot ID (e.g. 'snap_f47ac10b8e21')
    dataset_identity VARCHAR NOT NULL,           -- Deterministic key: 'binance:spot:BTC/USDT:1h:since:until'
    exchange_id VARCHAR NOT NULL,                -- 'binance'
    market_type VARCHAR NOT NULL,                -- 'spot'
    symbol VARCHAR NOT NULL,                     -- 'BTC/USDT'
    timeframe VARCHAR NOT NULL,                  -- '1h'
    requested_since TIMESTAMP WITH TIME ZONE NOT NULL,
    requested_until TIMESTAMP WITH TIME ZONE NOT NULL,
    actual_first_open TIMESTAMP WITH TIME ZONE,  -- NULL if EMPTY
    actual_last_close TIMESTAMP WITH TIME ZONE,  -- NULL if EMPTY
    expected_records BIGINT NOT NULL,            -- Total theoretical grid intervals
    record_count BIGINT NOT NULL,                -- Actual records captured (0 if EMPTY)
    gap_count INTEGER NOT NULL,                  -- Number of gap events
    missing_intervals BIGINT NOT NULL,           -- Total missing intervals across all gaps
    content_fingerprint_sha256 VARCHAR NOT NULL, -- Canonical content hash (Header-only digest if EMPTY)
    file_sha256 VARCHAR,                         -- NULL for EMPTY datasets
    file_path VARCHAR,                           -- NULL for EMPTY datasets
    file_size_bytes BIGINT,                      -- NULL for EMPTY datasets
    ccxt_version VARCHAR NOT NULL,
    ingestion_uuid VARCHAR NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    ingestion_status VARCHAR NOT NULL,           -- 'COMMITTED', 'FAILED', 'ABORTED'
    quality_status VARCHAR NOT NULL              -- 'EMPTY', 'GAP_FREE', 'GAP_CONTAINING'
);

CREATE TABLE IF NOT EXISTS dataset_gaps (
    gap_id VARCHAR PRIMARY KEY,
    snapshot_id VARCHAR REFERENCES datasets(snapshot_id),
    gap_type VARCHAR NOT NULL,                   -- 'LEADING', 'INTERNAL', 'TRAILING', 'FULL_WINDOW'
    expected_open TIMESTAMP WITH TIME ZONE NOT NULL,
    subsequent_open TIMESTAMP WITH TIME ZONE,    -- NULL for TRAILING and FULL_WINDOW gaps
    missing_count BIGINT NOT NULL
);

-- Dedicated verification audit log; preserves immutability of datasets table
CREATE TABLE IF NOT EXISTS dataset_verification_history (
    verification_id VARCHAR PRIMARY KEY,
    snapshot_id VARCHAR REFERENCES datasets(snapshot_id),
    verified_at TIMESTAMP WITH TIME ZONE NOT NULL,
    verification_result VARCHAR NOT NULL,        -- 'VERIFIED', 'CORRUPTED', 'MISSING_FILE', 'CONFLICT'
    computed_file_sha256 VARCHAR,
    error_message VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_datasets_identity 
ON datasets(dataset_identity, ingested_at DESC);
```

### B. EMPTY Snapshot Deduplication & Re-Ingestion (`--force`)
1. **Deduplication:** When `open-quant ingest` is invoked:
   - Queries DuckDB for the latest `COMMITTED` snapshot matching `dataset_identity`.
   - If a verified `EMPTY` snapshot exists (`quality_status = 'EMPTY'`), the CLI skips exchange queries, logs: `Verified EMPTY dataset already recorded for this range. Skipping ingestion. Use --force to re-query exchange.`, and exits with returncode 0.
2. **Re-Ingestion (`--force`):**
   - Re-queries Binance vision endpoints.
   - If still empty, records a new immutable `EMPTY` snapshot with a fresh `snapshot_id` and updated `ingested_at` timestamp. Historical records remain immutable.

---

## 12. CLI Proposal & User Experience

```bash
# 1. Ingest historical public data with strict grid-aligned boundaries
open-quant ingest \
  --exchange binance \
  --symbol BTC/USDT \
  --timeframe 1h \
  --since 2026-11-01T00:00:00Z \
  --until 2026-11-05T00:00:00Z \
  [--output-dir data/] \
  [--fail-on-gaps] \
  [--force]

# 2. List registered dataset snapshots
open-quant datasets list [--symbol BTC/USDT] [--timeframe 1h] [--status GAP_FREE]

# 3. Inspect dataset details, gap distributions, and provenance
open-quant datasets inspect <snapshot_id_or_fingerprint>

# 4. Verify physical Parquet integrity against catalog fingerprints
open-quant datasets verify <snapshot_id_or_fingerprint>
```

### Sample CLI Report Output (Illustrative)
```text
============================================================
Open Quant Intelligence — Historical Ingestion Report
============================================================
Snapshot ID:           snap_f47ac10b8e21
Ingestion UUID:        f47ac10b-58cc-4372-a567-0e02b2c3d479
Exchange:              binance (public vision cluster)
Symbol:                BTC/USDT | Market: spot | Timeframe: 1h
Requested Range (UTC): 2026-11-01T00:00:00Z -> 2026-11-05T00:00:00Z
Coverage Range (UTC):  2026-11-01T00:00:00Z -> 2026-11-05T00:00:00Z
Expected Intervals:    96
Recorded Intervals:    96 (0 missing)
Ingestion Status:      COMMITTED
Quality Status:        GAP_FREE
Content Fingerprint:   sha256:7f9a2b84c310d54e8b21c430aef7830182419ef8973b02196a84920485a3bb12
Parquet Artifact:      data/datasets/exchange=binance/market=spot/symbol=BTC_USDT/timeframe=1h/dataset_20261101T000000Z_20261105T000000Z_7f9a2b84_f47ac10b.parquet
Artifact SHA-256:      sha256:4d831b7908c84857b28ec95ef5476a8947cf839a823908dbca19273645bbef21
Catalog Record:        REGISTERED (catalog.duckdb)
============================================================
```

---

## 13. Deterministic Offline Testing Strategy

All automated CI test suites run **100% offline** without network access or live exchange calls.

### Required Deterministic Test Suite
1. **`test_incomplete_tail_discard_refetch_validate`**: Asserts incomplete candle original values are discarded without price validation, cursor is preserved, fresh server time is fetched on retry, and refetched row is fully validated upon finalization.
2. **`test_incomplete_tail_retry_exhaustion_aborts`**: Injects candle remaining incomplete across all 3 retries; asserts ingestion aborts with `IncompleteTailCandleError` without committing dataset.
3. **`test_future_window_rejected_zero_requests`**: Asserts requesting `until_ms > server_time_ms` raises `FutureWindowError` and issues zero Kline queries.
4. **`test_full_window_coverage_leading_gap`**: Asserts missing first interval is correctly classified as a `LEADING` gap and increments `missing_intervals`.
5. **`test_full_window_coverage_trailing_gap`**: Asserts missing last interval is correctly classified as a `TRAILING` gap with `subsequent_open = NULL`.
6. **`test_full_window_coverage_internal_gap`**: Asserts gap between consecutive candles is recorded as `INTERNAL`.
7. **`test_unaligned_candle_timestamp_aborts`**: Asserts candle with timestamp not on the requested timeframe grid raises `UnalignedCandleError`.
8. **`test_coverage_invariant_holds`**: Asserts `record_count + missing_intervals == expected_records` across all gap permutations.
9. **`test_preexisting_target_collision_preserved_byte_for_byte`**: Asserts pre-existing target file with mismatched hash causes `RecoveryConflictError` and remains completely unmodified.
10. **`test_malformed_timestamp_raises_structured_error`**: Asserts row with string, missing, or negative timestamp raises `MalformedPageError` without silent skipping.
11. **`test_pagination_regression_and_stall_rejection`**: Asserts page with timestamps prior to cursor or with zero forward progress raises `PaginationRegressionError`.
12. **`test_out_of_range_row_corruption_ignored`**: Injects an exchange page containing rows beyond `until` with malformed OHLCV fields; asserts out-of-range rows are discarded before validation and do not cause ingestion failure.
13. **`test_empty_dataset_path_committed_without_file`**: Asserts zero admitted candles bypass M0 sequence validator, publish no Parquet file, compute canonical header-only SHA-256, and register a committed EMPTY snapshot.
14. **`test_crash_recovery_all_transitions_idempotent`**: Exercises all 8 transitions in the crash-recovery table using mock crashes; asserts reconciliation leaves catalog and files consistent.

---

## 14. Acceptance Criteria Checklist for M1

| # | Criterion | Verification Method |
|---|---|---|
| 1 | Allowlisted `fetch_spot_symbol_metadata` contract rejecting delisted or non-tradable pairs | Pre-flight unit tests |
| 2 | Reject future request windows ($until\_ms > server\_time\_ms$) with zero Kline requests | Pre-flight window unit tests |
| 3 | Reject malformed timestamps and non-advancing pages with structured errors | Pagination unit tests |
| 4 | Upper-bound clamp $[since, until)$ before field validation (out-of-range corruption ignored) | Pagination boundary unit test |
| 5 | Incomplete tail candle: original values discarded, fresh time per retry, re-fetch upon finalization | Tail guard unit tests |
| 6 | Full window coverage: leading, internal, trailing gaps, and grid alignment invariant | Gap analysis unit tests |
| 7 | Clean zero-candle branch committing EMPTY snapshots without Parquet files | Empty-path unit tests |
| 8 | Pre-existing target files protected against overwrite or automatic quarantine on hash collision | Collision preservation unit tests |
| 9 | Parquet physical rows strictly match `record_count`, with `record_count + missing == expected` | Staging verification unit tests |
| 10 | Cross-platform atomic no-clobber publication adhering to Win32 and POSIX API contracts | Storage publication unit tests |
| 11 | Complete 8-state crash-recovery transition protocol with durable manifests | Crash-recovery reconciliation tests |
| 12 | Immutable snapshot catalog with dedicated verification audit history | DuckDB catalog DDL & audit tests |
| 13 | 100% offline deterministic test suite passing on Python 3.11 and 3.12 | CI workflow matrix run |

---

## 15. Risks, Trade-offs & Unresolved Decisions

1. **Exchange Historical Data Corrections:** While rare, exchanges may occasionally revise historical klines. By storing immutable snapshots indexed by `snapshot_id`, OQI ensures backtests are permanently pinned to the dataset state as observed at ingestion time.
2. **Incomplete Tail Abort vs Truncation:** Aborting ingestion when a requested tail candle is still forming prioritizes exact window coverage over partial downloads. Users requesting recent data must wait until the interval completes.
3. **DuckDB In-Process Concurrency:** DuckDB supports concurrent readers but a single writer. The active ingestion manifest and CLI process lock serialize write access, preventing database lock contention.

---
**Milestone M1 Proposal Revision Complete. FINAL CANDIDATE — AWAITING TECHNICAL APPROVAL.**
