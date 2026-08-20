# Provider runtime refactor execution tracker

This is the authoritative implementation/progress tracker for [`refactor.plan.md`](./refactor.plan.md). The plan explains *why*; this file defines dependency order, ownership, acceptance, and accounting. Implementation has not started: every implementation checkbox is intentionally unchecked.

## Status legend and operating rules

- `[ ]` pending; `[x]` complete; `[-]` blocked; `[~]` deferred by an explicit recorded decision.
- A **chunk is ready** only when every ID in `Depends on` is `[x]`, the chunk itself is `[ ]`, and no active blocker names it.
- The **next chunk** is the first ready chunk in numeric order. Future sessions should read the accounting table and that chunk only; reread the narrative plan only when a task links to an unresolved design constraint.
- Mark a chunk `[x]` only after all child tasks and acceptance commands pass and the commit boundary is respected. Never mark a parent complete from code inspection alone.
- Keep `binance.rs` and `hyperliquid.rs` work sequential by default. A chunk marked parallel-safe may run concurrently only if its owned files do not overlap another active chunk.
- Do not mix implementation from a later chunk into an earlier commit. If file ownership must expand, record the added path and overlap before editing.

### Chunk rollback and recovery

- Every chunk commit must compile and pass that chunk's literal acceptance commands. Do not commit or mark `[x]` for a partial cutover, a compatibility shim, or a tree that requires a later chunk to compile.
- Before starting a chunk, record its pre-change commit. If acceptance fails before commit, amend the working tree until the chunk passes or restore only that chunk's owned edits to the recorded commit; never leave a failed partial cutover as the dependency base for another chunk. If acceptance fails after a local chunk commit, amend that commit when its contract remains valid; otherwise revert the whole chunk commit before further implementation.
- When extraction disproves a design assumption, stop that chunk, record `BLOCKED` evidence, update the narrative assumption plus this tracker's tasks, ownership, overlap, and dependency/accounting rows in a planning-only commit, then resume from the first dependency-ready unchecked chunk. A failed chunk retains exclusive ownership until it is amended or fully reverted; no later chunk may borrow its files meanwhile.
- `Scope completed` in a blocker is informational only: reverted work is not complete, and retained work may be counted only after it is recommitted in a buildable chunk whose full acceptance passes. Never reorder/split a chunk or expand ownership silently; record the new IDs/dependencies/paths first.

### Blocker/deferred record format

```text
BLOCKED <chunk-id> — <date> — <owner/session>
Reason: <specific unavailable prerequisite or failing acceptance>
Evidence: <exact command/result or path/line>
Unblock: <single concrete condition>
Scope completed: <finished tasks that remain valid>

DEFERRED <decision-id> — <date> — <authority>
Decision: <explicitly non-applicable work>
Revisit when: <concrete architecture/protocol change, or "never for this refactor">
```

## Recorded constraints (not implementation tasks)

- **C-01:** Do not add Binance-style dynamic `SUBSCRIBE` to the one-market Binance feed; URL-bound streams remain until multiplexing is explicitly required.
- **C-02:** Do not add Binance JSON heartbeat; Binance uses WebSocket Ping/Pong frames. Hyperliquid alone sends `{"method":"ping"}` and validates `{"channel":"pong"}`.
- **C-03:** Do not add Hyperliquid protocol details to Binance: POST `/info`, `now_ms`, boundary-row truncation, number-or-string decimals, REST symbol/interval echoes, HIP-3, or 1s/6h rejection.
- **C-04:** Do not add Binance protocol details to Hyperliquid: URL stream binding, `serverShutdown`, explicit `x`, fixed 24-hour lifetime, 418 ban semantics, 1s/6h support, or strict over-requested-limit rejection.
- **C-05:** Preserve `Instrument` display/wire identity separation; do not add Binance mapping special cases.
- **C-06:** Local provider canonicalization metadata remains independent of transport registration.
- **C-07:** Shared runtime uses minimal internal hooks/policies, not a public protocol-detail-heavy `MarketDataProvider`/`ProviderAdapter` trait.
- **C-08:** Hyperliquid implicit finality is successor evidence: current candle is authoritative open; a later `open_time` upgrades its predecessor to authoritative closed. Local wall clock alone is not finality evidence.
- **C-09:** Hyperliquid REST retains request-kind-aware truncation: `Latest`/`Older` retain newest N, `Gap` retains earliest N; an overlap row is not itself a protocol error.
- **C-10:** Shared codecs remain statically dispatched where practical, receive `&mut self`, and may emit multiple outcomes into a caller-owned queue without a per-frame temporary allocation.

## Dependency-ordered chunks

### R01 — Implement Hyperliquid WebSocket protocol contracts

**Status:** [x]
**Depends on:** none  
**Owned files:** `src/provider/hyperliquid.rs`, `src/error.rs`, `tests/hyperliquid_ws_codec.rs`, `tests/hyperliquid_live.rs`, `tests/fixtures/hyperliquid_candle_open.json`, `tests/fixtures/hyperliquid_candle_closed.json`  
**Parallel safety:** Sequential; `hyperliquid.rs` is a high-overlap file and is reserved exclusively.  
**Commit boundary:** `fix(hyperliquid): enforce websocket protocol contracts`

- [x] Replace the old “subscription response is ignored” expectation with matching subscribe-ack acceptance and mismatched `method`/`type`/`coin`/`interval` rejection cases.
- [x] Add `TimeoutKind::SubscribeAck`. Hyperliquid connection policy owns `subscribe_ack_timeout`, defaults it to 10 seconds, validates it in the same `1ms..=60s` range as live protocol deadlines, and measures it with the injected monotonic `Clock` from the successful subscribe-message flush. Expiry returns `ProviderError::Timeout { kind: TimeoutKind::SubscribeAck, context: ErrorOperation::WebSocket + market/timeframe }`; it is a recoverable setup failure and must never be reported as `FirstKline`.
- [x] While readiness is pending, use this deterministic precedence: cancellation wins over every simultaneous event; then a received Close or transport/read/write error; then malformed/mismatched subscribe ack; then subscribe-ack deadline; then WebSocket inactivity. Transport Ping/Pong remains serviced, but the 50-second application heartbeat does not start until the matching ack is accepted. If ack and its deadline become observable in the same poll, accept the already-received matching ack. Add focused tests for every boundary and tie.
  - Re-review fix verified: readiness coalesces lower-priority outcomes while draining currently ready input, yields after 256 frames so deadlines/cancellation cannot starve, and end-to-end supervisor-loop cases cover more than 64 malformed acknowledgements before Close and abrupt EOF/read failure plus actual ack/deadline/inactivity ties.
- [x] Make connection readiness mean subscribe sent, successfully flushed, and matching `subscriptionResponse` validated before `GapSync`; only after readiness start application JSON ping every 50 seconds and matching pong handling/tests, without weakening transport Ping/Pong/Close behavior or inactivity enforcement. Assert Binance-style JSON ping is not a shared default.
- [x] Make the Hyperliquid codec stateful and implement this exact successor-finality transition table:

  | Input relative to retained current candle `T` | Emitted outcomes, in order | Retained state after input | Error/drop rule |
  |---|---|---|---|
  | no retained candle; first row `U` | `U` as `WsAuthoritativeOpen` | latest payload for `U` | none |
  | same `open_time == T`, payload changed | updated `T` as `WsAuthoritativeOpen` | replace retained `T` | none |
  | same `open_time == T`, payload identical | none | retain `T` unchanged | drop exact duplicate |
  | immediate successor `U == T + interval` | retained latest `T` payload as `WsAuthoritativeClosed`, then `U` as `WsAuthoritativeOpen` | retain `U` | none |
  | skipped successor `U > T + interval` | retained latest `T` payload as `WsAuthoritativeClosed`, then `U` as `WsAuthoritativeOpen` | retain `U`; synthesize no missing candles | none; gap reconciliation owns the missing interval(s) |
  | regressive/out-of-order `U < T` | none | retain `T` unchanged | drop as stale, not a protocol error and not a reconnect request |

- [x] Do not use local wall clock as finality evidence. Before finality state mutation, validate the subscribed timeframe's open-time grid and exact close boundary, including calendar-month boundaries; malformed off-grid, inconsistent-close, and forged between-interval frames must not replace retained state. Cover first/current open, changed same-time update, exact duplicate, immediate successor, skipped successor, regressive input, retained state after duplicate/regressive/malformed input, and the two-outcome close-then-open order.
- [x] Preserve the existing wire coin, unsupported-timeframe, symbol/interval echo, malformed payload, and transport control-frame contracts.
- [x] Verify: `cargo test --locked --test hyperliquid_ws_codec --no-default-features --features test-transport` (9 passed).
- [x] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport` (13 passed).

**Implementation/verification notes:** Complete after second-review fixes. Readiness work is bounded to 256 transport frames per poll while lower-priority outcomes are coalesced, so the former 64-outcome retention cap is not an arbitration horizon; production supervisor-loop tests cover terminal and deadline precedence; fixed and calendar candle windows are validated before retained-state mutation; deterministic heartbeat start/due hooks replace scheduler-sensitive millisecond windows. Orchestrator formatting and focused gates passed: `hyperliquid_ws_codec` 9/9 and `hyperliquid_live` 13/13.

### R02 — Implement bounded Hyperliquid REST and provider rate-limit contracts

**Status:** [ ]  
**Depends on:** R01  
**Owned files:** `src/provider/hyperliquid.rs`, `src/provider/binance.rs`, `tests/hyperliquid_rest.rs`, `tests/binance_rest.rs`, `tests/binance_live.rs`  
**Parallel safety:** Sequential; both large provider files overlap later extraction work and are reserved exclusively.  
**Commit boundary:** `fix(provider): bound Hyperliquid REST and separate rate limits`

- [ ] Replace whole-response `Value` plus per-row clone decoding with a Hyperliquid streaming bounded array visitor. Define `HYPERLIQUID_MAX_RESPONSE_ROWS = 1001` independently from `requested_limit` (which remains `1..=1000`); the existing capped HTTP body remains an additional byte bound, not the row-count contract.
- [ ] Validate required candle field `n` as a non-negative integer while retaining number/string decimal parsing, symbol/interval validation, and malformed-row rejection.
- [ ] Exact retention/absolute-limit behavior: for `N = requested_limit`, arrays of `0..=N` rows are accepted unchanged; arrays of `N+1..=1001` valid rows are accepted and truncated while streaming (`Latest`/`Older` retain the newest N with an N-slot ring/deque, `Gap` retains the earliest N and validates but does not retain later rows); row 1002 causes a protocol/payload oversized-array error with no partial result. Thus the documented 1001-row overlap is valid even when `N == 1000`, while 1002 rows is resource/protocol abuse. Add boundary tests for `N`, `N+1`, 1001, and 1002 for every request kind.
- [ ] Add/retain Binance contracts for strict 12-field rows, over-requested-limit rejection, `-1121`, 429 fallback/Retry-After, and 418 valid-expiry versus invalid/missing-expiry process block.
- [ ] Remove Hyperliquid’s Binance-only invalid-ban/process-block branches; test 429 valid Retry-After or local fallback and prove absent Binance ban metadata never causes permanent block.
- [ ] Verify: `cargo test --locked --test hyperliquid_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.

### R03 — Extract shared WebSocket transport and EventEmitter

**Status:** [ ]  
**Depends on:** R02  
**Owned files:** `src/provider/runtime/mod.rs` (new), `src/provider/runtime/websocket.rs` (new), `src/provider/runtime/emitter.rs` (new), `src/provider/binance.rs`, `src/provider/hyperliquid.rs`, `tests/binance_ws_codec.rs`, `tests/hyperliquid_ws_codec.rs`  
**Parallel safety:** Sequential; both large provider files and both codec suites overlap. No concurrent provider refactor chunk.  
**Commit boundary:** `refactor(provider): share websocket transport and emitter`

- [ ] Declare the internal runtime module without broadening the public API.
- [ ] Move `WsConfig`, `RawWebSocket`, read/write/flush pump, control-frame flush, stalled-write and inactivity deadlines, decoded queue, error mapping, loopback URL safety, and connect-config validation into `runtime/websocket.rs`.
- [ ] Define the mutable codec/outcome contract with provider-neutral `ReconnectRequested`; keep Binance `serverShutdown` mapping inside its codec.
- [ ] Move shared `EventEmitter`/queue-emission mechanics into `runtime/emitter.rs`, preserving keyed candle capacity, emergency control pair, saturation behavior, and cancellation precedence.
- [ ] Adapt Binance and Hyperliquid codecs/connection setup to the shared transport while keeping provider subscription and heartbeat policies separate.
- [ ] Mechanically migrate both codec suites' imports and calls from provider-local `WsConfig`, `DecodedFrame`, connector/raw-socket, and decoder test exports to the shared runtime API or provider-owned codec harness. Keep all existing test cases in place in this commit; R04 owns semantic relocation.
- [ ] Remove duplicated transport/emitter definitions and obsolete provider-local test-only exports only after both codec suites compile against their new owners; migrate all callers in the same commit.
- [ ] Verify: `cargo test --locked --test binance_ws_codec --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_ws_codec --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.

### R04 — Establish shared WebSocket/EventEmitter contract tests

**Status:** [ ]  
**Depends on:** R03  
**Owned files:** `tests/provider_runtime_websocket.rs` (new), `tests/binance_ws_codec.rs`, `tests/hyperliquid_ws_codec.rs`, `tests/binance_live.rs`, `tests/hyperliquid_live.rs`, `src/provider/runtime/websocket.rs`, `src/provider/runtime/emitter.rs`  
**Parallel safety:** Sequential with R03/R05 because runtime files and provider WS/live suites overlap.  
**Commit boundary:** `test(provider): centralize websocket runtime contracts`

- [ ] Move provider-neutral socket-pump/config/control-frame cases out of `tests/binance_ws_codec.rs`: stalled write, inactivity, transport Ping/Pong, Close ordering, automatic flush, decode queue, reconnect request, cancellation priority, and loopback/config rejection. Move any provider-neutral equivalents discovered in `tests/hyperliquid_ws_codec.rs`; do not duplicate them.
- [ ] Move provider-neutral emitter cases from live suites: keyed replacement, control saturation, emergency pair ordering/capacity, terminal-event delivery, and producer completion.
- [ ] Retain only protocol-specific WS cases in codec/live suites: Binance hosts/paths/stream URL, payload schema and `x`, `serverShutdown`; Hyperliquid subscribe ack, JSON ping/pong, implicit finality, wire coin and symbol/interval validation. Provider-specific readiness integration stays in live suites.
- [ ] Ensure each migrated test exercises the shared runtime once rather than once per provider.
- [ ] Verify: `cargo test --locked --test provider_runtime_websocket --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_ws_codec --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_ws_codec --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.

### R05 — Extract the shared live engine

**Status:** [ ]  
**Depends on:** R04  
**Owned files:** `src/provider/runtime/live.rs` (new), `src/provider/runtime/mod.rs`, `src/provider/binance.rs`, `src/provider/hyperliquid.rs`, `tests/binance_live.rs`, `tests/hyperliquid_live.rs`  
**Parallel safety:** Sequential; highest overlap across both providers and both live suites.  
**Commit boundary:** `refactor(provider): share live supervision engine`

- [ ] Move `LiveSupervisorConfig`, supervision, generation lifecycle, connected loop, gap REST/WS reconciliation, accepted-watermark pursuit, revision/ack gate, backoff, generation invalidation/purge, queue saturation handling, emergency barrier, classifications, cancellation precedence, and filtered event stream into `runtime/live.rs`.
- [ ] Define only the minimal internal hooks needed for request validation, ready socket connection, history, rate gate, live config, history page limit, and connection rotation.
- [ ] Enforce that `connect_ready_socket()` returns only after provider-specific subscription establishment; Binance URL handshake and Hyperliquid subscribe ack remain provider-owned.
- [ ] Preserve REST concurrency during WS activity, first-candle timeout semantics, rate-gate/backoff deadline combination, reconnect generation purge, and terminal/recoverable error precedence.
- [ ] Migrate every provider caller and both live suites' imports; remove duplicated live state-machine code/types/test exports in the same commit. Keep semantic test relocation for R06.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test app_live_contract --no-default-features --features test-transport`.

### R06 — Migrate shared live contract tests

**Status:** [ ]  
**Depends on:** R05  
**Owned files:** `tests/provider_runtime_live.rs` (new), `tests/binance_live.rs`, `tests/hyperliquid_live.rs`, `src/provider/runtime/live.rs`  
**Parallel safety:** Sequential with R05/R07; overlaps shared runtime and Binance live suite.  
**Commit boundary:** `test(provider): centralize live runtime contracts`

- [ ] Migrate reconciliation, ack gate, accepted-watermark changes, REST-during-WS concurrency, cancellation precedence, queue saturation/emergency pair, exponential backoff sequence, reconnect generation purge, first-candle timeout, rate-gate/backoff deadline, and completion/error classification to provider-neutral shared tests.
- [ ] Build a minimal deterministic runtime harness rather than a fake third provider abstraction.
- [ ] Preserve provider live suites only for connection readiness and protocol-policy differences.
- [ ] Confirm migrated tests fail against plausible runtime regressions and do not assert implementation-only text or type locations.
- [ ] Verify: `cargo test --locked --test provider_runtime_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test app_live_contract --no-default-features --features test-transport`.

### R07 — Extract shared HTTP runtime and rate-limit decision policy

**Status:** [ ]  
**Depends on:** R06  
**Owned files:** `src/provider/runtime/http.rs` (new), `src/provider/runtime/mod.rs`, `src/provider/binance.rs`, `src/provider/hyperliquid.rs`, `tests/provider_runtime_http.rs` (new), `tests/binance_rest.rs`, `tests/hyperliquid_rest.rs`  
**Parallel safety:** Sequential; both provider files and both REST suites overlap.  
**Commit boundary:** `refactor(provider): share HTTP runtime and rate gating`

- [ ] Move safe `reqwest::Client` construction, `no_proxy`, disabled redirects, User-Agent, request cancellation, timeout/transport mapping, capped body reader, rate-gate wait, and absorbing/max-deadline gate mechanics into `runtime/http.rs`.
- [ ] Keep URL/method/query/body, provider error payload parsing, invalid-symbol detection, rate-limit status interpretation, and success decoder in provider code.
- [ ] Introduce the internal provider-specific `RateLimitDecision` mapping: Binance may process-block only for its 418 contract; Hyperliquid never derives a permanent block from absent Binance ban metadata.
- [ ] Remove both duplicate `read_capped` implementations and duplicate common status skeletons without moving Binance `-1121` or Hyperliquid `{error: ...}` into shared code.
- [ ] Move provider-neutral capped-body, cancellation, timeout mapping, safe-client, rate-gate wait, maximum timed deadline, and absorbing process-block cases from both REST suites into `tests/provider_runtime_http.rs`. Retain provider payload/status/decoder behavior in its provider REST suite.
- [ ] Verify: `cargo test --locked --test provider_runtime_http --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.

### R08 — Add capabilities and internal protocol policy

**Status:** [ ]  
**Depends on:** R07  
**Owned files:** `src/provider/mod.rs`, `src/provider/runtime/live.rs`, `src/provider/binance.rs`, `src/provider/hyperliquid.rs`, `src/app.rs`, `src/history.rs`, `src/snapshot.rs`, `tests/provider_contract.rs`, `tests/app_live_contract.rs`, `tests/history_coordinator.rs`, `tests/snapshot_runner.rs`, `tests/provider_runtime_live.rs`, `tests/binance_live.rs`, `tests/hyperliquid_live.rs`  
**Parallel safety:** Sequential; overlaps both providers, live runtime, every caller-visible capability consumer, and every current fake `MarketDataProvider`.  
**Commit boundary:** `feat(provider): expose capabilities and isolate protocol policy`

- [ ] Add `fn capabilities(&self) -> ProviderCapabilities` as a required `MarketDataProvider` method with no default. `ProviderCapabilities` exposes supported markets, supported timeframes, and a non-zero `history_page_limit` whose contract is the provider maximum rows accepted by one history request; it is not an exact size every caller must request. Protocol-only policy remains internal.
- [ ] Implement the required method for Binance, Hyperliquid, and every current fake provider in `provider_contract`, `app_live_contract`, `history_coordinator`, and `snapshot_runner`; fake capabilities must be explicit per test so maximum-size/preflight assertions cannot accidentally inherit production defaults. Validate/reject a zero advertised history maximum before network I/O.
- [ ] Implement Binance capabilities with all supported timeframes and Hyperliquid capabilities with 1s/6h excluded.
- [ ] Route initial app startup, interactive switch preparation, snapshot startup, `HistoryCoordinator` older-page requests, and shared live gap requests through capabilities before network I/O. Preserve `INITIAL_HISTORY_LIMIT = 500` for initial app/switch requests and `SNAPSHOT_HISTORY_LIMIT = 500` for snapshot requests as desired product request sizes; each actual request must use `min(desired_limit, capabilities.history_page_limit)`. Older-history and shared live gap requests use the advertised maximum directly. Remove provider-local `GAP_PAGE_LIMIT`, `history::HISTORY_PAGE_LIMIT`, and duplicated Hyperliquid timeframe-preflight callsites once every caller consumes the advertised maximum; retain the two intentional 500-row desired-size constants and provider decoder/request validation as defense in depth.
- [ ] Keep subscription style, heartbeat, finality evidence, rate-limit semantics, connection rotation, and payload retention as internal policies/hooks rather than public trait fields. In particular, expose no `connection_rotation` field in `ProviderCapabilities`; the shared live runtime obtains rotation only through its internal provider policy/hook.
- [ ] Add contract tests for pre-network rejection at initial app/snapshot/switch/live/history entry points, including a zero advertised maximum; complete Binance support; and Hyperliquid exclusions. In `app_live_contract` and `snapshot_runner`, assert the retained desired size is 500 when the provider maximum is at least 500 and is capped to a smaller fake maximum before any request. In `history_coordinator` and the shared/provider live contract suites, assert older-history and gap requests use the fake advertised maximum exactly. In `provider_contract`, assert capabilities describe a provider maximum and contain no protocol-policy field.
- [ ] Verify: `cargo test --locked --test provider_contract --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test history_coordinator --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test snapshot_runner --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test app_live_contract --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.

### R09 — Generalize ProviderRegistry

**Status:** [ ]  
**Depends on:** R08  
**Owned files:** `src/provider/mod.rs`, `src/main.rs`, `src/app.rs`, `tests/provider_contract.rs`, `tests/app_live_contract.rs`, `tests/binance_live.rs`, `tests/api_boundaries.rs`  
**Parallel safety:** Parallel-safe only against work that touches none of these files; otherwise sequential. Does not touch provider implementations.  
**Commit boundary:** `refactor(provider): use generic provider registry`

- [ ] Replace hard-coded fields with `BTreeMap<ProviderId, Arc<dyn MarketDataProvider>>`; construction/registration rejects duplicate IDs and `get` borrows `&ProviderId`.
- [ ] Remove `with_hyperliquid`, `with_test_provider`, injected-provider special case, and Binance-only accessor in the same commit.
- [ ] Migrate `src/main.rs` to register Binance and Hyperliquid through the generic constructor/registration path; migrate both `src/app.rs` lookups to `get(provider_id_ref)` without cloning.
- [ ] Migrate `tests/app_live_contract.rs` fake registration to the ordinary generic path. Migrate `tests/binance_live.rs` constructor calls, `.binance()` pointer assertion, and owned-ID `.get(...)` calls to generic registration, trait-object identity/behavior assertions, and borrowed lookup. Update both compile fixtures in `tests/api_boundaries.rs` to the new constructor while preserving what each fixture proves.
- [ ] Preserve canonicalization of known-but-unregistered providers independently from registry transport lookup.
- [ ] Test two-provider registration, borrowed lookup, duplicate rejection, unknown/unregistered failure, test-provider registration, and absence of provider-specific registry paths.
- [ ] Verify: `cargo test --locked --test provider_contract --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test app_live_contract --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test api_boundaries --no-default-features --features test-transport`.

### R10 — Complete protocol/shared-test migration and cleanup

**Status:** [ ]  
**Depends on:** R09  
**Owned files:** `src/provider/binance.rs`, `src/provider/hyperliquid.rs`, `tests/binance_ws_codec.rs`, `tests/hyperliquid_ws_codec.rs`, `tests/binance_rest.rs`, `tests/hyperliquid_rest.rs`, `tests/binance_live.rs`, `tests/hyperliquid_live.rs`, `tests/provider_contract.rs`, `tests/provider_runtime_websocket.rs`, `tests/provider_runtime_live.rs`, `tests/provider_runtime_http.rs`  
**Parallel safety:** Sequential; final provider and provider-test cleanup, with all overlapping runtime contract suites explicitly owned.  
**Commit boundary:** `refactor(provider): finish runtime cutover and cleanup`

- [ ] Audit every recommendation in `refactor.plan.md` against this tracker; add any missing actionable item before closing this chunk.
- [ ] Final shared inventory: `provider_runtime_websocket` alone owns socket config/pump/control frames/deadlines/flush/decoded queue and emitter mechanics; `provider_runtime_live` alone owns reconciliation, generation, ack/watermark, concurrency, cancellation, saturation, backoff and purge; `provider_runtime_http` alone owns capped body/common cancellation-timeout/client/rate-gate mechanics.
- [ ] Final Binance protocol inventory: `binance_ws_codec`/`binance_live` retain Spot/USD-M hosts and paths, stream URL, payload schema, `x`, provider readiness and `serverShutdown`; `binance_rest` retains strict 12-field rows, over-limit rejection, `-1121`, and 418/429 semantics. No provider-neutral runtime contract remains in those files.
- [ ] Final Hyperliquid protocol inventory: `hyperliquid_ws_codec`/`hyperliquid_live` retain wire remap, unsupported timeframes, subscribe ack, JSON ping/pong, implicit finality, symbol/interval echoes and provider readiness; `hyperliquid_rest` retains candle window, bounded overlap/truncation, number/string decimals, `n`, symbol/interval echoes, error payload and Hyperliquid 429 policy. No provider-neutral runtime contract remains in those files.
- [ ] Delete obsolete duplicate runtime code, aliases, re-exports, test-only accessors, comments, constants, and scaffolding; migrate every caller rather than leaving compatibility shims.
- [ ] Confirm constraints C-01 through C-10 remain true and no explicitly non-applicable mechanism was implemented.
- [ ] Record before/after provider file sizes as informational evidence only; do not make line count a correctness gate.
- [ ] Verify: `cargo test --locked --test provider_runtime_websocket --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test provider_runtime_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test provider_runtime_http --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_ws_codec --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_ws_codec --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_rest --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test binance_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test hyperliquid_live --no-default-features --features test-transport`.
- [ ] Verify: `cargo test --locked --test provider_contract --no-default-features --features test-transport`.

### R11 — Final repository gates and tracker accounting

**Status:** [ ]  
**Depends on:** R10  
**Owned files:** `docs/plan/refactor.todo.md`, `docs/plan/refactor.plan.md` (only if assumptions changed), `.github/workflows/ci.yml` (only if commands genuinely changed), `Cargo.toml` (only if features genuinely changed)  
**Parallel safety:** Final exclusive gate; no concurrent implementation edits.  
**Commit boundary:** `chore(provider): close refactor tracker` (only if accounting changes remain after implementation commits)

- [ ] Run `cargo fmt --check`.
- [ ] Run `cargo test --locked --all-targets --no-default-features --features test-transport`.
- [ ] Run `cargo clippy --locked --all-targets --no-default-features --features test-transport -- -D warnings`.
- [ ] Run `cargo check --locked --all-targets --no-default-features --features production-transport`.
- [ ] Run `cargo clippy --locked --all-targets --no-default-features --features production-transport -- -D warnings`.
- [ ] Run `cargo test --locked --test feature_selection default_is_production_only -- --exact`.
- [ ] Run this complete mutual-exclusion assertion (the same logic as CI), which succeeds only for Cargo exit 101, exactly one dedicated conflict error, and no additional compiler error:

  ```bash
  set -euo pipefail
  set +e
  output="$(cargo check --locked --all-targets --no-default-features --features production-transport,test-transport 2>&1)"
  status=$?
  set -e
  printf '%s\n' "$output"

  if [[ $status -ne 101 ]]; then
    echo "Expected Cargo exit 101 for mutually exclusive features, got $status" >&2
    exit 1
  fi

  expected='error: features `test-transport` and `production-transport` are mutually exclusive'
  if [[ "$(printf '%s\n' "$output" | awk -v expected="$expected" '$0 == expected { count++ } END { print count + 0 }')" -ne 1 ]]; then
    echo "Expected exactly one dedicated feature-conflict compile_error" >&2
    exit 1
  fi

  if [[ "$(printf '%s\n' "$output" | awk '/^error(\[[^]]+\])?:/ && $0 !~ /^error: could not compile / { count++ } END { print count + 0 }')" -ne 1 ]]; then
    echo "Mutual-exclusion check failed for an additional compiler error" >&2
    exit 1
  fi
  ```

- [ ] Run `cargo test --locked --test api_boundaries --no-default-features --features test-transport combined_production_constructors_are_unnameable -- --exact`.
- [ ] Run `cargo test --locked --test api_boundaries --no-default-features --features production-transport combined_production_constructors_are_unnameable -- --exact`.
- [ ] Run `cargo test --locked --test app_live_contract --no-default-features --features test-transport reconciliation_target_and_state_persistence -- --exact`.
- [ ] Update the accounting table statuses/evidence; ensure every child checkbox is accounted for and no `[ ]`/`[-]` remains before marking R11 complete.
- [ ] Confirm the narrative plan links here, constraints match final behavior, and no stale location/count claim is presented as current implementation evidence.

## Chunk accounting

| ID | Status | Depends on | Ready now | Evidence / blocker |
|---|---|---|---|---|
| R01 | [x] | — | no | Complete after second-review fixes; formatted focused gates passed: `hyperliquid_ws_codec` 9/9 and `hyperliquid_live` 13/13 |
| R02 | [ ] | R01 | yes | — |
| R03 | [ ] | R02 | no | — |
| R04 | [ ] | R03 | no | — |
| R05 | [ ] | R04 | no | — |
| R06 | [ ] | R05 | no | — |
| R07 | [ ] | R06 | no | — |
| R08 | [ ] | R07 | no | — |
| R09 | [ ] | R08 | no | — |
| R10 | [ ] | R09 | no | — |
| R11 | [ ] | R10 | no | — |

**Next dependency-ready unchecked chunk:** R02 — implement bounded Hyperliquid REST and provider rate-limit contracts.
