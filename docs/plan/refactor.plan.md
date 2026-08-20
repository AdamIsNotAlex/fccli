## 结论

这两个 provider 当前最大的结构问题，不是“彼此缺少机制”，而是**公共运行时被复制了两份**。

- `binance.rs`：3023 行
- `hyperliquid.rs`：3053 行
- provider 名称归一化后的行级 diff：约 **89.6% 相似**
- `LiveSupervisorConfig`、`WsConfig`、`RawWebSocket`、重连、REST/WS 对账、backoff、事件队列、rate gate 等基本是同一套代码。

因此：

1. Binance → Hyperliquid：主要补**同等级的协议严谨性和测试覆盖**，不能照搬 Binance 协议细节。
2. Hyperliquid → Binance：目前几乎没有应直接移植的运行机制；值得吸收的是**能力描述和协议策略分层**。
3. 应尽快抽出 shared runtime，否则加入第三个 provider 会再复制约 2500 行复杂并发代码。

---

# 1. Binance 有、Hyperliquid 应该补的机制

## 1.1 WebSocket 订阅必须有明确的“建立成功”阶段

**建议补，优先级高。**

Binance 把订阅绑定在 URL：

- `/ws/<symbol>@kline_<interval>`
- 收到数据后再次校验 symbol 和 interval  
  `src/provider/binance.rs:340-383, 451-458`

Hyperliquid 当前：

1. 先发出 `GapSync`
2. 再发送 `subscribe`
3. 所有非 `candle` channel 都直接忽略，包括 `subscriptionResponse`

位置：

- `src/provider/hyperliquid.rs:391-393`
- `src/provider/hyperliquid.rs:1392-1424`
- 测试甚至只验证 subscription response 被忽略：`tests/hyperliquid_ws_codec.rs:52-63`

建议改成：

```text
Connecting
  -> TCP/TLS/WebSocket connected
  -> send subscribe
  -> validate matching subscriptionResponse
  -> connection is ready
  -> GapSync
```

至少校验 ack 中：

- `method == "subscribe"`
- `type == "candle"`
- `coin == instrument.provider_symbol()`
- `interval == timeframe`

这样订阅失败不会退化成含糊的 `FirstKline` timeout。

实际主网探测确认有效订阅依次返回：

```text
subscriptionResponse -> pong -> candle
```

---

## 1.2 Hyperliquid 需要自己的 application-level heartbeat

**建议补，不能复用 Binance 心跳模型。**

Binance 是服务端发送 WebSocket Ping frame，客户端自动 Pong；单连接明确限制 24 小时。当前 `RawWebSocket` 很适合这个协议。

Hyperliquid 官方 SDK 则每 50 秒发送：

```json
{"method":"ping"}
```

服务端返回：

```json
{"channel":"pong"}
```

当前 Hyperliquid provider 只发送订阅消息，没有主动 ping；但共用的 message inactivity timeout 是 60 秒。

风险：[INFERENCE] 对成交稀疏的市场，如果服务端没有其他消息，可能每 60 秒被本地 inactivity timeout 主动重连，并消耗 Hyperliquid 的每分钟新建连接额度。

建议将 heartbeat 定义为 provider policy：

```rust
enum HeartbeatPolicy {
    ServerPing,
    ApplicationJsonPing { interval: Duration },
}
```

- Binance：`ServerPing`
- Hyperliquid：`ApplicationJsonPing { interval: 50s }`

不要给 Binance 发送 Hyperliquid 风格的 JSON ping。

---

## 1.3 REST 解码应达到 Binance 的资源边界和字段验证强度

**建议补，但不要照搬 Binance 的“超出 requested_limit 就报错”。**

Binance：

- 使用 bounded serde visitor
- 不会先把完整响应解析成任意大的 `Value`
- 严格验证 12 个字段
- 连暂时不用的 trade count、quote volume 等字段也验证类型、有限性和非负性  
  `src/provider/binance.rs:2747-2917`

Hyperliquid 当前：

- 先把完整响应解析成 `Value`
- 每一行再 `clone()` 后 `from_value`
- 只验证 OHLCV、symbol、interval
- 官方 candle 中的 `n` trade count 没有进入 `HlCandle`，因此缺失或类型错误都会被静默接受  
  `src/provider/hyperliquid.rs:2750-2838`

建议：

- 写 Hyperliquid 专用的 streaming array visitor。
- `Latest/Older`：有界保留最新 N 条。
- `Gap`：有界保留最早 N 条。
- 验证 `n` 为非负整数。
- 仍允许 JSON number/string 两种 decimal 表达，这是 Hyperliquid codec policy。
- 保留当前 request-kind-aware truncation。

最后一点很重要：主网实际探测中，1000 个 interval 的时间窗口返回了 **1001 条**，因为边界 candle 会重叠。因此不能复制 Binance 的“超过 requested limit 即协议错误”。

---

## 1.4 补齐 Hyperliquid live/runtime 契约测试

**强烈建议，优先级最高。**

当前测试规模：

| Provider | REST | WS codec | Live | 合计 |
|---|---:|---:|---:|---:|
| Binance | 23 | 23 | 42 | 88 |
| Hyperliquid | 7 | 3 | 1 | 11 |

Hyperliquid 的 live supervisor 虽然几乎复制了 Binance 的实现，但基本没有验证：

- first-candle timeout
- reconciliation ack gate
- accepted watermark 变化
- REST 请求期间的 WS 并发
- cancellation precedence
- queue saturation/emergency pair
- backoff 序列
- reconnect 后 generation purge
- stalled write、Ping/Pong、Close 顺序
- rate gate 与 backoff deadline 组合

这些测试不应再复制一份。抽出 live engine 后，把 Binance 的绝大部分 live tests 改成 shared runtime contract tests；两个 provider 只保留协议差异测试。

---

## 1.5 Binance 的明确收盘标志不能直接移植

**不适用，需要 Hyperliquid 自己的 finality policy。**

Binance payload 有明确的：

```json
"x": true
```

对应：

- `src/provider/binance.rs:293-294`
- `src/provider/binance.rs:488`

Hyperliquid candle 没有 closed 字段。当前实现用本机墙上时钟推断：

```rust
let closed = kline.close_time < unix_now_ms().unwrap_or(kline.close_time);
```

`src/provider/hyperliquid.rs:412`

风险：[INFERENCE]

- 本机时钟领先：可能过早标记 closed。
- 本机时钟落后：可能延迟标记 closed。
- 一旦标为 `WsAuthoritativeClosed`，REST 不会覆盖它。

更稳妥的策略：

- 当前 candle 一律视为 authoritative open。
- 收到更晚 `open_time` 的 candle 后，才把上一根升级为 authoritative closed。
- 可选：服务器时间证据作为补充，不能只依赖本机时间。

这要求 Hyperliquid codec 是**有状态的**；也是为什么 shared `WsCodec` 应接收 `&mut self`。

---

## 1.6 Binance 418/IP-ban 机制不应进入 Hyperliquid

**不适用，而且当前 Hyperliquid 已经携带了过多 Binance 语义。**

Binance 官方合同明确：

- 429：必须 backoff
- 持续违反后返回 418
- 429/418 都有 `Retry-After`
- 418 缺少合法 expiry 时，当前程序进行 process-wide block

Hyperliquid 文档描述的是：

- 每 IP 每分钟 1200 weight
- `candleSnapshot` 按返回数量额外计 weight
- WebSocket 连接和消息额度

没有 Binance 式 418 ban 合同。

但 Hyperliquid 当前仍复制了：

- `ProcessBlocker::InvalidBanExpiry`
- `send_invalid_ban_and_stop`
- live loop 中大量 process-block 分支  
  `src/provider/hyperliquid.rs:1134-1195, 1241-1245` 等

建议抽成：

```rust
enum RateLimitDecision {
    TimedUntil(MonoInstant),
    ProcessBlocked(ProcessBlocker),
    NotRateLimited,
}
```

provider policy：

- Binance：429 fallback；418 必须有有效 expiry，否则 process block。
- Hyperliquid：429 使用合法 `Retry-After` 或本地 fallback；绝不因为缺少 Binance ban header 而永久阻断。

---

## 1.7 以下 Binance 机制不要补进 Hyperliquid

| Binance 机制 | Hyperliquid 是否适用 | 结论 |
|---|---|---|
| URL 中绑定 stream | 否，HL 使用 subscribe message | 不移植 |
| `serverShutdown` event | HL 未定义同类事件 | 普通 Close/transport reconnect 即可 |
| 明确 `x` closed flag | HL payload 没有 | 使用隐式 finality policy |
| 固定 24 小时连接寿命 | Binance 明确规定；HL 只说可能周期性断开 | 做 provider policy，不硬编码共同值 |
| 418 ban expiry | HL 没有该合同 | 不移植 |
| 1s、6h timeframe | HL 不支持 | 保持预检拒绝 |
| REST 超过 requested limit 即错误 | HL 会出现边界重叠 | 保持 request-kind-aware truncation |

---

# 2. Hyperliquid 有、Binance 应该补的机制

直接应该复制的运行机制很少。Binance 当前协议实现更完整。

## 2.1 Provider capability 描述：应该补到公共接口

Hyperliquid 已有明确的前置能力检查：

```rust
reject_unsupported_timeframe(timeframe)
```

`src/provider/hyperliquid.rs:2947-2975`

Binance 当前支持 `Timeframe::ALL`，所以不需要额外拒绝；但公共接口应该表达能力：

```rust
struct ProviderCapabilities {
    markets: &'static [Market],
    timeframes: &'static [Timeframe],
    history_page_limit: u16,
    connection_rotation: Option<Duration>,
}
```

用途：

- UI 在发起网络请求前就能过滤不支持的周期。
- live gap engine 不再硬编码 `GAP_PAGE_LIMIT = 1000`。
- 第三个 provider 不需要在不同调用入口重复做 capability check。

Binance 实现简单返回全部周期即可。

---

## 2.2 Display identity 与 wire identity 分离：Binance 已经具备基础

Hyperliquid 必须处理：

- `BTC` 展示为 `UBTC/USDC`，wire 为 `@142`
- HYPE 为 `@107`
- PURR 为 `PURR/USDC`
- HIP-3 为 `dex:COIN`

但公共 `Instrument` 已经分离了：

- `display_pair`
- `provider_symbol`

`src/model.rs:131-191`

所以 Binance 不需要再增加 Hyperliquid 式映射。Binance 当前的 wire symbol 正好等于 `BASEQUOTE`，只是该扩展点暂时是 identity mapping。

结论：**保留接口，不增加 Binance 特例。**

---

## 2.3 动态订阅不应加入当前 Binance feed

Binance 官方也支持发送 `SUBSCRIBE` 控制消息，但当前架构是一条 feed 对应一个 market，URL-bound raw stream 更简单：

- 建连时市场身份明确
- 没有 unsubscribe/subscribe 交错
- 不会混入旧市场的残留事件
- 市场切换本来就准备 replacement resources 后原子提交

只有未来明确要做“一条连接 multiplex 多市场”时，才值得改为动态订阅。现在增加只会扩大状态空间。

---

## 2.4 以下 Hyperliquid 机制不应移植到 Binance

| Hyperliquid 机制 | 原因 |
|---|---|
| JSON ping/pong | Binance 使用 WebSocket Ping/Pong frame |
| POST `/info` candle window | Binance klines 原生接受 `limit/startTime/endTime` |
| `now_ms` 注入 | Binance latest 请求不需要墙上时间 |
| extra-row truncation | Binance 合同明确最大 `limit`，严格拒绝协议漂移更好 |
| JSON number/string 双格式 decimal | Binance 文档规定价格字段为 string，严格解析更利于发现漂移 |
| REST candle symbol/interval 回显校验 | Binance REST 行本身没有 symbol/interval |
| HIP-3 venue | Binance 当前没有对应市场维度 |
| 不支持 1s/6h | Binance 实际支持 |

---

# 3. 应该抽出的共用机制

`src/provider/mod.rs` 已经抽出了正确的一部分：

- `MarketDataProvider`
- `LiveRequest` / `LiveFeed`
- accepted watermark channel
- reconciliation ack channel
- producer completion
- rate gate

`src/provider/mod.rs:23-626`

下一步应延伸这个边界，而不是创建一个包含所有协议细节的“大 provider trait”。

## 3.1 `provider/runtime/websocket.rs`

抽出：

- `WsConfig`
- `RawWebSocket`
- read/write/flush pump
- automatic control-frame flush
- stalled-write deadline
- inactivity deadline
- decoded outcome queue
- WebSocket error映射
- loopback URL 安全验证
- connect config validation

当前基本完全重复：

- Binance：`src/provider/binance.rs:153-931`
- Hyperliquid：`src/provider/hyperliquid.rs:150-864`

建议用内部泛型 codec，避免逐帧 trait-object dispatch：

```rust
trait WsCodec: Send {
    fn decode(
        &mut self,
        message: Message,
        market: &MarketContext,
        output: &mut VecDeque<Result<LiveFrame, ProviderError>>,
    );
}
```

使用 `&mut self` 和输出队列的理由：

- Binance codec 可以无状态。
- Hyperliquid 可以跟踪隐式 finality。
- 一条输入消息将来可能产生“上一根 closed + 当前 open”两个 candle。
- 不需要为每帧额外分配临时 `Vec`。

把 `DecodedFrame::ServerShutdown` 改成 provider-neutral 的 `ReconnectRequested`；Binance codec 再把 `serverShutdown` 映射进去。

---

## 3.2 `provider/runtime/live.rs`

抽出整套 live state machine：

- `supervise_live`
- `run_generation`
- `connected_loop`
- gap REST/WS reconciliation
- accepted watermark 追赶
- reconciliation revision/ack
- exponential backoff
- generation invalidation
- queue saturation处理
- `EventEmitter`
- `EmergencyBarrier`
- cancellation precedence
- terminal/recoverable classification
- `open_live` 中的 filtered event stream

当前重复范围：

- Binance：`src/provider/binance.rs:1276-2606`
- Hyperliquid：`src/provider/hyperliquid.rs:1228-2607`

shared engine 只需要几个内部 hook：

```text
validate_request()
connect_ready_socket()
history()
rate_gate()
live_config()
history_page_limit()
connection_rotation()
```

其中 `connect_ready_socket()` 的语义必须是：

> 返回时，provider-specific WebSocket subscription 已确认建立。

这样 Binance 的 URL handshake 和 Hyperliquid 的 subscribe+ack 都能保持在 provider 内。

---

## 3.3 `provider/runtime/http.rs`

抽出：

- `reqwest::Client` 安全构造
- `no_proxy`
- redirect disabled
- User-Agent
- request cancellation
- timeout/transport error映射
- capped body reader
- rate-gate wait
- rate-gate deadline max/absorbing规则
- 3xx/4xx/5xx 的公共处理骨架

当前相同的 `read_capped`：

- `src/provider/binance.rs:2696-2745`
- `src/provider/hyperliquid.rs:2699-2748`

建议形成：

```rust
struct HttpRuntime {
    client: Client,
    clock: Arc<dyn Clock>,
    gate_sender: RateGateSender,
    gate_snapshot: RateGateSnapshot,
    body_limit: usize,
    rate_limit_fallback: Duration,
}
```

仍留在 provider 内：

- URL、method、query/body
- client error body解析
- invalid-symbol 判断
- rate-limit status policy
- success payload decoder

不要把 Binance `-1121` 或 Hyperliquid `{error: ...}` 放进公共 HTTP 层。

---

## 3.4 Provider policy/capabilities

需要区分两类信息：

### 公共可见 capability

```text
supported markets
supported timeframes
history page size
```

供 UI、history coordinator 和 live engine 使用。

### 仅内部 protocol policy

```text
subscription style
heartbeat style
finality evidence
rate-limit semantics
connection rotation
payload retention policy
```

不要把这些全部公开进 `MarketDataProvider`；否则公共 trait 会成为协议细节垃圾场。

---

## 3.5 通用化 `ProviderRegistry`

当前 registry 硬编码：

```rust
binance: Arc<BinanceProvider>
hyperliquid: Option<Arc<HyperliquidProvider>>
injected: Option<Arc<dyn MarketDataProvider>>
```

`src/provider/mod.rs:44-104`

这意味着每新增 provider 都要：

- 加字段
- 加 `with_xxx`
- 修改 `match`
- 修改 test injection 特例

建议改成：

```rust
BTreeMap<ProviderId, Arc<dyn MarketDataProvider>>
```

原因：

- `ProviderId` 已实现 `Ord`
- provider 数量小，不需要引入复杂 registry abstraction
- 构造时拒绝重复 ID
- `get(&ProviderId)`，避免当前调用方为了 lookup clone `ProviderId`
- 测试 provider 直接注册，不需要 `injected` 特殊字段
- 删除只为 Binance 存在的 `binance()` accessor

本地 canonicalization metadata 仍然独立；不要因为 transport 没注册就让 CLI 无法识别已知 provider。这一点保持现有设计。

---

## 3.6 Shared contract tests

抽出后测试应分两层：

### Shared runtime contract

覆盖：

- reconciliation
- generation lifecycle
- cancellation priority
- ack/watermark
- queue saturation
- backoff
- socket pump
- capped body
- rate-gate absorbing/max deadline

运行一次即可，不再以 Binance 名义拥有这些测试。

### Provider protocol tests

Binance 保留：

- Spot/USD-M hosts 和 paths
- stream URL
- `x` finality
- `serverShutdown`
- 418/429
- `-1121`
- 12-field array codec

Hyperliquid 保留：

- wire coin remap
- unsupported timeframes
- candle window
- boundary overlap/truncation
- subscribe ack
- JSON ping/pong
- implicit finality
- REST/WS symbol+interval echo
- Hyperliquid error payload

---

# 建议实施顺序

1. **先补 Hyperliquid 协议测试**：subscribe ack、JSON heartbeat、finality、rate-limit。
2. **抽 WebSocket transport 和 EventEmitter**：几乎纯机械移动，风险最低。
3. **抽 live reconciliation engine**：以现有 Binance 42 个 live tests 作为迁移保护。
4. **抽 HTTP runtime 和 rate-limit policy**。
5. **加入 ProviderCapabilities**。
6. **最后通用化 Registry**。
7. 新增第三个 provider 时，只实现 endpoint、codec、canonicalization 和 protocol policy。

不要第一步就做一个巨大的 `ProviderAdapter` trait；先移动字节级相同的代码，再让真实差异决定最小 hook 集合。

## 验证

- 现有 6 个 provider 测试套件全部通过：**99 passed**。
- 实际 Hyperliquid 主网 WebSocket 验证了 `subscriptionResponse → pong → candle`。
- 实际 `candleSnapshot` 验证了 1000-interval 窗口可能返回 1001 条，说明 Hyperliquid truncation 不能改成 Binance 式严格拒绝。
- 业务代码未改；按项目规则只在 `AGENTS.md` 记录了两条可复用的 Hyperliquid 协议经验。

官方依据：

- [Binance REST API](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md)
- [Binance WebSocket Streams](https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md)
- [Hyperliquid Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
- [Hyperliquid WebSocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
- [Hyperliquid WebSocket subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
- [Hyperliquid rate limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)
- [Hyperliquid 官方 Python SDK WebSocket 实现](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/websocket_manager.py)
