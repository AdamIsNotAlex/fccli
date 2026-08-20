//! Integration-test-only transport harness.

pub use super::runtime::emitter::EventEmitterTestFacade;
pub use super::runtime::http::{
    HttpRuntime, RateLimitDecision, StatusDisposition, classify_status, is_cancelled,
};
pub use super::runtime::live::{
    CONTROL_CAPACITY, ConnectionRotation, EMERGENCY_CONTROL_CAPACITY,
    FIRST_KLINE_HANDSHAKE_TIMEOUT, KEYED_CANDLE_CAPACITY, LiveAdapter, LiveCompletionDisposition,
    LiveConfig, LiveErrorClassification, LiveErrorDisposition, LiveInBandEventDisposition,
    LiveInputClassification, LiveRateGate, LiveSocket, LiveSocketEvent, LiveSupervisorConfig,
    MARKET_EVENT_CHANNEL_CAPACITY, ProcessBlockPolicy, RECONCILE_ACK_TIMEOUT, ReconciliationLimits,
    ReconciliationPolicy, classify_live_error_for_test, classify_live_input_for_test,
    gap_target_within_generation_span_for_test, open_live,
    reconciliation_distinct_key_allowed_for_test, reconciliation_page_guard_for_test,
};
pub use super::runtime::websocket::{
    CloseFlushTestHook, DecodedFrame, HeartbeatTestHook, ReadinessDecodedAckTestHook,
    ReadinessDrainBudgetTestHook, SubscribeFlushTestHook, WS_FRAME_SIZE, WS_MAX_WRITE_BUFFER_SIZE,
    WS_MESSAGE_INACTIVITY_TIMEOUT, WS_MESSAGE_SIZE, WS_READ_BUFFER_SIZE, WS_STALLED_WRITE_TIMEOUT,
    WS_WRITE_BUFFER_SIZE, WsConfig, flush_raw_websocket, read_raw_websocket, send_raw_websocket,
};
pub use super::{binance::BinanceDecoded, hyperliquid::HyperliquidDecoded};

pub fn validate_loopback_websocket_base(
    base_url: &str,
) -> Result<reqwest::Url, crate::error::ProviderError> {
    super::runtime::websocket::validate_websocket_base(base_url, true)
}
