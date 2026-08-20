//! Integration-test-only transport harness.

pub use super::runtime::emitter::EventEmitterTestFacade;
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
