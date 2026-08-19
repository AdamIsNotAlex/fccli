#![cfg(feature = "test-transport")]

use std::sync::Arc;

use fccli::{
    clock::ManualClock,
    error::ProviderError,
    model::{Instrument, Market, MonoInstant, ProviderId, Timeframe},
    provider::{
        LiveRequest, MarketDataProvider,
        hyperliquid::{HyperliquidProvider, HyperliquidTestConfig},
        accepted_watermark_channel, reconcile_ack_channel,
    },
};
use tokio_util::sync::CancellationToken;

fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(MonoInstant::ZERO))
}

#[tokio::test]
async fn unsupported_timeframe_open_live_fails_before_connect() {
    let mut config = HyperliquidTestConfig::loopback("http://127.0.0.1:9");
    config.now_ms = Some(1_704_067_200_000);
    let provider = HyperliquidProvider::new_test_live(
        config.with_websocket_base("ws://127.0.0.1:9"),
        clock(),
    )
    .expect("provider");
    let instrument = Instrument::new(
        ProviderId::new("hyperliquid").expect("provider"),
        Market::Perpetual,
        "BTC",
        "USDC",
        "BTC",
    )
    .expect("instrument");
    let (_watermark_tx, watermark_rx) = accepted_watermark_channel(None);
    let (_ack_tx, ack_rx) = reconcile_ack_channel();
    let error = match provider
        .open_live(LiveRequest {
            instrument,
            timeframe: Timeframe::Second1,
            startup_watermark: None,
            accepted_watermark_rx: watermark_rx,
            reconcile_ack_rx: ack_rx,
            cancellation: CancellationToken::new(),
        })
        .await
    {
        Ok(_) => panic!("1s is unsupported"),
        Err(error) => error,
    };
    assert!(
        matches!(error, ProviderError::Configuration(message) if message.contains("1s or 6h")),
        "{error}"
    );
}
