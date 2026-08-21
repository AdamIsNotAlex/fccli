# Is it possible to add support to query NASDAQ/NYSE data?

Yes. The rendering and coordination layers are already provider-neutral through `MarketDataProvider`, so NASDAQ/NYSE-listed equities can be added without replacing the chart.

**Decision:** add an equities data-provider adapter, not separate “NASDAQ” and “NYSE” transports. Those are listing venues; one vendor API can usually supply both.

Required changes:

- Add `Market::Equity` in `src/model.rs`.
- Extend instrument parsing for equity symbols:
  - `AAPL`
  - `NYSE:IBM`
  - class shares such as `BRK.B`
- Avoid the existing crypto quote canonicalization. An equity instrument is a ticker plus optional venue, not an `AAPL/USD` pair.
- Implement a provider under `src/provider/`, conforming to:
  - `canonicalize`
  - `history`
  - `open_live`
  - `capabilities`
  - `rate_gate`
- Register it in `src/main.rs`.
- Map vendor OHLCV and streaming aggregate messages into the existing `Candle` and `MarketEvent` types.
- Add supported-timeframe and market-session handling.

The main complication is the data source:

| Approach | History | Live | Main limitation |
|---|---:|---:|---|
| Alpaca | Yes | Yes | Account and feed entitlement requirements |
| Massive/Polygon | Yes | Yes | Paid plans for meaningful real-time coverage |
| Databento | Yes | Yes | Usage-based commercial data |
| Nasdaq Data Link | Mostly | Limited | Better suited to datasets than live charting |
| Yahoo-style unofficial endpoint | Yes | Weak | Unstable and unsuitable as a production transport |

I would use **Alpaca** for a straightforward first integration, or **Databento/Massive** when authoritative market coverage matters.

Important differences from crypto:

- Exchange licensing and real-time entitlements.
- Regular, pre-market, and after-hours sessions.
- Overnight gaps and market holidays.
- Split/dividend-adjusted versus raw candles.
- SIP consolidated data versus exchange-specific feeds.
- Equity symbol punctuation currently conflicts with `validate_component`, which only accepts ASCII alphanumerics.
- The existing `Instrument` always requires a quote and renders `base/quote`; that invariant should be redesigned for asset classes rather than faking equities as `AAPL/USD`.

A sensible initial scope is **historical and delayed US equity candles**, followed by live streaming once credentials and feed entitlements are selected.
