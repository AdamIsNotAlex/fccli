# fccli

Terminal candlestick charts for Binance Spot markets.

![fccli rendering a BTC/USDT 1h candlestick chart](assets/fccli-demo.png)

_Example: `fccli btc 1h`_

## Install

Requires Rust 1.96 or newer.

```sh
cargo install --path .
```

## Usage

```text
fccli [OPTIONS] <INSTRUMENT> <TIMEFRAME>
```

By default, `fccli` renders one chart snapshot. Add `-i` or `--interactive` to open the interactive terminal UI.

```sh
fccli btc 1m
fccli binance:btc/usdc 1h
fccli BTCUSDT 1M --interactive
```

An instrument may be an asset (`btc`, quoted in USDT), pair (`btc/usdc` or `btc-usdc`), Binance symbol (`BTCUSDT`), or provider-prefixed pair (`binance:btc/usdc`).

Supported timeframes: `1s`, `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `8h`, `12h`, `1d`, `3d`, `1w`, `1M`. Timeframes are case-sensitive.

### Interactive controls

- `A` / `D` or `←` / `→`: pan through time
- `W` / `S` or `↑` / `↓`: pan the price range
- `h` / `H`: zoom time in / out
- `v` / `V`: zoom price in / out
- `End`: return to live data
- `r`: reset the view
- `q`, `Esc`, or `Ctrl-C`: quit

Run `fccli --help` for command-line help.
