# fccli

Terminal candlestick charts for Binance Spot markets.

_Example: `fccli btc 1h`_

![fccli rendering a BTC/USDT 1h candlestick chart](assets/fccli-demo.png)

## Install

Requires Rust 1.96 or newer.

```sh
cargo install --git https://github.com/AdamIsNotAlex/fccli
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

- `:`: open the market/timeframe command line
- `A` / `D` or `←` / `→`: pan through time
- `W` / `S` or `↑` / `↓`: pan the price range
- `h` / `H`: zoom time in / out
- `v` / `V`: zoom price in / out
- `End`: return to live data
- `r`: reset the view
- `q`, `Esc`, `Ctrl-C`, or `Ctrl-D`: quit

While the command line is open, enter a target using the same instrument and timeframe syntax as
the startup command, then press `Enter`:

```text
:btc 1m
:binance:btc/usdc 1h
:BTCUSDT 1M
```

The current chart remains live while the new market loads. A successful switch resets the chart
view to the new market; if loading fails, the current chart remains active and the error is shown
in the footer. Submitting the currently displayed market and timeframe is a no-op.

Command-line editing supports `Backspace`, `Delete`, `←`, `→`, `Home`, and `End`. `Esc` cancels
command entry without quitting; `Ctrl-C` and `Ctrl-D` always quit.

Run `fccli --help` for command-line help.
