import ccxt


def fetch_spot_symbols() -> list[str]:
    """Все спотовые пары Binance."""
    exchange = ccxt.binance()
    markets = exchange.load_markets()

    symbols: list[str] = []
    for m in markets.values():
        if m.get("spot") and (m.get("active") is not False):
            s = m["symbol"]
            # Преобразуем 'BTC/USDT' -> 'BTCUSDT'
            s = s.replace("/", "")
            # На всякий случай обрежем всё после ':'
            if ":" in s:
                s = s.split(":", 1)[0]
            symbols.append(s)

    # Уберем дубли и отсортируем
    return sorted(set(symbols))


def fetch_futures_symbols() -> list[str]:
    """Все USDT-маржинальные фьючерсы Binance (USDM)."""
    exchange = ccxt.binanceusdm()
    markets = exchange.load_markets()

    symbols: list[str] = []
    for m in markets.values():
        # contract == True → дериватив (perp/фьючерс)
        if m.get("contract") and (m.get("active") is not False):
            s = m["symbol"]
            # Например 'BTC/USDT:USDT' -> 'BTCUSDT'
            s = s.replace("/", "")
            if ":" in s:
                s = s.split(":", 1)[0]
            symbols.append(s)

    return sorted(set(symbols))


def save_list_to_txt(symbols: list[str], filename: str) -> None:
    """Сохраняем список пар в .txt, по одной паре в строке."""
    with open(filename, "w", encoding="utf-8") as f:
        for s in symbols:
            f.write(s + "\n")


if __name__ == "__main__":
    spot_symbols = fetch_spot_symbols()
    futures_symbols = fetch_futures_symbols()

    save_list_to_txt(spot_symbols, "binance_spot_all.txt")
    save_list_to_txt(futures_symbols, "binance_futures_all.txt")

    print(f"Spot:   {len(spot_symbols)} пар сохранено в binance_spot_all.txt")
    print(f"Futures:{len(futures_symbols)} пар сохранено в binance_futures_all.txt")
