# file: cross_spot_futures_pairs.py

EXCH_FILES = {
    "binance": {
        "spot": "parsing/binance_spot_all.txt",
        "futures": "parsing/binance_futures_all.txt",
    },
    "bybit": {
        "spot": "parsing/bybit_spot_all.txt",
        "futures": "parsing/bybit_futures_all.txt",
    },
    "mexc": {
        "spot": "parsing/mexc_spot_all.txt",
        "futures": "parsing/mexc_futures_all.txt",
    },
    "okx": {
        "spot": "parsing/okx_spot_all.txt",
        "futures": "parsing/okx_futures_all.txt",
    },
    "bingx": {
        "spot": "parsing/bingx_spot_all.txt",
        "futures": "parsing/bingx_futures_all.txt",
    },
}


def normalize_symbol(exchange: str, market: str, symbol: str) -> str:
    s = symbol.strip().upper()
    if not s:
        return ""

    if exchange == "bingx":
        # THETA-USDT -> THETAUSDT (spot и futures)
        s = s.replace("-", "")

    elif exchange == "okx":
        if market == "spot":
            # TRX-USDT -> TRXUSDT
            s = s.replace("-", "")
        elif market == "futures":
            # BTC-USDT-SWAP -> BTCUSDT
            # LTC-USD-SWAP  -> LTCUSD
            s = s.replace("-", "")
            s = s.replace("SWAP", "")

    # Остальные биржи уже в виде ATAUSDT и т.п.
    return s


def load_symbols(exchange: str, market: str, filename: str) -> set:
    symbols = set()
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            norm = normalize_symbol(exchange, market, line)
            if norm:
                symbols.add(norm)
    return symbols


def main():
    # читаем и нормализуем все списки
    data = {}  # data[exchange]["spot"|"futures"] = set(...)
    for ex, files in EXCH_FILES.items():
        data[ex] = {}
        data[ex]["spot"] = load_symbols(ex, "spot", files["spot"])
        data[ex]["futures"] = load_symbols(ex, "futures", files["futures"])

    exchanges = list(EXCH_FILES.keys())

    # все уникальные комбинации SPOT(A) vs FUTURES(B), A != B
    for spot_ex in exchanges:
        for fut_ex in exchanges:
            if spot_ex == fut_ex:
                continue

            common = data[spot_ex]["spot"] & data[fut_ex]["futures"]

            print(f"{spot_ex.upper()} SPOT  vs  {fut_ex.upper()} FUTURES")
            print(f"Общих пар: {len(common)}")
            if common:
                for sym in sorted(common):
                    print(sym)
            print("-" * 40)


if __name__ == "__main__":
    main()
