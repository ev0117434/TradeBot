import asyncio
import json
import time

import websockets

# ===== НАСТРОЙКИ =====
SPOT_SYMBOLS_FILE = "binance_spot_all.txt"
FUTURES_SYMBOLS_FILE = "binance_futures_all.txt"

SPOT_URL_BASE = "wss://stream.binance.com:9443/stream?streams="
FUTURES_URL_BASE = "wss://fstream.binance.com/stream?streams="

# ===== ХРАНИЛИЩЕ ЦЕН В ПАМЯТИ =====
# ключ: (market, symbol), значение: (bid, ask, ts)
prices: dict[tuple[str, str], tuple[str, str, int]] = {}


def load_symbols(path: str) -> list[str]:
    """Чтение списка символов из txt (по одному в строке)."""
    symbols = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                symbols.append(s)
    return symbols


def build_stream_url(base: str, symbols: list[str]) -> str:
    """Формирует combined-stream URL bookTicker для списка символов."""
    streams = [f"{s.lower()}@bookTicker" for s in symbols]
    return base + "/".join(streams)


async def run_ws(market: str, url: str):
    """Подключение к WS и обновление словаря цен."""
    while True:
        try:
            print(f"{market.upper()}: connect {url}")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                print(f"{market.upper()}: connected")
                async for msg in ws:
                    data = json.loads(msg)
                    d = data.get("data", {})
                    symbol = d.get("s")
                    bid = d.get("b")
                    ask = d.get("a")
                    if not symbol or bid is None or ask is None:
                        continue

                    ts = d.get("T") or d.get("E") or int(time.time() * 1000)
                    prices[(market, symbol)] = (bid, ask, ts)
        except Exception as e:
            print(f"{market.upper()}: error {e}, reconnect in 5s")
            await asyncio.sleep(5)


async def main():
    spot_symbols = load_symbols(SPOT_SYMBOLS_FILE)
    futures_symbols = load_symbols(FUTURES_SYMBOLS_FILE)

    spot_url = build_stream_url(SPOT_URL_BASE, spot_symbols)
    futures_url = build_stream_url(FUTURES_URL_BASE, futures_symbols)

    tasks = [
        asyncio.create_task(run_ws("spot", spot_url)),
        asyncio.create_task(run_ws("futures", futures_url)),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
