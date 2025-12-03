import asyncio
import json
import ssl
import time
import uuid
import zlib
from pathlib import Path

import websockets

# ================== НАСТРОЙКИ ==================

EXCHANGE_NAME = "BingX"

# WebSocket URL для spot и фьючерсов (USDT-M / USDⓢ-M)
SPOT_WS_URL = "wss://open-api-ws.bingx.com/market"
FUTURES_WS_URL = "wss://open-api-swap.bingx.com/swap-market"

# Файлы со списками символов (по одному в строке: BTC-USDT, ETH-USDT, ...)
SPOT_SYMBOLS_FILE = "dif type of pairs/actually all pomenshe/bingx_spot_all.txt"
FUTURES_SYMBOLS_FILE = "dif type of pairs/actually all pomenshe/bingx_futures_all.txt"

# Максимум 200 dataType на одно WS для spot по правилам BingX
MAX_SYMBOLS_PER_CONN = 200

# ================== УТИЛИТЫ ==================

def load_symbols(path: str) -> list[str]:
    file = Path(path)
    if not file.exists():
        return []
    symbols: list[str] = []
    with file.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            symbols.append(s)
    return symbols


def chunk_list(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def decompress_message(msg) -> str | None:
    """
    BingX шлёт gzip/zlib-сжатые payload'ы.
    Пытаемся разжать разными вариантами, если не вышло — игнорируем.
    """
    if isinstance(msg, str):
        return msg
    if not isinstance(msg, (bytes, bytearray)):
        return None

    data = bytes(msg)
    for wbits in (16 + zlib.MAX_WBITS, zlib.MAX_WBITS, 0):
        try:
            text = zlib.decompress(data, wbits).decode("utf-8")
            return text
        except Exception:
            continue
    return None


def parse_ticker_json(raw: str):
    """
    Пытаемся вытащить [symbol, bid, ask, ts] из JSON BingX.
    Формат сообщений в стиле:
    {
      "id": "...",
      "dataType": "BTC-USDT@ticker",
      "data": {
        "b": "12345.6",   # best bid
        "a": "12346.1",   # best ask
        "E": 1710000000000,
        ...
      }
    }
    Поля могут отличаться, поэтому берём несколько вариантов ключей.
    """
    obj = json.loads(raw)
    data_type = obj.get("dataType", "") or obj.get("topic", "")
    symbol = data_type.split("@", 1)[0] if "@" in data_type else data_type or obj.get("symbol", "")

    data = obj.get("data") or obj

    # bid / ask с несколькими вариантами ключей
    def get_float(d: dict, *keys: str) -> float | None:
        for k in keys:
            if k in d and d[k] is not None:
                try:
                    return float(d[k])
                except Exception:
                    continue
        return None

    bid = get_float(data, "b", "bid", "bestBidPrice")
    ask = get_float(data, "a", "ask", "bestAskPrice")

    # timestamp
    ts = None
    for k in ("E", "time", "ts", "T"):
        if k in data:
            try:
                ts = int(data[k])
                break
            except Exception:
                continue
    if ts is None:
        ts = int(time.time() * 1000)

    if not symbol or bid is None or ask is None:
        return None

    return symbol, bid, ask, ts


# ================== ОСНОВНАЯ ЛОГИКА WS ==================

async def run_ws_group(
    market: str,
    ws_url: str,
    symbols: list[str],
):
    """
    Один тип рынка (spot / futures), много WS-подключений по 200 символов максимум.
    """
    batches = chunk_list(symbols, MAX_SYMBOLS_PER_CONN)
    tasks = []

    for batch_idx, batch in enumerate(batches):
        task = asyncio.create_task(
            run_single_connection(market=market, ws_url=ws_url, symbols=batch, conn_id=batch_idx)
        )
        tasks.append(task)

    if tasks:
        await asyncio.gather(*tasks)


async def run_single_connection(
    market: str,
    ws_url: str,
    symbols: list[str],
    conn_id: int,
):
    """
    Один WebSocket, подписка на группу символов.
    Авто-reconnect бесконечным циклом.
    """
    ssl_ctx = ssl.create_default_context()

    while True:
        try:
            print(f"[{EXCHANGE_NAME}][{market}][conn={conn_id}] connecting to {ws_url} with {len(symbols)} symbols")
            async with websockets.connect(ws_url, ssl=ssl_ctx) as ws:
                # Подписки: по одному dataType на сообщение
                for sym in symbols:
                    sub = {
                        "id": str(uuid.uuid4()),
                        "reqType": "sub",
                        # @ticker даёт 24h-тикер с bid/ask/last для perp; для spot формат аналогичный
                        "dataType": f"{sym}@ticker",
                    }
                    await ws.send(json.dumps(sub))

                print(f"[{EXCHANGE_NAME}][{market}][conn={conn_id}] subscribed")

                async for msg in ws:
                    text = decompress_message(msg)
                    if text is None:
                        continue

                    # App-уровень Ping/Pong от BingX
                    if text == "Ping":
                        await ws.send("Pong")
                        continue

                    if not text or text[0] not in "{[":
                        # игнорируем не-JSON
                        continue

                    try:
                        parsed = parse_ticker_json(text)
                    except Exception:
                        continue

                    if not parsed:
                        continue

                    symbol, bid, ask, ts = parsed

                    # Модификация символа: убрать тире и SWAP
                    symbol = symbol.replace('-SWAP', '').replace('SWAP', '').replace('-', '')

                    # Формат вывода: без скобок, кавычек, через запятые
                    print(f"{EXCHANGE_NAME}, {market}, {symbol}, {bid}, {ask}, {ts}")

        except Exception as e:
            print(f"[{EXCHANGE_NAME}][{market}][conn={conn_id}] error: {e!r}, reconnect in 3s")
            await asyncio.sleep(3)


# ================== ENTRYPOINT ==================

async def main():
    spot_symbols = load_symbols(SPOT_SYMBOLS_FILE)
    fut_symbols = load_symbols(FUTURES_SYMBOLS_FILE)

    print(f"[INIT] Spot symbols: {len(spot_symbols)}, Futures symbols: {len(fut_symbols)}")

    tasks = []
    if spot_symbols:
        tasks.append(asyncio.create_task(
            run_ws_group("SPOT", SPOT_WS_URL, spot_symbols)
        ))
    if fut_symbols:
        tasks.append(asyncio.create_task(
            run_ws_group("FUTURES", FUTURES_WS_URL, fut_symbols)
        ))

    if not tasks:
        print("[INIT] No symbols loaded, nothing to do")
        return

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())