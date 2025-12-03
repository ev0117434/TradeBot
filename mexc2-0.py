import asyncio
import json
import time

import websockets  # pip install websockets

# ================= БАЗОВЫЕ НАСТРОЙКИ =================

SPOT_WS_URL = "wss://wbs-api.mexc.com/ws"
FUTURES_WS_URL = "wss://contract.mexc.com/edge"

SPOT_SYMBOLS_FILE = "dif type of pairs/actually all pomenshe/mexc_spot_all.txt"       # Ваши 614 пар
FUTURES_SYMBOLS_FILE = "dif type of pairs/actually all pomenshe/mexc_futures_all.txt" # Ваши 721 контрактов

SPOT_PING_INTERVAL = 20
FUTURES_PING_INTERVAL = 20

# ================= УТИЛИТЫ =================

def load_symbols(path: str) -> set[str]:
    symbols = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                symbols.add(s)
    return symbols

def current_ts_ms() -> int:
    return int(time.time() * 1000)

def handle_price(exchange: str,
                 market: str,
                 symbol: str,
                 bid: float,
                 ask: float,
                 ts: int | None) -> None:
    symbol = symbol.replace("_", "")  # Убираем "_" только перед выводом
    if ts is None or ts == 0:
        ts = current_ts_ms()
    line = f"{exchange},{market},{symbol},{bid},{ask},{ts}"
    print(line, flush=True)

# ================= SPOT: 2 WS, allBookTicker =================

async def spot_ping_loop(ws: websockets.WebSocketClientProtocol, conn_id: int) -> None:
    while True:
        await asyncio.sleep(SPOT_PING_INTERVAL)
        try:
            await ws.send(json.dumps({"method": "PING"}))
        except Exception:
            break

async def run_spot_connection(conn_id: int, symbols: set[str]) -> None:
    while True:
        try:
            async with websockets.connect(
                SPOT_WS_URL,
                ping_interval=None,
            ) as ws:
                sub_msg = {
                    "method": "SUBSCRIPTION",
                    "params": ["spot@public.allBookTicker.v3.api"],
                }
                await ws.send(json.dumps(sub_msg))

                asyncio.create_task(spot_ping_loop(ws, conn_id))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("c") != "spot@public.allBookTicker.v3.api":
                        continue

                    data = msg.get("d", [])
                    ts = msg.get("t")

                    for it in data:
                        symbol = it.get("s")
                        if symbol not in symbols:
                            continue

                        bid_str = it.get("b")
                        ask_str = it.get("a")

                        if not bid_str or not ask_str:
                            continue

                        try:
                            bid = float(bid_str)
                            ask = float(ask_str)
                        except ValueError:
                            continue

                        handle_price(
                            exchange="MEXC",
                            market="SPOT",
                            symbol=symbol,
                            bid=bid,
                            ask=ask,
                            ts=int(ts) if ts else None,
                        )

        except Exception as e:
            print(f"SPOT[{conn_id}] error: {e}, reconnect in 5s", flush=True)
            await asyncio.sleep(5)

# ================= FUTURES: 2 WS, sub.tickers =================

async def futures_ping_loop(ws: websockets.WebSocketClientProtocol, conn_id: int) -> None:
    while True:
        await asyncio.sleep(FUTURES_PING_INTERVAL)
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            break

async def run_futures_connection(conn_id: int, contracts: set[str]) -> None:
    while True:
        try:
            async with websockets.connect(
                FUTURES_WS_URL,
                ping_interval=None,
            ) as ws:
                sub_msg = {
                    "method": "sub.tickers",
                    "param": {},
                    "gzip": False
                }
                await ws.send(json.dumps(sub_msg))

                asyncio.create_task(futures_ping_loop(ws, conn_id))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    if msg.get("channel") != "push.tickers":
                        continue

                    data = msg.get("data", [])
                    for it in data:
                        symbol = it.get("symbol")
                        if symbol not in contracts:
                            continue

                        last = it.get("lastPrice")
                        bid = it.get("maxBidPrice")
                        ask = it.get("minAskPrice")

                        if last is None:
                            continue

                        try:
                            last_f = float(last)
                        except ValueError:
                            continue

                        try:
                            bid_f = float(bid) if bid is not None else last_f
                        except ValueError:
                            bid_f = last_f

                        try:
                            ask_f = float(ask) if ask is not None else last_f
                        except ValueError:
                            ask_f = last_f

                        ts = it.get("timestamp")
                        handle_price(
                            exchange="MEXC",
                            market="FUTURES",
                            symbol=symbol,
                            bid=bid_f,
                            ask=ask_f,
                            ts=int(ts) if ts else None,
                        )

        except Exception as e:
            print(f"FUTURES[{conn_id}] error: {e}, reconnect in 5s", flush=True)
            await asyncio.sleep(5)

# ================= MAIN =================

async def main() -> None:
    spot_symbols = load_symbols(SPOT_SYMBOLS_FILE)
    futures_contracts = load_symbols(FUTURES_SYMBOLS_FILE)

    tasks = [
        asyncio.create_task(run_spot_connection(1, spot_symbols)),
        asyncio.create_task(run_spot_connection(2, spot_symbols)),
        asyncio.create_task(run_futures_connection(1, futures_contracts)),
        asyncio.create_task(run_futures_connection(2, futures_contracts)),
    ]

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())