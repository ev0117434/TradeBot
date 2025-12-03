import asyncio
import json
import time

import websockets  # pip install websockets

# ================= БАЗОВЫЕ НАСТРОЙКИ =================

SPOT_WS_URL = "wss://wbs-api.mexc.com/ws"
FUTURES_WS_URL = "wss://contract.mexc.com/edge"

SPOT_SYMBOLS_FILE = "dif type of pairs/actually all pomenshe/mexc_spot_all.txt"       # 2059 пар, по одной в строке: BTCUSDT
FUTURES_SYMBOLS_FILE = "dif type of pairs/actually all pomenshe/mexc_futures_all.txt" # 826 контрактов: BTC_USDT

SPOT_TIMEZONE = "UTC+3"  # для miniTickers, влияет только на % изменения, не на цену

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
    """
    Здесь только формируем строку и печатаем.
    Никаких файлов/сокетов внутри WS-цикла, чтобы не тормозить.
    """
    if ts is None or ts == 0:
        ts = current_ts_ms()
    line = f"{exchange},{market},{symbol},{bid},{ask},{ts}"
    # print(line, flush=True)  # Original print removed for file output
    # Если нужно писать в файл — делай это в отдельном потоке/скрипте.
    # Простой вариант (НЕ РЕКОМЕНДУЮ внутри WS-цикла):
    with open("mexc_prices.txt", "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ================= SPOT: 2 WS, miniTickers =================

async def spot_ping_loop(ws: websockets.WebSocketClientProtocol, conn_id: int) -> None:
    while True:
        await asyncio.sleep(SPOT_PING_INTERVAL)
        try:
            await ws.send(json.dumps({"method": "PING"}))
        except Exception:
            # соединение умерло, выходим из ping-лупа
            break


async def run_spot_connection(conn_id: int, symbols: set[str]) -> None:
    """
    Один WS-коннект на miniTickers (все пары каждые ~3 с).
    Мы держим два таких коннекта (conn_id=1 и 2) для резервирования.
    """
    while True:
        try:
            async with websockets.connect(
                SPOT_WS_URL,
                ping_interval=None,  # управляем PING сами
            ) as ws:
                sub_msg = {
                    "method": "SUBSCRIPTION",
                    "params": [f"spot@public.miniTickers.v3.api.pb@{SPOT_TIMEZONE}"],
                }
                await ws.send(json.dumps(sub_msg))

                # отдельная задача для ping
                asyncio.create_task(spot_ping_loop(ws, conn_id))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    channel = msg.get("channel", "")
                    if not channel.startswith("spot@public.miniTickers.v3.api.pb@"):
                        continue

                    send_time = msg.get("sendTime")
                    items = msg.get("publicMiniTickers", {}).get("items", [])

                    for it in items:
                        symbol = it.get("symbol")
                        if symbol not in symbols:
                            continue

                        price_str = it.get("price")
                        if not price_str:
                            continue

                        try:
                            price = float(price_str)
                        except ValueError:
                            continue

                        # У miniTickers нет bid/ask → считаем bid=ask=last
                        handle_price(
                            exchange="MEXC",
                            market="SPOT",
                            symbol=symbol,
                            bid=price,
                            ask=price,
                            ts=int(send_time) if send_time else None,
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
    """
    Один WS-коннект на sub.tickers (все контракты каждые ~1 с).
    Два коннекта (1 и 2) – резерв/распараллеливание, но логика одинакова.
    """
    while True:
        try:
            async with websockets.connect(
                FUTURES_WS_URL,
                ping_interval=None,
            ) as ws:
                sub_msg = {
                    "method": "sub.tickers",
                    "param": {},   # все контракты
                    "gzip": False  # удобнее парсить
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
    spot_symbols = load_symbols(SPOT_SYMBOLS_FILE)          # 2059 пар
    futures_contracts = load_symbols(FUTURES_SYMBOLS_FILE)  # 826 контрактов

    tasks = [
        # 2 WS на SPOT (miniTickers)
        asyncio.create_task(run_spot_connection(1, spot_symbols)),
        asyncio.create_task(run_spot_connection(2, spot_symbols)),

        # 2 WS на FUTURES (sub.tickers)
        asyncio.create_task(run_futures_connection(1, futures_contracts)),
        asyncio.create_task(run_futures_connection(2, futures_contracts)),
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())