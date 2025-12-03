import asyncio
import json
import ssl
from typing import List

import websockets
import certifi

# ================== НАСТРОЙКИ ==================

# Основные публичные WS-эндпоинты V5
SPOT_WS_URL = "wss://stream.bybit.com/v5/public/spot"
FUTURES_WS_URL = "wss://stream.bybit.com/v5/public/linear"

# Если у тебя аккаунт TR/KZ/Georgia — здесь надо сменить домен на stream.bybit-tr.com / stream.bybit.kz / stream.bybitgeorgia.ge
# см. доку: Connect. :contentReference[oaicite:7]{index=7}

SPOT_SYMBOLS_FILE = "bybit_spot_all.txt"
FUTURES_SYMBOLS_FILE = "bybit_futures_all.txt"

# Лимиты из документации:
# - Spot: не более 10 args в одном запросе subscribe
# - Общая длина args по соединению <= 21000 символов
SPOT_SUB_BATCH_SIZE = 10
FUTURES_SUB_BATCH_SIZE = 100  # безопасное значение для linear

PING_INTERVAL = 20  # сек, рекомендовано Bybit
RECONNECT_DELAY = 5  # сек между попытками переподключения

# SSL-контекст с актуальными корневыми сертификатами
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


# ================== УТИЛИТЫ ==================

def load_symbols(path: str) -> List[str]:
    symbols: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            symbols.append(s)
    return symbols


def make_orderbook_batches(symbols: List[str], batch_size: int) -> List[List[str]]:
    topics = [f"orderbook.1.{s}" for s in symbols]
    batches: List[List[str]] = []
    for i in range(0, len(topics), batch_size):
        batches.append(topics[i:i + batch_size])
    return batches


async def send_periodic_ping(ws: websockets.WebSocketClientProtocol, name: str) -> None:
    while True:
        await asyncio.sleep(PING_INTERVAL)
        msg = {"op": "ping", "req_id": f"{name}_ping"}
        try:
            await ws.send(json.dumps(msg))
        except Exception:
            # Если отправка не удалась — вылетим, чтобы задача пинга завершилась
            break


def parse_best_bid_ask(msg: dict):
    # Ожидаем структуру orderbook.{depth}.{symbol}, type: snapshot (L1)
    data = msg.get("data")
    if not isinstance(data, dict):
        return None

    bids = data.get("b") or []
    asks = data.get("a") or []
    if not bids or not asks:
        return None

    symbol = data.get("s")
    if not symbol:
        return None

    bid_price = bids[0][0]
    ask_price = asks[0][0]

    ts = msg.get("cts") or msg.get("ts")

    return symbol, bid_price, ask_price, ts


async def subscribe_batches(ws: websockets.WebSocketClientProtocol, batches: List[List[str]]) -> None:
    for batch in batches:
        payload = {
            "op": "subscribe",
            "args": batch,
        }
        await ws.send(json.dumps(payload))


# ================== ОСНОВНЫЕ ЦИКЛИ ==================

async def run_orderbook_stream(
    name: str,
    url: str,
    symbols_file: str,
    sub_batch_size: int,
) -> None:
    """
    name: 'spot' или 'futures'
    url:  Bybit WS URL
    symbols_file: путь к txt с символами
    """
    symbols = load_symbols(symbols_file)
    batches = make_orderbook_batches(symbols, sub_batch_size)

    print(f"{name.upper()}: всего символов={len(symbols)}, батчей={len(batches)}")

    while True:
        try:
            print(f"{name.upper()}: подключаемся к {url}")
            async with websockets.connect(
                url,
                ssl=SSL_CONTEXT,
                ping_interval=None,   # выключаем встроенный ping websockets
                max_queue=None,       # не ограничивать очередь сообщений
                compression=None,     # без компрессии для минимальной задержки
            ) as ws:
                print(f"{name.upper()}: подключено, подписываемся на orderbook.1.*")

                await subscribe_batches(ws, batches)
                print(f"{name.upper()}: SUBSCRIBE отправлен")

                # Запускаем user-level ping по протоколу Bybit
                ping_task = asyncio.create_task(send_periodic_ping(ws, name))

                try:
                    async for raw in ws:
                        msg = json.loads(raw)

                        topic = msg.get("topic")
                        if not topic:
                            # служебные ответы subscribe/ping — пропускаем
                            continue

                        if not topic.startswith("orderbook.1."):
                            continue

                        best = parse_best_bid_ask(msg)
                        if not best:
                            continue

                        symbol, bid, ask, ts = best

                        # Здесь ЛЮБОЙ нужный формат: файл / TCP / очередь.
                        # Сейчас просто вывод в консоль:
                        print(f"bybit,{name},{symbol},{bid},{ask},{ts}")

                finally:
                    ping_task.cancel()
                    with contextlib.suppress(Exception):
                        await ping_task

        except Exception as e:
            print(f"{name.upper()}: ошибка: {e}. Переподключение через {RECONNECT_DELAY} c...")
            await asyncio.sleep(RECONNECT_DELAY)


# ================== ТОЧКА ВХОДА ==================

import contextlib


async def main():
    spot_task = asyncio.create_task(
        run_orderbook_stream(
            name="spot",
            url=SPOT_WS_URL,
            symbols_file=SPOT_SYMBOLS_FILE,
            sub_batch_size=SPOT_SUB_BATCH_SIZE,
        )
    )

    futures_task = asyncio.create_task(
        run_orderbook_stream(
            name="futures",
            url=FUTURES_WS_URL,
            symbols_file=FUTURES_SYMBOLS_FILE,
            sub_batch_size=FUTURES_SUB_BATCH_SIZE,
        )
    )

    await asyncio.gather(spot_task, futures_task)


if __name__ == "__main__":
    asyncio.run(main())
