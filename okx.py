import asyncio
import json
import ssl
from pathlib import Path

import certifi
import websockets

# ================= НАСТРОЙКИ =================

OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"

# файлы со списками инструментов (по одному в строке, без пробелов):
#   BTC-USDT
#   ETH-USDT
SPOT_SYMBOLS_FILE = "okx_spot_all.txt"

#   BTC-USDT-SWAP
#   ETH-USDT-SWAP
FUTURES_SYMBOLS_FILE = "okx_futures_all.txt"

# размер батча для одного subscribe (примерно 150–200 символов)
BATCH_SIZE = 200

# пауза между двумя subscribe-запросами (чтобы не упираться в лимит 3 req/sec)
SUBSCRIBE_INTERVAL = 0.5

# интервалы ping/ping_timeout на уровне библиотеки websockets
PING_INTERVAL = 20
PING_TIMEOUT = 10


# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def read_symbols(path: str) -> list:
    """
    Читает файл с инструментами, по одному в строке.
    Игнорирует пустые строки и строки, начинающиеся с #.
    """
    symbols = []
    p = Path(path)

    if not p.exists():
        print(f"Файл {path} не найден, список будет пустым")
        return symbols

    with p.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            symbols.append(s)

    return symbols


def chunked(seq, size: int):
    """
    Разбивает список на куски по size элементов.
    """
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def build_subscribe_message(symbols_batch: list) -> dict:
    """
    Формирует одно сообщение subscribe для канала tickers с батчем инструментов.
    """
    return {
        "op": "subscribe",
        "args": [
            {"channel": "tickers", "instId": inst_id}
            for inst_id in symbols_batch
        ],
    }


async def subscribe_in_batches(ws, symbols: list):
    """
    Отправляет несколько subscribe-запросов батчами и делает паузу между ними.
    """
    if not symbols:
        return

    total = len(symbols)
    print(f"Подписка на {total} инструментов (батч {BATCH_SIZE})")

    for batch in chunked(symbols, BATCH_SIZE):
        msg = build_subscribe_message(batch)
        await ws.send(json.dumps(msg))
        await asyncio.sleep(SUBSCRIBE_INTERVAL)


# ================= ГЛАВНЫЙ ЦИКЛ ДЛЯ ОДНОГО РЫНКА =================

async def handle_okx_stream(url: str, symbols: list, market_type: str):
    """
    Одна WS-сессия для одного рынка (spot или futures).
    Минимальная логика внутри цикла: только парсинг и print.
    """
    ssl_context = ssl.create_default_context(cafile=certifi.where())

    if not symbols:
        print(f"{market_type.upper()}: список символов пуст, поток не будет запущен")
        return

    while True:
        try:
            print(f"{market_type.upper()}: подключение к {url} ...")
            async with websockets.connect(
                url,
                ssl=ssl_context,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
            ) as ws:
                print(f"{market_type.upper()}: подключено, подписываемся...")

                # подписка батчами
                await subscribe_in_batches(ws, symbols)

                # основной цикл чтения сообщений
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        # некорректный JSON – пропускаем
                        continue

                    # служебные события (подписка/ошибка и т.п.)
                    if "event" in msg:
                        event = msg.get("event")
                        if event != "subscribe":
                            # редкий лог, чтобы не спамить
                            print(f"{market_type.upper()} EVENT:", msg)
                        continue

                    # рабочие данные
                    arg = msg.get("arg") or {}
                    if arg.get("channel") != "tickers":
                        continue

                    data_list = msg.get("data") or []
                    for item in data_list:
                        inst_id = item.get("instId")
                        bid = item.get("bidPx")
                        ask = item.get("askPx")
                        ts = item.get("ts")

                        if not inst_id or bid is None or ask is None:
                            continue

                        # единый формат:
                        # [биржа, рынок, символ, bid, ask, ts]
                        out = ["OKX", market_type, inst_id, bid, ask, ts]

                        # здесь только print -> минимум задержек и блокировок
                        # дальше можно уже парсить этот вывод другим скриптом
                        print(json.dumps(out, ensure_ascii=False))

        except Exception as e:
            # при любой ошибке – короткий лог и реконнект
            print(f"{market_type.upper()}: ошибка: {e}. Переподключение через 5 секунд...")
            await asyncio.sleep(5)


# ================= ТОЧКА ВХОДА =================

async def main():
    # читаем списки инструментов
    spot_symbols = read_symbols(SPOT_SYMBOLS_FILE)
    futures_symbols = read_symbols(FUTURES_SYMBOLS_FILE)

    print(f"SPOT: {len(spot_symbols)} символов")
    print(f"FUTURES: {len(futures_symbols)} символов")

    tasks = []

    if spot_symbols:
        tasks.append(asyncio.create_task(
            handle_okx_stream(OKX_WS_URL, spot_symbols, "spot")
        ))

    if futures_symbols:
        tasks.append(asyncio.create_task(
            handle_okx_stream(OKX_WS_URL, futures_symbols, "futures")
        ))

    if not tasks:
        print("Нет символов для подписки. Проверь файлы okx_spot.txt и okx_futures.txt")
        return

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
