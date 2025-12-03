import asyncio
import json
import ssl
import time
from pathlib import Path

import certifi
import websockets


# ================= НАСТРОЙКИ =================

# Binance Spot & Futures WS
SPOT_URL = "wss://stream.binance.com:9443/ws"
FUTURES_URL = "wss://fstream.binance.com/ws"

# Файлы со списками пар (по одной в строке: BTCUSDT, ETHUSDT, ...)
SPOT_SYMBOLS_FILE = "binance_spot_all.txt"
FUTURES_SYMBOLS_FILE = "binance_futures_all.txt"

# Максимум символов в одном SUBSCRIBE-сообщении
BATCH_SIZE = 300

# Задержка перед переподключением при ошибке (сек)
RECONNECT_DELAY = 5

# SSL-контекст
SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())


# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================

def load_symbols(path: str) -> list[str]:
    """
    Читает файл с символами, удаляет пустые строки и пробелы.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Файл с символами не найден: {path}")

    symbols: list[str] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            symbols.append(s.upper())
    return symbols


def chunk_list(items: list[str], size: int) -> list[list[str]]:
    """
    Делит список на чанки по size элементов.
    """
    return [items[i:i + size] for i in range(0, len(items), size)]


def split_spot_symbols(symbols: list[str]) -> tuple[list[str], list[str]]:
    """
    Делит спотовые пары на две примерно равные группы
    для двух WebSocket-соединений.
    """
    n = len(symbols)
    mid = n // 2
    part1 = symbols[:mid]
    part2 = symbols[mid:]
    return part1, part2


def build_subscribe_message(symbols: list[str], request_id: int) -> str:
    """
    Формирует JSON SUBSCRIBE на @bookTicker для списка символов.
    """
    params = [f"{s.lower()}@bookTicker" for s in symbols]
    payload = {
        "method": "SUBSCRIBE",
        "params": params,
        "id": request_id,
    }
    return json.dumps(payload)


def process_bookticker_message(
    raw_msg: str,
    market_type: str,  # "spot" или "futures"
) -> str | None:
    """
    Обрабатывает одно сообщение bookTicker.
    Возвращает строку формата:
    BINANCE,spot|futures,SYMBOL,BID,ASK,TS

    Если это служебный ответ (result, id и т.п.) — возвращает None.
    """
    try:
        msg = json.loads(raw_msg)
    except json.JSONDecodeError:
        return None

    # Пропускаем ответы на SUBSCRIBE:
    # {"result": null, "id": 1}
    if isinstance(msg, dict) and "result" in msg:
        return None

    # Некоторые сообщения могут приходить как объект, а не как список
    data = msg

    # Формат spot/futures bookTicker:
    # {
    #   "s": "BTCUSDT",
    #   "b": "123.45",
    #   "a": "123.46",
    #   ...
    # }
    symbol = data.get("s")
    bid = data.get("b")
    ask = data.get("a")

    if not symbol or bid is None or ask is None:
        # Не bookTicker или странный формат — пропускаем
        return None

    # Локный timestamp (мс)
    ts_ms = int(time.time() * 1000)

    line = f"BINANCE,{market_type},{symbol},{bid},{ask},{ts_ms}"
    return line


# ================= ОСНОВНАЯ ЛОГИКА WS-ПОДКЛЮЧЕНИЙ =================

async def run_ws_connection(
    name: str,
    url: str,
    symbols: list[str],
    market_type: str,  # "spot" или "futures"
):
    """
    Универсальная функция:
    - подключается к WS
    - подписывается на @bookTicker по символам
    - слушает сообщения и печатает их
    - при ошибке переподключается
    """
    while True:
        try:
            print(f"[{name}] Подключаемся к {url}, символов: {len(symbols)}")
            async with websockets.connect(
                url,
                ssl=SSL_CONTEXT,
                ping_interval=20,
                ping_timeout=20,
                max_queue=None,  # не ограничиваем внутреннюю очередь
            ) as ws:
                print(f"[{name}] Подключено, отправляем SUBSCRIBE...")

                # Отправляем SUBSCRIBE батчами по BATCH_SIZE символов
                request_id = 1
                for batch in chunk_list(symbols, BATCH_SIZE):
                    sub_msg = build_subscribe_message(batch, request_id)
                    await ws.send(sub_msg)
                    print(f"[{name}] SUBSCRIBE на {len(batch)} стримов (id={request_id})")
                    request_id += 1
                    # Небольшая пауза, чтобы не спамить лимиты Binance
                    await asyncio.sleep(0.2)

                print(f"[{name}] Ожидаем сообщения bookTicker...")

                async for raw_msg in ws:
                    line = process_bookticker_message(raw_msg, market_type)
                    if line is None:
                        continue
                    # Здесь минимальная работа: просто печатаем строку
                    # В реальном проекте можно отправлять в очередь/локальный TCP и т.д.
                    print(line)

        except asyncio.CancelledError:
            # Корректное завершение таска
            print(f"[{name}] Task cancelled, выходим.")
            return
        except Exception as e:
            print(f"[{name}] Ошибка: {e!r}, переподключение через {RECONNECT_DELAY} c")
            await asyncio.sleep(RECONNECT_DELAY)


async def main():
    # Загружаем списки символов
    futures_symbols = load_symbols(FUTURES_SYMBOLS_FILE)
    spot_symbols = load_symbols(SPOT_SYMBOLS_FILE)

    print(f"[INIT] Futures символов: {len(futures_symbols)}")
    print(f"[INIT] Spot символов: {len(spot_symbols)}")

    # Futures — один WS
    futures_task = asyncio.create_task(
        run_ws_connection(
            name="FUTURES",
            url=FUTURES_URL,
            symbols=futures_symbols,
            market_type="futures",
        )
    )

    # Spot — два WS, делим список пополам
    spot_part1, spot_part2 = split_spot_symbols(spot_symbols)

    spot_task_1 = asyncio.create_task(
        run_ws_connection(
            name="SPOT-1",
            url=SPOT_URL,
            symbols=spot_part1,
            market_type="spot",
        )
    )

    spot_task_2 = asyncio.create_task(
        run_ws_connection(
            name="SPOT-2",
            url=SPOT_URL,
            symbols=spot_part2,
            market_type="spot",
        )
    )

    # Ждем все три таска (они по факту вечные)
    await asyncio.gather(futures_task, spot_task_1, spot_task_2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Остановка по Ctrl+C")
