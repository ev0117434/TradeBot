# collector.py
#!/usr/bin/env python3
import socket
from time import time

# ================== НАСТРОЙКИ ==================
UDP_IP   = "0.0.0.0"      # слушать на всех интерфейсах
UDP_PORT = 5555           # порт, на который шлют все твои скрипты

# Хранилище: (exchange, market, symbol) → {bid, ask, ts}
prices = {}
stats_last_print = 0

# ================== UDP СЕРВЕР ==================
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind((UDP_IP, UDP_PORT))

print(f"UDP-коллектор запущен → {UDP_IP}:{UDP_PORT}")
print("Ожидаю данные от бирж...\n")

while True:
    data, _ = sock.recvfrom(4096)                    # буфер больше любой строки
    line = data.decode("utf-8", errors="ignore").strip()
    if not line:
        continue

    parts = line.split(",")
    if len(parts) != 6:
        continue

    exchange, market, symbol, bid_str, ask_str, ts_str = parts

    try:
        bid = float(bid_str)
        ask = float(ask_str)
        ts  = int(ts_str)
    except ValueError:
        continue

    key = (exchange.upper(), market.lower(), symbol.upper())

    # Обновляем только если пришедшие данные свежее или равны по времени
    old = prices.get(key, {})
    if ts >= old.get("ts", 0):
        prices[key] = {"bid": bid, "ask": ask, "ts": ts}

    # Статистика каждые 5 секунд
    now = time()
    if now - stats_last_print >= 5:
        total = len(prices)
        print(f"Активных инструментов: {total} | Последнее: {exchange} {market} {symbol} → {bid} / ask:.6f}")
        stats_last_print = now

        # Пример: как получить цену BTC на всех биржах
        # for (ex, mk, sym), val in prices.items():
        #     if sym == "BTCUSDT":
        #         print(f"  {ex:7} {mk:7} {val['bid']} / {val['ask']}")