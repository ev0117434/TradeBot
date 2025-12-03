import requests

# ===== URL MEXC =====
SPOT_URL = "https://api.mexc.com/api/v3/exchangeInfo"
FUTURES_URL = "https://contract.mexc.com/api/v1/contract/detail"

# ===== ИМЕНА ФАЙЛОВ =====
SPOT_OUTPUT_FILE = "mexc_spot_all.txt"
FUTURES_OUTPUT_FILE = "mexc_futures_all.txt"


def get_mexc_spot_symbols():
    """
    Все активные спотовые пары MEXC.
    """
    resp = requests.get(SPOT_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    symbols = []
    for s in data.get("symbols", []):
        # status: "1" = online
        if s.get("status") != "1":
            continue

        # спотовая торговля разрешена
        if not s.get("isSpotTradingAllowed", False):
            continue

        perms = s.get("permissions") or []
        if "SPOT" not in perms:
            continue

        symbols.append(s["symbol"])

    return symbols


def get_mexc_futures_symbols():
    """
    Все фьючерсные контракты MEXC (perpetual),
    кроме явно оффлайн (state == 3).

    ВАЖНО: без фильтра по apiAllowed, чтобы не отрезать большинство пар.
    """
    resp = requests.get(FUTURES_URL, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    data = payload.get("data")

    # Нормализация формата: на всякий случай обрабатываем и список, и dict.
    if isinstance(data, list):
        contracts = data
    elif isinstance(data, dict):
        # если это уже один контракт
        if "symbol" in data:
            contracts = [data]
        else:
            # если вдруг dict вида { "BTC_USDT": {...}, ... }
            contracts = list(data.values())
    else:
        contracts = []

    symbols = []
    for c in contracts:
        state = c.get("state")  # 0 enabled, 1 delivery, 2 delivered, 3 offline, 4 paused
        if state == 3:  # offline — пропускаем
            continue

        sym = c.get("symbol")
        if sym:
            symbols.append(sym)

    return symbols


def save_list_to_file(symbols, filename):
    """
    Сохраняет список строк в файл, по одной в строке.
    """
    with open(filename, "w", encoding="utf-8") as f:
        for s in symbols:
            f.write(s + "\n")


if __name__ == "__main__":
    # забираем все пары
    spot_symbols = get_mexc_spot_symbols()
    futures_symbols = get_mexc_futures_symbols()

    # сохраняем в отдельные файлы
    save_list_to_file(spot_symbols, SPOT_OUTPUT_FILE)
    save_list_to_file(futures_symbols, FUTURES_OUTPUT_FILE)

    print(f"SPOT: сохранено {len(spot_symbols)} пар в {SPOT_OUTPUT_FILE}")
    print(f"FUTURES: сохранено {len(futures_symbols)} пар в {FUTURES_OUTPUT_FILE}")
