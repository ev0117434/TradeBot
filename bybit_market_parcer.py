import requests

BASE_URL = "https://api.bybit.com"
INSTRUMENTS_PATH = "/v5/market/instruments-info"

# файлы, куда запишем пары
SPOT_FILE = "bybit_spot_all.txt"
FUTURES_FILE = "bybit_futures_all.txt"


def get_instruments(category: str):
    """
    Возвращает список инструментов для заданной категории:
    category = "spot" | "linear" | "inverse"
    """
    url = BASE_URL + INSTRUMENTS_PATH
    params = {"category": category}
    result = []
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        info = data.get("result", {})
        items = info.get("list", []) or []
        result.extend(items)

        cursor = info.get("nextPageCursor")
        if not cursor:
            break

    return result


def main():
    # все спотовые пары
    spot_instruments = get_instruments("spot")
    spot_symbols = [item["symbol"] for item in spot_instruments]

    # все фьючерсные пары: linear (USDT/USDC) + inverse (coin-m)
    linear_instruments = get_instruments("linear")
    inverse_instruments = get_instruments("inverse")
    futures_instruments = linear_instruments + inverse_instruments
    futures_symbols = [item["symbol"] for item in futures_instruments]

    # пишем в файлы по одной паре в строке
    with open(SPOT_FILE, "w", encoding="utf-8") as f:
        for s in spot_symbols:
            f.write(s + "\n")

    with open(FUTURES_FILE, "w", encoding="utf-8") as f:
        for s in futures_symbols:
            f.write(s + "\n")

    print(f"Всего спот пар: {len(spot_symbols)} (файл: {SPOT_FILE})")
    print(f"Всего фьючерсных пар: {len(futures_symbols)} (файл: {FUTURES_FILE})")


if __name__ == "__main__":
    main()
