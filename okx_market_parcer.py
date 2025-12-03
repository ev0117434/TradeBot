import requests

BASE_URL = "https://www.okx.com"


def fetch_instruments(inst_type: str):
    """
    Получить список инcтрументов нужного типа (SPOT, SWAP, FUTURES)
    Возвращает список instId, только живые пары (state == 'live').
    """
    url = f"{BASE_URL}/api/v5/public/instruments"
    params = {"instType": inst_type}

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "0":
        raise RuntimeError(f"OKX error: {data.get('code')} {data.get('msg')}")

    instruments = data.get("data", [])
    return [
        item["instId"]
        for item in instruments
        if item.get("state") == "live"
    ]


def main():
    # Все живые спотовые пары
    spot_pairs = fetch_instruments("SPOT")

    # Перпетуальные фьючерсы (на OKX это SWAP)
    swap_pairs = fetch_instruments("SWAP")

    # Фьючерсы с датой экспирации
    futures_pairs = fetch_instruments("FUTURES")

    print(f"OKX SPOT: {len(spot_pairs)} пар")
    print(f"OKX SWAP (perp): {len(swap_pairs)} пар")
    print(f"OKX FUTURES (dated): {len(futures_pairs)} пар")

    # Сохраняем по одной паре в строке
    with open("okx_spot_all.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(spot_pairs))

    # Все фьючерсные инструменты в один файл
    with open("okx_futures_all.txt", "w", encoding="utf-8") as f:
        for inst in swap_pairs + futures_pairs:
            f.write(inst + "\n")


if __name__ == "__main__":
    main()
