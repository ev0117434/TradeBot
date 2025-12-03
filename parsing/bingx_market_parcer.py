import requests

BASE_URL = "https://open-api.bingx.com"

def get_spot_symbols() -> list[str]:
    url = BASE_URL + "/openApi/spot/v1/common/symbols"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return [item["symbol"] for item in data["data"]["symbols"]]

def get_futures_symbols() -> list[str]:
    url = BASE_URL + "/openApi/swap/v2/quote/contracts"
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    data = r.json()
    return [item["symbol"] for item in data["data"]]

if __name__ == "__main__":
    spot_symbols = get_spot_symbols()
    futures_symbols = get_futures_symbols()

    print(f"SPOT: {len(spot_symbols)} пар")
    print(f"FUTURES: {len(futures_symbols)} пар")

    # сохраняем в txt по одной паре в строке
    with open("bingx_spot_all.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(spot_symbols))

    with open("bingx_futures_all.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(futures_symbols))
