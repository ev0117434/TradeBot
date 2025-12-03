import os
from pathlib import Path

# ----------------- НАСТРОЙКИ (меняешь только здесь) -----------------
RAW_FOLDER = Path("parsing")          # сюда кидаешь 10 исходных файлов
RESULT_FOLDER = Path("unique pairs")     # сюда упадут 20 готовых файлов
RESULT_FOLDER.mkdir(exist_ok=True)
# ------------------------------------------------------------------

# Функция нормализации (очень агрессивная — ловит все варианты)
def normalize(symbol: str) -> str:
    s = symbol.upper()
    s = s.replace("-", "").replace("_", "").replace(" ", "")
    # Убираем суффиксы типа SWAP, PERP, USD и т.д.
    for suffix in ["SWAP", "PERP", "USD", "USDC", "BUSD", "FDUSD"]:
        s = s.replace(suffix, "")
    # Всё что не USDT → делаем USDT
    if not s.endswith("USDT"):
        for stable in ["USDC", "BUSD", "FDUSD", "USD"]:
            if stable in s:
                s = s.replace(stable, "USDT")
    return s

# Загружаем и нормализуем все файлы
files = {
    "binance_spot":    "binance_spot_all.txt",
    "binance_futures": "binance_futures_all.txt",
    "bybit_spot":      "bybit_spot_all.txt",
    "bybit_futures":   "bybit_futures_all.txt",
    "mexc_spot":       "mexc_spot_all.txt",
    "mexc_futures":    "mexc_futures_all.txt",
    "okx_spot":        "okx_spot_all.txt",
    "okx_futures":     "okx_futures_all.txt",
    "bingx_spot":      "bingx_spot_all.txt",
    "bingx_futures":   "bingx_futures_all.txt",
}

sets = {}
for key, filename in files.items():
    path = RAW_FOLDER / filename
    if not path.exists():
        print(f"Не найден: {path}")
        continue
    normalized = {normalize(line.strip()) for line in path.open(encoding="utf-8") if line.strip()}
    sets[key] = normalized

# Ровно 20 нужных комбинаций (в твоём порядке)
tasks = [
    ("binance_spot", "bybit_futures", "binance_s_bybit_f.txt"),
    ("binance_spot", "mexc_futures",  "binance_s_mexc_f.txt"),
    ("binance_spot", "okx_futures",   "binance_s_okx_f.txt"),
    ("binance_spot", "bingx_futures", "binance_s_bingx_f.txt"),
    
    ("bybit_spot", "binance_futures", "bybit_s_binance_f.txt"),
    ("bybit_spot", "mexc_futures",    "bybit_s_mexc_f.txt"),
    ("bybit_spot", "okx_futures",     "bybit_s_okx_f.txt"),
    ("bybit_spot", "bingx_futures",   "bybit_s_bingx_f.txt"),
    
    ("mexc_spot", "binance_futures", "mexc_s_binance_f.txt"),
    ("mexc_spot", "bybit_futures",   "mexc_s_bybit_f.txt"),
    ("mexc_spot", "okx_futures",      "mexc_s_okx_f.txt"),
    ("mexc_spot", "bingx_futures",    "mexc_s_bingx_f.txt"),
    
    ("okx_spot", "binance_futures", "okx_s_binance_f.txt"),
    ("okx_spot", "bybit_futures",   "okx_s_bybit_f.txt"),
    ("okx_spot", "mexc_futures",    "okx_s_mexc_f.txt"),
    ("okx_spot", "bingx_futures",   "okx_s_bingx_f.txt"),
    
    ("bingx_spot", "binance_futures", "bingx_s_binance_f.txt"),
    ("bingx_spot", "bybit_futures",   "bingx_s_bybit_f.txt"),
    ("bingx_spot", "mexc_futures",    "bingx_s_mexc_f.txt"),
    ("bingx_spot", "okx_futures",     "bingx_s_okx_f.txt"),
]

# Генерируем файлы
for spot_key, fut_key, out_name in tasks:
    if spot_key not in sets or fut_key not in sets:
        print(f"Пропущено {out_name} — нет данных")
        continue
    intersection = sets[spot_key] & sets[fut_key]
    out_path = RESULT_FOLDER / out_name
    with out_path.open("w", encoding="utf-8") as f:
        for pair in intersection:
            f.write(pair + "\n")
    print(f"Готово → {out_name} ({len(intersection)} пар)")

print("\nВсё сделано! Файлы лежат в папке:", RESULT_FOLDER.resolve())