import os
from pathlib import Path

# ====================== НАСТРОЙКИ ======================
input_dir = Path("dif type of pairs/really all")      # ← сюда кладёте 10 файлов *_all.txt
output_dir = Path("unique pairs")   # ← сюда будут сохранены все *_s_*.txt
# =======================================================

output_dir.mkdir(exist_ok=True)

# Правило нормализации символов для каждой биржи
def normalize_symbol(symbol: str, exchange: str) -> str:
    s = symbol.strip().upper()
    # Удаляем всё, что не буквы и не USDT в конце
    s = s.replace("-SWAP", "").replace("_SWAP", "")
    s = s.replace("-", "").replace("_", "")
    # Гарантируем, что заканчивается на USDT
    if s.endswith("USDT"):
        return s
    # Если USDT где-то посередине или вообще нет — просто возвращаем как есть (на всякий случай)
    return s

# Загружаем и нормализуем файл
def load_normalized_set(filepath: Path, exchange_key: str) -> set[str]:
    if not filepath.exists():
        print(f"Файл не найден: {filepath}")
        return set()
    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()
    return {normalize_symbol(line, exchange_key) for line in lines if line.strip()}

# Список всех базовых файлов
base_files = {
    "binance_spot":    input_dir / "binance_spot_all.txt",
    "binance_futures": input_dir / "binance_futures_all.txt",
    "bybit_spot":      input_dir / "bybit_spot_all.txt",
    "bybit_futures":   input_dir / "bybit_futures_all.txt",
    "mexc_spot":       input_dir / "mexc_spot_all.txt",
    "mexc_futures":    input_dir / "mexc_futures_all.txt",
    "okx_spot":        input_dir / "okx_spot_all.txt",
    "okx_futures":     input_dir / "okx_futures_all.txt",
    "bingx_spot":      input_dir / "bingx_spot_all.txt",
    "bingx_futures":   input_dir / "bingx_futures_all.txt",
}

# Загружаем все множества один раз
sets = {}
for key, path in base_files.items():
    sets[key] = load_normalized_set(path, key)

# Все нужные комбинации (spot одной биржи ∩ futures другой)
combinations = [
    ("binance_spot", "bybit_futures",   "binance_s_bybit_f.txt"),
    ("binance_spot", "mexc_futures",    "binance_s_mexc_f.txt"),
    ("binance_spot", "okx_futures",     "binance_s_okx_f.txt"),
    ("binance_spot", "bingx_futures",   "binance_s_bingx_f.txt"),

    ("bybit_spot",   "binance_futures", "bybit_s_binance_f.txt"),
    ("bybit_spot",   "mexc_futures",    "bybit_s_mexc_f.txt"),
    ("bybit_spot",   "okx_futures",     "bybit_s_okx_f.txt"),
    ("bybit_spot",   "bingx_futures",   "bybit_s_bingx_f.txt"),

    ("mexc_spot",    "binance_futures", "mexc_s_binance_f.txt"),
    ("mexc_spot",    "bybit_futures",   "mexc_s_bybit_f.txt"),
    ("mexc_spot",    "okx_futures",     "mexc_s_okx_f.txt"),
    ("mexc_spot",    "bingx_futures",   "mexc_s_bingx_f.txt"),

    ("okx_spot",     "binance_futures", "okx_s_binance_f.txt"),
    ("okx_spot",     "bybit_futures",   "okx_s_bybit_f.txt"),
    ("okx_spot",     "mexc_futures",    "okx_s_mexc_f.txt"),
    ("okx_spot",     "bingx_futures",   "okx_s_bingx_f.txt"),

    ("bingx_spot",   "binance_futures", "bingx_s_binance_f.txt"),
    ("bingx_spot",   "bybit_futures",   "bingx_s_bybit_f.txt"),
    ("bingx_spot",   "mexc_futures",    "bingx_s_mexc_f.txt"),
    ("bingx_spot",   "okx_futures",     "bingx_s_okx_f.txt"),
]

# Генерируем файлы пересечений
for spot_key, fut_key, filename in combinations:
    intersection = sets[spot_key] & sets[fut_key]
    if intersection:
        output_path = output_dir / filename
        with open(output_path, "w", encoding="utf-8") as f:
            for symbol in sorted(intersection):
                f.write(symbol + "\n")
        print(f"Создан {filename}: {len(intersection)} пар")
    else:
        print(f"Пересечение пустое: {filename}")

print("Готово. Все файлы лежат в папке:", output_dir.resolve())