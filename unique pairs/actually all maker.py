from pathlib import Path

# ================= НАСТРОЙКИ =================

# Папка, где лежат исходные 20 файлов
INPUT_DIR = Path("unique pairs")     # поменяй на свою

# Папка, куда будут сохраняться новые файлы
OUTPUT_DIR = Path("dif type of pairs/actually all")   # поменяй на свою

# Логика: подстрока в имени файла -> имя итогового файла
PATTERNS_TO_OUTPUT = {
    "binance_f": "binance_futures_all.txt",
    "binance_s": "binance_spot_all.txt",
    "bybit_f":   "bybit_futures_all.txt",
    "bybit_s":   "bybit_spot_all.txt",
    "mexc_f":    "mexc_futures_all.txt",
    "mexc_s":    "mexc_spot_all.txt",
    "okx_f":     "okx_futures_all.txt",
    "okx_s":     "okx_spot_all.txt",
    "bingx_f":   "bingx_futures_all.txt",
    "bingx_s":   "bingx_spot_all.txt",
}


def collect_symbols_for_pattern(pattern: str) -> set[str]:
    """Собирает уникальные символы из всех файлов, где имя содержит pattern."""
    symbols: set[str] = set()

    for path in INPUT_DIR.glob("*.txt"):
        if pattern in path.name:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    s = line.strip()
                    if s:
                        symbols.add(s)

    return symbols


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for pattern, out_filename in PATTERNS_TO_OUTPUT.items():
        symbols = collect_symbols_for_pattern(pattern)
        out_path = OUTPUT_DIR / out_filename

        # Сортировка не обязательна, но удобно глазами
        sorted_symbols = sorted(symbols)

        with out_path.open("w", encoding="utf-8") as f:
            for s in sorted_symbols:
                f.write(s + "\n")

        print(f"{out_filename}: {len(sorted_symbols)} пар")


if __name__ == "__main__":
    main()
