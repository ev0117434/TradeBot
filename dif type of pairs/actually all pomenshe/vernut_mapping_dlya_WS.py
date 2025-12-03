import os

# Правила для каждого файла
RULES = {
    "mexc_futures_all.txt":  {"sep": "_", "suffix": ""},
    "okx_futures_all.txt":   {"sep": "-", "suffix": "-SWAP"},
    "okx_spot_all.txt":      {"sep": "-", "suffix": ""},
    "bingx_futures_all.txt": {"sep": "-", "suffix": ""},
    "bingx_spot_all.txt":    {"sep": "-", "suffix": ""}
}

def format_pair(line, sep, suffix):
    line = line.strip().upper()
    if len(line) < 4:
        return line
    quote = line[-4:]
    if quote in ("USDT", "USDC"):
        base = line[:-4]
        return f"{base}{sep}{quote}{suffix}"
    return line  # если не заканчивается на USDT/USDC — оставляем как есть

# Папки
input_folder = "dif type of pairs/actually all pomenshe/nerpav mapping"
output_folder = "dif type of pairs/actually all pomenshe"
os.makedirs(output_folder, exist_ok=True)

# Обработка всех файлов
for filename in os.listdir(input_folder):
    if filename not in RULES:
        print(f"Пропущен (нет правил): {filename}")
        continue
    
    sep = RULES[filename]["sep"]
    suffix = RULES[filename]["suffix"]
    
    input_path = os.path.join(input_folder, filename)
    output_path = os.path.join(output_folder, filename.replace(".txt", "_fixed.txt"))
    
    with open(input_path, "r", encoding="utf-8") as f_in, \
         open(output_path, "w", encoding="utf-8") as f_out:
        
        for line in f_in:
            new_line = format_pair(line, sep, suffix)
            f_out.write(new_line + "\n")
    
    print(f"Готово: {filename} → {os.path.basename(output_path)}")

print("Все файлы обработаны.")