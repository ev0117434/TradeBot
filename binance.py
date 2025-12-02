import asyncio
import ccxt.pro as ccxtpro


async def fetch_all_tickers(exchange, label: str):
    # Загружаем рынки (список всех символов для данного типа: spot/future)
    await exchange.load_markets()

    # Разовый снимок всех тикеров сразу
    tickers = await exchange.fetch_tickers()

    print(f"\n=== {label} — всего {len(tickers)} символов ===")
    for symbol, t in tickers.items():
        bid = t.get("bid")
        ask = t.get("ask")
        last = t.get("last")

        # Пропускаем «пустые» тикеры без цен
        if bid is None and ask is None and last is None:
            continue

        print(f"{symbol:15} bid={bid} ask={ask} last={last}")


async def main():
    # Отдельный клиент для spot
    spot = ccxtpro.binance({
        "options": {
            "defaultType": "spot",
        }
    })

    # Отдельный клиент для USDⓈ-M futures
    futures = ccxtpro.binance({
        "options": {
            "defaultType": "future",  # для Binance фьючерсов
        }
    })

    try:
        # Параллельно получаем все тикеры spot и futures
        await asyncio.gather(
            fetch_all_tickers(spot, "BINANCE SPOT"),
            fetch_all_tickers(futures, "BINANCE FUTURES"),
        )
    finally:
        # Корректно закрываем соединения
        await spot.close()
        await futures.close()


if __name__ == "__main__":
    asyncio.run(main())
