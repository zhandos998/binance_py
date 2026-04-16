# Binance USDT-M Futures Momentum Bot

Бот для Binance USDT-M Futures на `python-binance`.

Что умеет:

- сканировать `USDT-M perpetual` контракты;
- считать `EMA`, `RSI`, `volume ratio`, `movement`;
- открывать `LONG` и `SHORT`;
- ставить защитные `SL/TP` через algo orders;
- вести позиции, журнал сделок и risk-state;
- считать статистику по сделкам;
- запускать backtest и walk-forward.

## Архитектура

- [bot.py](d:/zhandos998/Desktop/binance_py/bot.py) — запуск и основной цикл.
- [bot_base.py](d:/zhandos998/Desktop/binance_py/bot_base.py) — конфиг, dataclass-структуры, базовые утилиты.
- [bot_exchange.py](d:/zhandos998/Desktop/binance_py/bot_exchange.py) — universe и metadata по символам.
- [bot_market.py](d:/zhandos998/Desktop/binance_py/bot_market.py) — свечи, funding и `MarketSnapshot`.
- [bot_strategy.py](d:/zhandos998/Desktop/binance_py/bot_strategy.py) — сигналы и блокеры входа.
- [bot_risk.py](d:/zhandos998/Desktop/binance_py/bot_risk.py) — risk-state, cooldown, sizing.
- [bot_state.py](d:/zhandos998/Desktop/binance_py/bot_state.py) — позиции и журнал сделок.
- [bot_storage.py](d:/zhandos998/Desktop/binance_py/bot_storage.py) — SQLite-хранилище и lazy-миграция legacy `CSV/JSON`.
- [bot_execution.py](d:/zhandos998/Desktop/binance_py/bot_execution.py) — ордера, protection, sync live positions.
- [bot_scan.py](d:/zhandos998/Desktop/binance_py/bot_scan.py) — scan рынка и close-on-opposite signal.
- [bot_reporting.py](d:/zhandos998/Desktop/binance_py/bot_reporting.py) — summary логов.
- [trade_stats.py](d:/zhandos998/Desktop/binance_py/trade_stats.py) — статистика по журналу сделок.
- [backtest.py](d:/zhandos998/Desktop/binance_py/backtest.py) — backtest и walk-forward.
- [tests](d:/zhandos998/Desktop/binance_py/tests) — unit-тесты.

## Стратегия

Бот строит входы на базе:

- momentum по последним закрытым свечам;
- базового тренда `EMA fast / EMA slow`;
- `RSI`;
- всплеска объема;
- фильтра старшего таймфрейма;
- фильтра funding-rate.

Логика:

- `BUY` открывает `LONG`, если есть восходящий импульс, объем и подтверждение по тренду;
- `SELL` открывает `SHORT`, если есть нисходящий импульс, объем и подтверждение по тренду;
- противоположный сигнал может закрыть текущую позицию;
- отдельно работают закрытия по `stop-loss` и `take-profit`.

После открытия бот пытается поставить:

- `STOP_MARKET`;
- `TAKE_PROFIT_MARKET`;
- с `closePosition=true`.

## Фильтры качества

### Higher timeframe

Если `HIGHER_TIMEFRAME_ENABLED=true`, вход разрешается только когда старший ТФ подтверждает направление:

- для `LONG`: `HTF close > HTF EMA fast > HTF EMA slow`
- для `SHORT`: `HTF close < HTF EMA fast < HTF EMA slow`

Основные настройки:

```env
HIGHER_TIMEFRAME_ENABLED=true
HIGHER_TIMEFRAME_INTERVAL=1h
HIGHER_TIMEFRAME_EMA_FAST=9
HIGHER_TIMEFRAME_EMA_SLOW=21
```

### Funding filter

Если `FUNDING_FILTER_ENABLED=true`, бот режет входы с дорогим funding:

- `LONG` блокируется, если `funding > MAX_LONG_FUNDING_RATE_PCT`
- `SHORT` блокируется, если `funding < MIN_SHORT_FUNDING_RATE_PCT`

Пример:

```env
FUNDING_FILTER_ENABLED=true
MAX_LONG_FUNDING_RATE_PCT=0.03
MIN_SHORT_FUNDING_RATE_PCT=-0.03
```

Значения здесь в процентах за funding-period.  
Например `0.03` означает `0.03%`.

### Spread filter

Если `MAX_ENTRY_SPREAD_PCT > 0`, бот проверяет текущий spread по `bid/ask` перед market-входом.

- если spread шире порога, вход пропускается;
- это live-only фильтр, в backtest он не моделируется.

Пример:

```env
MAX_ENTRY_SPREAD_PCT=0.10
```

### Universe whitelist

Если задан `SYMBOL_WHITELIST`, бот сканирует только эти символы.

Пример:

```env
SYMBOL_WHITELIST=BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,BNBUSDT,DOGEUSDT
```

## Риск

Размер позиции ограничивается одновременно:

- `TRADE_RISK_PCT`;
- `STOP_LOSS_PCT`;
- `LEVERAGE`;
- `MIN_MARGIN_USDT`;
- `MAX_MARGIN_USDT`;
- минимальным `notional` биржи;
- `MAX_OPEN_POSITIONS`;
- `MAX_TRADES_PER_CYCLE`.

Дополнительно есть risk-контур:

- `MAX_DAILY_LOSS_USDT`;
- `MAX_CONSECUTIVE_LOSSES`;
- `SYMBOL_COOLDOWN_MINUTES_AFTER_STOP`;
- `SYMBOL_COOLDOWN_MINUTES_AFTER_CLOSE`.

Если лимит достигнут, бот перестает открывать новые позиции, но все еще может закрывать старые.

## Хранилище

Основное хранилище — `SQLite`.

Примеры:

- `bot.sqlite3`
- `bot_test.sqlite3`
- `bot_work.sqlite3`

Legacy-файлы:

- `TRADE_LOG_FILE`
- `POSITIONS_FILE`
- `RISK_STATE_FILE`

нужны только для:

- lazy-миграции старых данных в SQLite;
- fallback-режима `trade_stats.py`, если базы еще нет.

## Установка

```powershell
cd d:\zhandos998\Desktop\binance_py
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

В `.env` нужно вставить ключи Binance Demo Futures.

## Профили

Ключи лежат в `.env`, а торговые профили вынесены отдельно:

- [.env.work](d:/zhandos998/Desktop/binance_py/.env.work) — более спокойный demo-профиль;
- [.env.test](d:/zhandos998/Desktop/binance_py/.env.test) — агрессивный профиль для проверки механики входа/выхода.

Пример storage-настроек:

```env
DATABASE_FILE=bot_work.sqlite3
TRADE_LOG_FILE=futures_trades_work.csv
POSITIONS_FILE=futures_positions_work.json
RISK_STATE_FILE=risk_state_work.json
```

## Запуск

Рабочий профиль:

```powershell
$env:BOT_PROFILE_FILE=".env.work"
py bot.py
```

Тестовый профиль:

```powershell
$env:BOT_PROFILE_FILE=".env.test"
py bot.py
```

Сброс профиля:

```powershell
Remove-Item Env:BOT_PROFILE_FILE
```

Для demo-исполнения ордеров:

```env
LIVE_TRADING=true
FUTURES_DEMO=true
FUTURES_BASE_URL=https://demo-fapi.binance.com/fapi
USE_TEST_ORDER=false
```

Для безопасного dry-run:

```env
LIVE_TRADING=false
```

## Логи и состояния

Бот пишет:

- `bot.log` / `bot_*.log`;
- `bot.sqlite3` / `bot_*.sqlite3`;
- при наличии старых файлов может один раз импортировать `futures_trades*.csv`, `futures_positions*.json`, `risk_state*.json`.

Если включены:

- `LOG_SCANNED_SYMBOLS=true` — логируется universe;
- `LOG_SCAN_SUMMARY=true` — логируется summary по скану;
- `LOG_SYMBOL_DECISIONS=true` — логируется подробное решение по каждой монете.

## Статистика по сделкам

По умолчанию `trade_stats.py` читает `DATABASE_FILE` активного профиля.

```powershell
py trade_stats.py
```

Для рабочего профиля:

```powershell
$env:BOT_PROFILE_FILE=".env.work"
py trade_stats.py
```

Явно по SQLite:

```powershell
py trade_stats.py --db bot_work.sqlite3 --top 5 --days 10
```

Legacy CSV по-прежнему можно открыть напрямую:

```powershell
py trade_stats.py --file futures_trades_work.csv --top 5 --days 10
```

Скрипт считает:

- `winrate`;
- `avg win / avg loss`;
- `profit factor`;
- `expectancy`;
- `max drawdown`.

## Backtest / Walk-forward

```powershell
$env:BOT_PROFILE_FILE=".env.work"
py backtest.py --symbols BTCUSDT,ETHUSDT --days 30 --walk-forward --train-bars 300 --test-bars 100
```

С сохранением simulated trades:

```powershell
py backtest.py --symbols BTCUSDT,ETHUSDT,SOLUSDT --days 60 --save-trades backtest_trades.csv
```

Backtest использует ту же логику:

- momentum/EMA/RSI/volume;
- higher timeframe filter;
- funding-rate filter;
- стоп/тейк внутри свечи;
- комиссии и slippage.

## Тесты

Быстрый compile-check:

```powershell
py -m py_compile bot.py bot_base.py bot_exchange.py bot_execution.py bot_market.py bot_math.py bot_reporting.py bot_risk.py bot_scan.py bot_state.py bot_storage.py bot_strategy.py backtest.py trade_stats.py
```

Unit-тесты:

```powershell
py -m unittest discover -s tests -v
```

Обычный dev-запуск через `pytest`:

```powershell
pip install -r requirements-dev.txt
pytest -q
```

## CI

Есть workflow GitHub Actions: [.github/workflows/tests.yml](d:/zhandos998/Desktop/binance_py/.github/workflows/tests.yml)

Он делает:

1. установку `requirements-dev.txt`;
2. `py_compile`;
3. `pytest -q`.

## Что дальше

Практичные следующие шаги:

1. Telegram-уведомления по входам, выходам и ошибкам;
2. richer reporting поверх SQLite;
3. ATR-based stop и фильтр режима рынка;
4. dashboard по PnL, risk и execution quality.
