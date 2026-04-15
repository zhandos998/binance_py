# Binance USDT-M Futures Momentum Bot

Бот работает с Binance USDT-M Futures через `python-binance`.

## Стратегия

Бот сканирует активные perpetual-пары с `USDT` каждые 1-5 минут и считает:

- EMA fast / EMA slow для направления тренда;
- RSI для подтверждения momentum;
- изменение цены за несколько закрытых свечей;
- текущий объем относительно среднего объема.

При `LOG_SCANNED_SYMBOLS=true` бот пишет в лог список монет, которые попали в скан, и объясняет фильтр отбора. Если `MAX_SYMBOLS > 0`, по умолчанию выбираются самые ликвидные контракты по `24h quoteVolume`, потому что `SYMBOL_SELECTION=volume`.

При `LOG_SCAN_SUMMARY=true` бот пишет короткую сводку: сколько сигналов найдено, главные причины отказа и таблицу ближайших кандидатов. `LOG_SYMBOL_DECISIONS=true` включает подробную строку по каждой монете, но обычно это слишком шумно.

Сигналы:

- `BUY` открывает `LONG`, если есть сильное движение вверх, объем выше среднего, цена выше EMA и RSI сильный, но не перегретый.
- `SELL` открывает `SHORT`, если есть сильное движение вниз, объем выше среднего, цена ниже EMA и RSI слабый.
- Если уже открыта позиция, противоположный сигнал закрывает позицию reduce-only ордером.
- Stop-loss и take-profit закрывают позицию отдельно для LONG и SHORT.

Бот рассчитан на One-way position mode. При `ENSURE_ONE_WAY_MODE=true` он пытается переключить аккаунт в One-way перед запуском.

## Риск

`TRADE_RISK_PCT` трактуется как максимальный риск от депозита при достижении `STOP_LOSS_PCT`. Размер позиции дополнительно ограничивается:

- `LEVERAGE`;
- `MIN_MARGIN_USDT`;
- `MAX_MARGIN_USDT`;
- минимальным notional биржи;
- `MAX_OPEN_POSITIONS`;
- `MAX_TRADES_PER_CYCLE`.

По умолчанию реальные ордера выключены.

## Установка

```powershell
cd d:\zhandos998\Desktop\binance_py
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

В `.env` вставь API ключи demo futures.

## Первый безопасный запуск

Сначала можно оставить:

```text
LIVE_TRADING=false
```

Запуск:

```powershell
py bot.py
```

В этом режиме бот только симулирует сделки и пишет:

- `bot.log`;
- `futures_trades.csv`;
- `futures_positions.json`.

## Demo Futures исполнение

Для отправки ордеров в Binance Demo Trading USDT-M Futures:

```text
LIVE_TRADING=true
FUTURES_DEMO=true
FUTURES_BASE_URL=https://demo-fapi.binance.com/fapi
USE_TEST_ORDER=false
```

Для первого demo-запуска лучше ограничить рынок:

```text
MAX_SYMBOLS=20
SYMBOL_SELECTION=volume
LOG_SCANNED_SYMBOLS=true
LOG_SCAN_SUMMARY=true
SCAN_SUMMARY_TOP_N=8
LOG_SYMBOL_DECISIONS=false
MAX_TRADES_PER_CYCLE=1
MAX_OPEN_POSITIONS=2
LEVERAGE=2
MAX_MARGIN_USDT=5
```

После этого:

```powershell
py bot.py
```
