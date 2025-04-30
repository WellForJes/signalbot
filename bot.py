import pandas as pd
import numpy as np
import requests
import json
import ta
import asyncio
import os
import time
import datetime
import websockets
from binance.client import Client
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, ContextTypes

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

client = Client(API_KEY, API_SECRET)
exchange_info = client.futures_exchange_info()
precisions = {s['symbol']: s['quantityPrecision'] for s in exchange_info['symbols']}

active_positions = set()
sent_signals = set()

app = Application.builder().token(TELEGRAM_TOKEN).build()

async def send_signal(symbol, interval, side, entry_price, tp_price, sl_price):
    keyboard = [[
        InlineKeyboardButton("✅ Я вошёл", callback_data=f"enter_{symbol}"),
        InlineKeyboardButton("🚪 Я вышел", callback_data=f"exit_{symbol}")
    ]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message = (f"📊 Сигнал на вход\n"
               f"Монета: {symbol}\n"
               f"Таймфрейм: {interval}\n"
               f"Направление: {side}\n"
               f"Цена входа: {entry_price}\n"
               f"TP: {tp_price}\n"
               f"SL: {sl_price}")
    await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, reply_markup=reply_markup)

def calculate_tp_sl(entry_price, direction, take_percent=1.5, stop_percent=0.5):
    if direction == "LONG":
        tp = round(entry_price * (1 + take_percent / 100), 5)
        sl = round(entry_price * (1 - stop_percent / 100), 5)
    elif direction == "SHORT":
        tp = round(entry_price * (1 - take_percent / 100), 5)
        sl = round(entry_price * (1 + stop_percent / 100), 5)
    else:
        raise ValueError("Direction must be 'LONG' or 'SHORT'")
    return tp, sl

def get_binance_klines(symbol, interval, limit=500):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
    data = requests.get(url).json()
    df = pd.DataFrame(data, columns=[
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
    return df

def prepare_data(df):
    df['EMA50'] = ta.trend.ema_indicator(df['close'], window=50)
    df['EMA200'] = ta.trend.ema_indicator(df['close'], window=200)
    df['RSI'] = ta.momentum.rsi(df['close'], window=14)
    df['ADX'] = ta.trend.adx(df['high'], df['low'], df['close'], window=14)
    df['volatility'] = (df['high'] - df['low']) / df['close']
    df['volume_mean'] = df['volume'].rolling(window=50).mean()
    df['CCI'] = ta.trend.cci(df['high'], df['low'], df['close'], window=20)
    return df.dropna()

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    if data.startswith("enter_"):
        symbol = data.replace("enter_", "")
        active_positions.add(symbol)
        await query.edit_message_reply_markup(reply_markup=None)
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"✅ Отмечено: Вы вошли в позицию {symbol}")
    elif data.startswith("exit_"):
        symbol = data.replace("exit_", "")
        active_positions.discard(symbol)
        await query.edit_message_reply_markup(reply_markup=None)
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"🚪 Отмечено: Вы вышли из позиции {symbol}")

async def stream_price(symbol):
    uri = f"wss://fstream.binance.com/ws/{symbol.lower()}@kline_1m"
    async with websockets.connect(uri) as websocket:
        df_30m = get_binance_klines(symbol, '30m', limit=100)
        df_1h = get_binance_klines(symbol, '1h', limit=100)
        df_30m = prepare_data(df_30m)
        # df_1h['EMA200'] = ta.trend.ema_indicator(df_1h['close'], window=200)
        last_30m_time = df_30m.index[-1]

        while True:
            try:
                data = json.loads(await websocket.recv())
                kline = data['k']
                if kline['x']:  # свеча закрыта
                    # Проверяем: закрылась ли новая 30-минутка
                    new_candle_time = pd.to_datetime(int(kline['t']), unit='ms')
                    if new_candle_time > last_30m_time:
                        df_30m = get_binance_klines(symbol, '30m', limit=100)
                        df_1h = get_binance_klines(symbol, '1h', limit=100)
                        df_30m = prepare_data(df_30m)
                        df_1h['EMA200'] = ta.trend.ema_indicator(df_1h['close'], window=200)
                        last_row = df_30m.iloc[-1]
                        trend_row = df_1h.iloc[-1]
                        entry_price = last_row['close']
                        signal_id = f"{symbol}-30m-{round(entry_price, 4)}"

                        if signal_id in sent_signals or symbol in active_positions:
                            last_30m_time = new_candle_time
                            continue

                        if (
                            last_row['ADX'] > 20 and
                            last_row['volatility'] > 0.0015 and
                            last_row['volume'] > last_row['volume_mean'] and
                            abs(last_row['CCI']) > 100
                        ):
                            if (
                                last_row['EMA50'] > last_row['EMA200'] and
                                last_row['close'] > last_row['EMA200'] and
                                True  # убрано EMA200 с 1H
                            ):
                                tp, sl = calculate_tp_sl(entry_price, "LONG")
                                await send_signal(symbol, '30m', "LONG", entry_price, tp, sl)
                                sent_signals.add(signal_id)
                            elif (
                                last_row['EMA50'] < last_row['EMA200'] and
                                last_row['close'] < last_row['EMA200'] and
                                True  # убрано EMA200 с 1H
                            ):
                                tp, sl = calculate_tp_sl(entry_price, "SHORT")
                                await send_signal(symbol, '30m', "SHORT", entry_price, tp, sl)
                                sent_signals.add(signal_id)

                        last_30m_time = new_candle_time
            except Exception as e:
                print(f"Ошибка для {symbol}: {e}")
                await asyncio.sleep(1)

async def hourly_status():
    while True:
        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg = f"✅ Бот работает. Текущее время: {now}"
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)
        await asyncio.sleep(3600)

async def start_streaming():
    symbols = ['btcusdt', 'ethusdt', 'solusdt', 'xrpusdt', 'ltcusdt', 'adausdt']
    status_queue = asyncio.Queue()

    async def monitored_stream(symbol):
        await status_queue.put(f"🟢 {symbol.upper()} подключён через WebSocket")
        await stream_price(symbol)

    async def startup_log():
        await asyncio.sleep(2)
        connected = []
        while not status_queue.empty():
            connected.append(await status_queue.get())
       now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
message = f"🤖 Бот запущен: {now}\nУспешно подключены WebSocket потоки:\n" + "\n".join(connected)
        await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)

    async def hourly_check():
        while True:
            await asyncio.sleep(3600)
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            active = "
".join([f"✅ {s.upper()} WebSocket активен" for s in symbols])
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"🕒 Проверка состояния: {now}
{active}")

    tasks = [monitored_stream(symbol) for symbol in symbols]
    tasks.append(startup_log())
    tasks.append(hourly_check())
    await asyncio.gather(*tasks)

app.add_handler(CallbackQueryHandler(button_handler))

if __name__ == '__main__':
    async def run():
        try:
            await app.initialize()
            await app.start()
            await start_streaming()
        except Exception as e:
            msg = f"❌ Бот аварийно остановлен: {e}"
            print(msg)
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)

    asyncio.run(run())
