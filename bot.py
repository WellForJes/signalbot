import pandas as pd
import numpy as np
import requests
import ta
import asyncio
import os
import time
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

def get_binance_klines(symbol, interval, limit=500):
    url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval={interval}&limit={limit}"
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

async def trading_bot(symbols, interval='30m'):
    await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🤖 Сигнальный бот с кнопками запущен!")
    hourly_data = {}
    for symbol in symbols:
        df_1h = get_binance_klines(symbol, interval='1h', limit=200)
        df_1h['EMA200_1h'] = ta.trend.ema_indicator(df_1h['close'], window=200)
        hourly_data[symbol] = df_1h
    last_hourly_update = time.time()

    while True:
        try:
            if time.time() - last_hourly_update > 3600:
                for symbol in symbols:
                    df_1h = get_binance_klines(symbol, interval='1h', limit=200)
                    df_1h['EMA200_1h'] = ta.trend.ema_indicator(df_1h['close'], window=200)
                    hourly_data[symbol] = df_1h
                last_hourly_update = time.time()

            session_log = "📈 Проверка рынка:\n"
            for symbol in symbols:
                try:
                    df = get_binance_klines(symbol, interval)
                    df = prepare_data(df)
                    if df.empty:
                        session_log += f"{symbol}: Ошибка данных\n"
                        continue
                    last_row = df.iloc[-1]
                    entry_price = last_row['close']
                    signal_id = f"{symbol}-{interval}-{round(entry_price, 4)}"
                    if signal_id in sent_signals or symbol in active_positions:
                        session_log += f"{symbol}: Уже в позиции или сигнал отправлен\n"
                        continue
                    if (last_row['ADX'] > 20 and last_row['volatility'] > 0.0015 and last_row['volume'] > last_row['volume_mean'] and abs(last_row['CCI']) > 100):
                        if (last_row['EMA50'] > last_row['EMA200'] and last_row['close'] > last_row['EMA200']):
                            tp, sl = calculate_tp_sl(entry_price, "LONG")
                            await send_signal(symbol, interval, "LONG", entry_price, tp, sl)
                            sent_signals.add(signal_id)
                            session_log += f"{symbol}: ✅ Сигнал LONG\n"
                        elif (last_row['EMA50'] < last_row['EMA200'] and last_row['close'] < last_row['EMA200']):
                            tp, sl = calculate_tp_sl(entry_price, "SHORT")
                            await send_signal(symbol, interval, "SHORT", entry_price, tp, sl)
                            sent_signals.add(signal_id)
                            session_log += f"{symbol}: ✅ Сигнал SHORT\n"
                        else:
                            session_log += f"{symbol}: EMA не подтверждает вход\n"
                    else:
                        session_log += f"{symbol}: Фильтры не проходят\n"
                except Exception as ex:
                    session_log += f"{symbol}: Ошибка {str(ex)}\n"
            session_log += "\nОбновление завершено."
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=session_log)
            await asyncio.sleep(300)
        except Exception as e:
            await app.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=f"🔥 Ошибка: {str(e)}")
            await asyncio.sleep(300)

app.add_handler(CallbackQueryHandler(button_handler))
symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'LTCUSDT', 'ADAUSDT']

if __name__ == '__main__':
    async def run():
        await app.initialize()
        await app.start()
        await trading_bot(symbols)
        await app.stop()
    asyncio.run(run())
