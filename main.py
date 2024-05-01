import requests, time, telebot
from threading import Thread
import concurrent.futures
import speedtest
import ta
import pandas as pd


print(time.time())
time.sleep(7)


def get_exchange_tickers(exchange_url, name):
    response = requests.get(exchange_url)
    tickers = response.json()
    if name == 'binance':
        return [ticker['symbol'] for ticker in tickers['symbols']]
    if name == 'bitget':
        return [ticker['symbol'] for ticker in tickers['data']]
    if name == 'mexc':
        return [ticker['symbol'].replace('_', '') for ticker in tickers['data']]


def get_exchange_precision(exchange_url):
    response = requests.get(exchange_url)
    tickers = response.json()
    return {ticker['symbol']: ticker['quotePrecision'] for ticker in tickers['symbols']}


print(time.time())


# Получение тикеров монет на Binance Futures
binance_spot_url = 'https://api.binance.com/api/v3/exchangeInfo'
binance_spot_tickers = get_exchange_tickers(binance_spot_url, "binance")
binance_spot_precision = get_exchange_precision(binance_spot_url)

# Получение тикеров монет на Bitget Futures
bitget_futures_url = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
bitget_futures_tickers = get_exchange_tickers(bitget_futures_url, "bitget")

# Получение тикеров монет на MXC Futures
mexc_futures_url = 'https://contract.mexc.com/api/v1/contract/ticker'
mexc_futures_tickers = get_exchange_tickers(mexc_futures_url, "mexc")

print(len(binance_spot_tickers))
tickers = list(set(binance_spot_tickers) & set(bitget_futures_tickers + mexc_futures_tickers))
print(len(tickers))


def speedtester():
    print("speedtest")
    st = speedtest.Speedtest()
    st.get_best_server()
    download_speed = st.download()
    upload_speed = st.upload()
    ping = st.results.ping
    download_mbps = round(download_speed / 1_000_000, 2)
    upload_mbps = round(upload_speed / 1_000_000, 2)
    print(f"Скорость загрузки: {download_mbps} Мбит/с")
    print(f"Скорость отдачи: {upload_mbps} Мбит/с")
    print(f"Пинг: {ping} мс")


def get_order_book(symbol):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=500"
    fail = True
    while fail:
        try:
            response = requests.get(url, timeout=10)
            fail = False
        except:
            print("except: ", url)
    data = response.json()
    return data


def calculate_volatility(symbol, candles_data):
    df = pd.DataFrame([row[1:5] for row in candles_data],  columns=['open', 'high', 'low', 'close'])
    df['NATR'] = ta.volatility.average_true_range(df['high'], df['low'], df['close'], n=14, fillna=False) / df['close'] * 100
    return df['NATR'].tail(1)


def calculate_average_volume(symbol):
    candles_url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit=151"
    fail = True
    while fail:
        try:
            candles_response = requests.get(candles_url, timeout=10)
            fail = False
        except:
            print("except: ", candles_url)
    candles_data = candles_response.json()
    sum_volume = 0
    for i in range(len(candles_data) - 1):
        sum_volume += float(candles_data[i][5])
    volume = sum_volume / 150

    natr = calculate_volatility(symbol, candles_data)
    return volume * 2, natr


def detect_mm_large_orders(large_orders):
    list_index = []
    i = 0
    for bid in large_orders:
        j = 0
        for ask in large_orders:
            if bid['side'] == 'BUY' and ask['side'] == 'SELL':
                if abs(ask['diff'] + bid['diff']) < 0.2 and abs(ask['qty'] - bid['qty']) < 0.2 * max(ask['qty'], bid['qty']):
                    list_index.append(i)
                    list_index.append(j)
            j += 1
        i += 1

    list_index = (set(list_index))
    list_index = sorted(list_index, reverse=True)
    while len(list_index) != 0:
        print(large_orders[list_index[0]])
        large_orders.pop(list_index[0])
        list_index.pop(0)
    return large_orders


def find_large_orders(symbol, precision):
    print("f start: ", time.time(), symbol, precision)
    time.sleep(0.5)
    order_book = get_order_book(symbol)
    time.sleep(0.25)
    average_volume, natr = calculate_average_volume(symbol)
    print("f end: ", time.time(), symbol, "natr: ", natr)

    large_orders = []
    if len(order_book['bids']) == 0 and len(order_book['bids']) == 0:
        return large_orders

    curr_price = float(order_book['bids'][0][0])
    for bid in order_book['bids']:
        if (float(bid[1]) > average_volume * 15 and float(bid[1]) * float(bid[0]) >= 100000 and float(precision) > 3) or (float(bid[1]) > average_volume * 45 and float(bid[1]) * float(bid[0]) >= 250000 and float(precision) <= 3):
            large_orders.append({
                'ticker': symbol,
                'side': 'BUY',
                'price': bid[0],
                'diff': -round((curr_price - float(bid[0])) / curr_price * 100, 2),
                'volume': round(float(bid[1]) * float(bid[0])),
                'qty': round(float(bid[1]), 2),
                'time_corr': round(float(bid[1]) / average_volume, 2),
                'time_find': round(time.time()),
                'natr': round(natr, 2)
            })

    curr_price = float(order_book['asks'][0][0])
    for ask in order_book['asks']:
        if (float(ask[1]) > average_volume * 15 and float(ask[1]) * float(ask[0]) >= 100000 and float(precision) > 3) or (float(ask[1]) > average_volume * 45 and float(ask[1]) * float(ask[0]) >= 250000 and float(precision) <= 3):
            large_orders.append({
                'ticker': symbol,
                'side': 'SELL',
                'price': ask[0],
                'diff': round((float(ask[0]) - curr_price) / curr_price * 100, 2),
                'volume': round(float(ask[1]) * float(ask[0])),
                'qty': round(float(ask[1]), 2),
                'time_corr': round(float(ask[1]) / average_volume, 2),
                'time_find': round(time.time()),
                'natr': round(natr, 2)
            })

    large_orders = detect_mm_large_orders(large_orders)
    return large_orders


def update_large_orders(temp_all_large_orders, all_large_orders, n):
    if n == 0:
        return temp_all_large_orders
    else:
        for i in range(len(temp_all_large_orders)):
            if len(temp_all_large_orders[i]) != 0:
                ticker_price = []
                del_list = []
                for j in range(len(all_large_orders[i])):
                    ticker_price.append(float(all_large_orders[i][j]['price']))
                if len(all_large_orders[i]) != 0:
                    for j in range(len(all_large_orders[i])):
                        is_exist = False
                        for k in range(len(temp_all_large_orders[i])):
                            if float(temp_all_large_orders[i][k]['price']) == float(all_large_orders[i][j]['price']) and temp_all_large_orders[i][k]['side'] == all_large_orders[i][j]['side']:
                                time_find = all_large_orders[i][j]['time_find']
                                all_large_orders[i][j] = temp_all_large_orders[i][k]
                                all_large_orders[i][j]['time_find'] = time_find
                                is_exist = True
                            elif float(temp_all_large_orders[i][k]['price']) not in ticker_price:
                                all_large_orders[i].append(temp_all_large_orders[i][k])
                                ticker_price.append(float(temp_all_large_orders[i][k]['price']))
                                is_exist = True
                                break
                            elif float(temp_all_large_orders[i][k]['price']) == float(all_large_orders[i][j]['price']) and temp_all_large_orders[i][k]['side'] != all_large_orders[i][j]['side']:
                                all_large_orders[i].append(temp_all_large_orders[i][k])
                                is_exist = False
                                break
                        if not is_exist:
                            del_list.append(j)
                    del_list.reverse()
                    for j in range(len(del_list)):
                        all_large_orders[i].pop(del_list[j])
                else:
                    for k in range(len(temp_all_large_orders[i])):
                        all_large_orders[i].append(temp_all_large_orders[i][k])
            else:
                if len(all_large_orders) > 0:
                    for j in range(len(all_large_orders[i])):
                        all_large_orders[i].pop(0)
        return all_large_orders


def alert(all_large_orders):
    for i in range(len(all_large_orders)):
        for j in range(len(all_large_orders[i])):
            ticker = all_large_orders[i][j]['ticker']
            duration = (time.time() - all_large_orders[i][j]["time_find"]) / 60
            if (abs(all_large_orders[i][j]['diff']) < 0.8 or abs(all_large_orders[i][j]['diff']) < abs(all_large_orders[i][j]['natr']) * 2) and duration > 1 and ticker != "USDCUSDT":
                for client, tickers in clients.items():
                    if ticker not in tickers.keys() or time.time() - tickers[ticker] > 1800:
                        if ticker == 'BCHUSDT':
                            str_msg = f"""
                            limit order on *БЦХ - лайфчендж*:
side: {all_large_orders[i][j]["side"]}  
price: {all_large_orders[i][j]["price"]} 
amount: {all_large_orders[i][j]["volume"]}$
difference: {all_large_orders[i][j]["diff"]}% 
eating time: {all_large_orders[i][j]["time_corr"]}m
duration: {round(duration, 2)}m
                            """
                        elif all_large_orders[i][j]["volume"] > 1000000:
                            str_msg = f"""
                            limit order on {ticker}:
side: {all_large_orders[i][j]["side"]}  
price: {all_large_orders[i][j]["price"]} 
*amount: {all_large_orders[i][j]["volume"]}$*
difference: {all_large_orders[i][j]["diff"]}% 
eating time: {all_large_orders[i][j]["time_corr"]}m
duration: {round(duration, 2)}m
                            """
                        elif all_large_orders[i][j]["time_corr"] > 30:
                            str_msg = f"""
                            limit order on {ticker}:
side: {all_large_orders[i][j]["side"]}  
price: {all_large_orders[i][j]["price"]} 
amount: {all_large_orders[i][j]["volume"]}$
difference: {all_large_orders[i][j]["diff"]}% 
*eating time: {all_large_orders[i][j]["time_corr"]}m*
duration: {round(duration, 2)}m
                            """
                        elif duration > 600:
                            str_msg = f"""
                            limit order on {ticker}:
side: {all_large_orders[i][j]["side"]}  
price: {all_large_orders[i][j]["price"]} 
amount: {all_large_orders[i][j]["volume"]}$
difference: {all_large_orders[i][j]["diff"]}% 
eating time: {all_large_orders[i][j]["time_corr"]}m
*duration: {round(duration, 2)}m*
                            """
                        else:
                            str_msg = f"""
                            limit order on {ticker}:
side: {all_large_orders[i][j]["side"]}  
price: {all_large_orders[i][j]["price"]} 
amount: {all_large_orders[i][j]["volume"]}$
difference: {all_large_orders[i][j]["diff"]}% 
eating time: {all_large_orders[i][j]["time_corr"]}m
duration: {round(duration, 2)}m
                            """

                        bot.send_message(client, str_msg, parse_mode="Markdown")
                        clients[client][ticker] = time.time()
                        print("client: ", clients)
                        time.sleep(0.5)


secret_key = b"2b51734631bb4c0fa540a20022637fb8"
token = "6944484655:AAFe27etqbzunIJeL2rpvX9moVhugCvk6p0"
bot = telebot.TeleBot(token)
clients = dict()


@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, "Let's do real shit bro!")
    clients[message.chat.id] = dict()


def process():
    all_large_orders = []
    n = 0
    while True:
        try:
            speedtester()
        except Exception as e:
            print("xyi: ", e)
        temp_all_large_orders = []
        ticker_pairs = [(ticker, binance_spot_precision[ticker]) for ticker in tickers if ticker in binance_spot_precision]
        with concurrent.futures.ThreadPoolExecutor() as executor:
            temp_all_large_orders = list(executor.map(lambda pair: find_large_orders(*pair), ticker_pairs))
        time.sleep(2)
        all_large_orders = update_large_orders(temp_all_large_orders, all_large_orders, n)
        if n > 0:
            alert(all_large_orders)
        n += 1


Thread(target=process).start()


telebot.apihelper.RETRY_ON_ERROR = True
while True:
    try:
        print('start')
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except Exception as Argument:
        print(str(Argument))
        continue
