import requests, time, telebot
from threading import Thread
import concurrent.futures


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

print(time.time())


# Получение тикеров монет на Binance Futures
binance_spot_url = 'https://api.binance.com/api/v3/exchangeInfo'
binance_spot_tickers = get_exchange_tickers(binance_spot_url, "binance")

# Получение тикеров монет на Bitget Futures
bitget_futures_url = 'https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES'
bitget_futures_tickers = get_exchange_tickers(bitget_futures_url, "bitget")

# Получение тикеров монет на MXC Futures
mexc_futures_url = 'https://contract.mexc.com/api/v1/contract/ticker'
mexc_futures_tickers = get_exchange_tickers(mexc_futures_url, "mexc")

print(len(binance_spot_tickers))
tickers = list(set(binance_spot_tickers) & set(bitget_futures_tickers + mexc_futures_tickers))
print(len(tickers))


def get_order_book(symbol):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=500"
    try:
        response = requests.get(url, timeout=10)
    except:
        response = requests.get(url, timeout=10)
    data = response.json()
    return data


def calculate_average_volume(symbol):
    candles_url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=15m&limit=11"
    try:
        candles_response = requests.get(candles_url, timeout=10)
    except:
        candles_response = requests.get(candles_url, timeout=10)
    #print(candles_response)
    candles_data = candles_response.json()
    #print(candles_data)
    sum_volume = 0
    for i in range(len(candles_data) - 1):
        sum_volume += float(candles_data[i][5])
    volume = sum_volume / 150
    return volume


def find_large_orders(symbol):
    time.sleep(1)
    print("f: ", time.time(), symbol)
    order_book = get_order_book(symbol)
    time.sleep(1)
    average_volume = calculate_average_volume(symbol)

    large_orders = []
    #print(order_book)
    if len(order_book['bids']) == 0 and len(order_book['bids']) == 0:
        return large_orders

    curr_price = float(order_book['bids'][0][0])
    for bid in order_book['bids']:
        if float(bid[1]) > average_volume * 10 and float(bid[1]) * float(bid[0]) >= 100000:
            large_orders.append({
                'ticker': symbol,
                'side': 'BUY',
                'price': bid[0],
                'diff': -round((curr_price - float(bid[0])) / curr_price * 100, 2),
                'volume': round(float(bid[1]) * float(bid[0])),
                'time_corr': round(float(bid[1]) / average_volume, 2),
                'time_find': round(time.time())
            })

    curr_price = float(order_book['asks'][0][0])
    for ask in order_book['asks']:
        if float(ask[1]) > average_volume * 10 and float(ask[1]) * float(ask[0]) >= 100000:
            large_orders.append({
                'ticker': symbol,
                'side': 'SELL',
                'price': ask[0],
                'diff': round((float(ask[0]) - curr_price) / curr_price * 100, 2),
                'volume': round(float(ask[1]) * float(ask[0])),
                'time_corr': round(float(ask[1]) / average_volume, 2),
                'time_find': round(time.time())
            })

    return large_orders


def update_large_orders(temp_all_large_orders, all_large_orders, n):
    if n == 0:
        return temp_all_large_orders
    else:
        for i in range(len(temp_all_large_orders)):
            if len(temp_all_large_orders[i]) != 0:
                print("upd_s: ", temp_all_large_orders[i], i)
                ticker_price = []
                del_list = []
                for j in range(len(all_large_orders[i])):
                    #print(all_large_orders[i])
                    ticker_price.append(float(all_large_orders[i][j]['price']))
                if len(all_large_orders[i]) != 0:
                    for j in range(len(all_large_orders[i])):
                        is_exist = False
                        for k in range(len(temp_all_large_orders[i])):
                            if float(temp_all_large_orders[i][k]['price']) == float(all_large_orders[i][j]['price']) and temp_all_large_orders[i][k]['side'] == all_large_orders[i][j]['side']:
                                print(" in 3 for: ", all_large_orders[i][j], temp_all_large_orders[i][k])
                                #print("next: ", len(all_large_orders[i]), len(temp_all_large_orders[i]))
                                time_find = all_large_orders[i][j]['time_find']
                                all_large_orders[i][j] = temp_all_large_orders[i][k]
                                all_large_orders[i][j]['time_find'] = time_find
                                is_exist = True
                            elif float(temp_all_large_orders[i][k]['price']) not in ticker_price:
                                print(" in 4 for: ", all_large_orders[i][j], temp_all_large_orders[i][k])
                                #print("next: ", len(all_large_orders[i]), len(temp_all_large_orders[i]))
                                all_large_orders[i].append(temp_all_large_orders[i][k])
                                ticker_price.append(float(temp_all_large_orders[i][k]['price']))
                                is_exist = True
                                break
                            elif float(temp_all_large_orders[i][k]['price']) == float(all_large_orders[i][j]['price']) and temp_all_large_orders[i][k]['side'] != all_large_orders[i][j]['side']:
                                print(" in 5 for: ", all_large_orders[i][j], temp_all_large_orders[i][k])
                                #print("next: ", len(all_large_orders[i]), len(temp_all_large_orders[i]))
                                all_large_orders[i].append(temp_all_large_orders[i][k])
                                is_exist = False
                                break
                        if not is_exist:
                            del_list.append(j)
                    del_list.reverse()
                    for j in range(len(del_list)):
                        all_large_orders[i].pop(del_list[j])
                    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! all_large_orders: ", all_large_orders[i])
                else:
                    for k in range(len(temp_all_large_orders[i])):
                        all_large_orders[i].append(temp_all_large_orders[i][k])
            else:
                print("!!!!!!!!!!!!!!!!!!!!!! temp_all_large_orders: ", i, temp_all_large_orders[i])
                if len(all_large_orders) > 0:
                    for j in range(len(all_large_orders[i])):
                        all_large_orders[i].pop(0)
        return all_large_orders


def alert(all_large_orders):
    for i in range(len(all_large_orders)):
        for j in range(len(all_large_orders[i])):
            ticker = all_large_orders[i][j]['ticker']
            duration = (time.time() - all_large_orders[i][j]["time_find"]) / 60
            if abs(all_large_orders[i][j]['diff']) < 1.5 and duration > 1 and ticker != "USDCUSDT":
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
                        time.sleep(1)
                        print("ticker: ", time.time() - tickers[ticker])



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
        temp_all_large_orders = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            temp_all_large_orders = list(executor.map(find_large_orders, tickers))
        time.sleep(2)

        # for ticker in tickers:
        #     large_orders = find_large_orders(ticker)
        #     temp_all_large_orders.append(large_orders)
        #     #print("all: ", all_large_orders)
        #     print(time.time(), ticker)
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
