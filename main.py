import json
import requests, time, telebot
from threading import Thread
import speedtest
import ta
import pandas as pd
import websocket
import tracemalloc
import traceback


print(time.time())
tracemalloc.start()
time.sleep(5)
all_order_books = {}
counter = {}
all_large_orders = {}
temp_all_large_orders = {}
average_volume = {}
lastUpdateId = {}
access_confirmed = []
isDebut = {}
great_count = 0
volume_great_count = 0
super_ticker = ''
start_pizdec = False
missed_messages = {}
tickers = []
msg_id_alerted_large_orders = {}
counter_error = 0


def get_exchange_tickers(exchange_url, name):
    fail = True
    while fail:
        try:
            response = requests.get(exchange_url)
            fail = False
        except Exception as e:
            print("except: ", exchange_url, e)
    tickers = response.json()
    if name == 'binance':
        return [ticker['symbol'] for ticker in tickers['symbols'] if ticker["status"] == 'TRADING']
    if name == 'bitget':
        return [ticker['symbol'] for ticker in tickers['data']]
    if name == 'mexc':
        return [ticker['symbol'].replace('_', '') for ticker in tickers['data']]


# Получение тикеров монет на Binance Futures
def update_tickers():
    global tickers
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
    print(len(tickers), tickers)


def speedtester():
    print("speedtest")
    st = speedtest.Speedtest()
    print("speedtest1")
    st.get_best_server()
    print("speedtest2")
    download_speed = st.download()
    print("speedtest3")
    upload_speed = st.upload()
    print("speedtest4")
    ping = st.results.ping
    download_mbps = round(download_speed / 1_000_000, 2)
    upload_mbps = round(upload_speed / 1_000_000, 2)
    print(f"Скорость загрузки: {download_mbps} Мбит/с")
    print(f"Скорость отдачи: {upload_mbps} Мбит/с")
    print(f"Пинг: {ping} мс")


def apply_updates_to_order_book(side, updates, order_book):
    for price, qty in updates:
        price = float(price)
        qty = float(qty)
        if qty == 0:
            # Удаляем цену из стакана, если количество равно 0
            if price in order_book[side]:
                del order_book[side][price]
        else:
            # Обновляем или добавляем цену и количество в стакан
            order_book[side][price] = qty
    return order_book


def async_on_message(ws, message):
    message = json.loads(message)
    ticker = message['data']['s']
    global lastUpdateId, access_confirmed, super_ticker, missed_messages, counter_error, great_count, tickers
    if ticker == super_ticker:
        print("check time 1: ", ticker, time.time(), len(missed_messages[ticker]), great_count)
    is_not_missed_access = False
    if ticker not in access_confirmed:
        missed_messages[ticker].append(message)
        for i in range(len(missed_messages[ticker])):
            if ticker in lastUpdateId and missed_messages[ticker][i]['data']['U'] <= lastUpdateId[ticker] <= missed_messages[ticker][i]['data']['u']:
                is_not_missed_access = True
    else:
        missed_messages[ticker] = []
    if (ticker in lastUpdateId and (int(message['data']['U']) <= lastUpdateId[ticker] <= int(message['data']['u'])) or is_not_missed_access or ticker in access_confirmed):
        if ticker not in access_confirmed:
            access_confirmed.append(ticker)
            missed_messages[ticker] = []
        global temp_all_large_orders, all_large_orders, all_order_books, counter
        order_book = {'bids': {}, 'asks': {}}
        bids = message['data']['b']
        asks = message['data']['a']
        if len(missed_messages[ticker]) != 0:
            for i in range(len(missed_messages[ticker])):
                if int(message['data']['U']) <= lastUpdateId[ticker]:
                    bids = missed_messages[ticker][i]['data']['b']
                    asks = missed_messages[ticker][i]['data']['a']
                    order_book = apply_updates_to_order_book('bids', bids, all_order_books[ticker])
                    order_book = apply_updates_to_order_book('asks', asks, all_order_books[ticker])
        order_book = apply_updates_to_order_book('bids', bids, all_order_books[ticker])
        order_book = apply_updates_to_order_book('asks', asks, all_order_books[ticker])
        all_order_books[ticker] = order_book.copy()
        counter[ticker] += 1
        if counter[ticker] == 13:
            counter[ticker] = 0
            all_order_books[ticker]['bids'] = dict(sorted(all_order_books[ticker]['bids'].items(), reverse=True))
            all_order_books[ticker]['asks'] = dict(sorted(all_order_books[ticker]['asks'].items()))
            if ticker == super_ticker:
                print(time.time(), great_count, len(access_confirmed))
                print('asks', next(iter(all_order_books[ticker]['asks'])), all_order_books[ticker]['asks'][next(iter(all_order_books[ticker]['asks']))])
                print('bids', next(iter(all_order_books[ticker]['bids'])), all_order_books[ticker]['bids'][next(iter(all_order_books[ticker]['bids']))])
            ticker, large_orders = find_large_orders(ticker, all_order_books[ticker])
            temp_all_large_orders[ticker] = large_orders
            all_large_orders[ticker] = update_large_orders(temp_all_large_orders, all_large_orders, 1, ticker)
        if isDebut[ticker] and counter_error == 0:
            all_order_books[ticker]['bids'] = dict(sorted(all_order_books[ticker]['bids'].items(), reverse=True))
            all_order_books[ticker]['asks'] = dict(sorted(all_order_books[ticker]['asks'].items()))
            ticker, large_orders = find_large_orders(ticker, all_order_books[ticker])
            temp_all_large_orders[ticker] = large_orders
            all_large_orders[ticker] = update_large_orders(temp_all_large_orders, all_large_orders, 0, ticker)
            isDebut[ticker] = False
        elif isDebut[ticker]:
            all_order_books[ticker]['bids'] = dict(sorted(all_order_books[ticker]['bids'].items(), reverse=True))
            all_order_books[ticker]['asks'] = dict(sorted(all_order_books[ticker]['asks'].items()))
            ticker, large_orders = find_large_orders(ticker, all_order_books[ticker])
            temp_all_large_orders[ticker] = large_orders
            all_large_orders[ticker] = update_large_orders(temp_all_large_orders, all_large_orders, 1, ticker)
            isDebut[ticker] = False
        if great_count % 5000 == 0 and len(all_large_orders) == len(tickers):
            great_count = 1
            print("42: ", time.time())
            alert(all_large_orders)
        great_count += 1
        if ticker == super_ticker:
            print("check time 12: ", ticker, time.time())


def on_message(ws, message):
    async_on_message(ws, message)


def on_error(ws, error):
    print("Ошибка:", error)
    traceback.print_exc()
    ws.close()


def on_close(ws, close_status_code, close_msg):
    global counter_error
    counter_error += 1
    print("### Соединение закрыто ###", close_status_code, close_msg)


def on_open(ws):
    print("Соединение установлено")
    global start_pizdec
    start_pizdec = True
    print(start_pizdec)


def gem():
    streams = "/".join([f"{tickers[i].lower()}@depth@1000ms" for i in range(len(tickers))])
    print(streams)
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    ws = websocket.WebSocketApp(socket_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
    print("fuck")
    process()


def get_order_book(symbol):
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit=500"
    fail = True
    while fail:
        try:
            response = requests.get(url, timeout=10)
            data = response.json()
            fail = False
            if "lastUpdateId" not in data:
                fail = True
        except Exception as e:
            print("except: ", url, e)
    return data


def calculate_volatility(symbol, candles_data):
    df = pd.DataFrame([row[1:5] for row in candles_data],  columns=['open', 'high', 'low', 'close'])
    df['NATR'] = ta.volatility.average_true_range(pd.to_numeric(df['high'], errors='coerce'), pd.to_numeric(df['low'], errors='coerce'), pd.to_numeric(df['close'], errors='coerce'), window=14, fillna=False) / pd.to_numeric(df['close'], errors='coerce') * 100
    return df['NATR'].iloc[-1]


def calculate_average_volume():
    candles_url = f"https://api.binance.com/api/v3/ticker/24hr"
    fail = True
    while fail:
        try:
            candles_response = requests.get(candles_url, timeout=10)
            fail = False
            volume_data = candles_response.json()
            if "code" in volume_data:
                fail = True
        except Exception as e:
            print("except: ", candles_url, e)
    try:
        for i in range(len(volume_data)):
            if volume_data[i]['symbol'] in tickers:
                average_volume[volume_data[i]['symbol']] = float(volume_data[i]['quoteVolume']) / 720
    except Exception as e:
        print("candle error: ", e, volume_data)
    #natr = calculate_volatility(symbol, candles_data)
    return average_volume


def detect_mm_large_orders(large_orders):
    list_index = []
    i = 0
    for bid in large_orders:
        j = 0
        for ask in large_orders:
            if bid['side'] == 'BUY' and ask['side'] == 'SELL':
                if abs(ask['diff'] + bid['diff']) < 0.7 and abs(ask['qty'] - bid['qty']) < 0.5 * max(ask['qty'], bid['qty']):
                    list_index.append(i)
                    list_index.append(j)
            j += 1
        i += 1

    list_index = (set(list_index))
    list_index = sorted(list_index, reverse=True)
    while len(list_index) != 0:
        large_orders.pop(list_index[0])
        list_index.pop(0)
    return large_orders


def find_large_orders(symbol, order_book = None):
    global counter, all_order_books, average_volume, volume_great_count, super_ticker
    if order_book is None:
        counter[symbol] = 0
        order_book = get_order_book(symbol)
        lastUpdateId[symbol] = int(order_book['lastUpdateId'])
        all_order_books[symbol] = {'bids': {}, 'asks': {}}
        all_order_books[symbol]['bids'] = {float(value[0]): float(value[1]) for value in order_book['bids']}
        all_order_books[symbol]['asks'] = {float(value[0]): float(value[1]) for value in order_book['asks']}
        print('bch', symbol)
    if volume_great_count % 100000 == 0:
        volume_great_count = 1
        average_volume = calculate_average_volume()
        print("YEEEEEEEEEEAAAAAAAAAAAAHHHHHHHHHHHH!!!!!!!!!")
    volume_great_count += 1
    large_orders = []
    if len(order_book['bids']) == 0 and len(order_book['asks']) == 0:
        print("shit!", symbol)
        return symbol, large_orders
    curr_price = float(next(iter(all_order_books[symbol]['bids'])))
    dict_bids = all_order_books[symbol]['bids']
    for bid_p, bid_q in dict_bids.items():
        amount_dollars = bid_p * bid_q
        if (amount_dollars > average_volume[symbol] * 10 and amount_dollars >= 100000):
            large_orders.append({
                'ticker': symbol,
                'side': 'BUY',
                'price': bid_p,
                'diff': -round((curr_price - bid_p) / curr_price * 100, 2),
                'volume': round(amount_dollars),
                'qty': round(bid_q, 2),
                'time_corr': round(amount_dollars / average_volume[symbol], 2),
                'time_find': round(time.time()),
                'volumeg': average_volume[symbol],
                'cur_price': curr_price,
                'id': symbol + 'BUY' + str(bid_p) + str(time.time())
            })
    curr_price = next(iter(all_order_books[symbol]['asks']))
    dict_asks = all_order_books[symbol]['asks']
    for ask_p, ask_q in dict_asks.items():
        amount_dollars = ask_p * ask_q
        if (amount_dollars > average_volume[symbol] * 10 and amount_dollars >= 100000):
            large_orders.append({
                'ticker': symbol,
                'side': 'SELL',
                'price': ask_p,
                'diff': round((ask_p - curr_price) / curr_price * 100, 2),
                'volume': round(amount_dollars),
                'qty': round(ask_q, 2),
                'time_corr': round(amount_dollars / average_volume[symbol], 2),
                'time_find': round(time.time()),
                'volumeg': average_volume[symbol],
                'cur_price': curr_price,
                'id': symbol + 'SELL' + str(ask_p) + str(time.time())
            })
    large_orders = detect_mm_large_orders(large_orders)
    return symbol, large_orders


def update_large_orders(temp_all_large_orders, all_large_orders, n, ticker = None):
    if n == 0:
        return temp_all_large_orders[ticker].copy()
    else:
        if len(temp_all_large_orders[ticker]) != 0:
            ticker_price = []
            del_list = []
            for j in range(len(all_large_orders[ticker])):
                ticker_price.append(float(all_large_orders[ticker][j]['price']))
            if len(all_large_orders[ticker]) != 0:
                for j in range(len(all_large_orders[ticker])):
                    is_exist = False
                    for k in range(len(temp_all_large_orders[ticker])):
                        if float(temp_all_large_orders[ticker][k]['price']) == float(all_large_orders[ticker][j]['price']) and temp_all_large_orders[ticker][k]['side'] == all_large_orders[ticker][j]['side']:
                            time_find = all_large_orders[ticker][j]['time_find']
                            id = all_large_orders[ticker][j]['id']
                            all_large_orders[ticker][j] = temp_all_large_orders[ticker][k]
                            all_large_orders[ticker][j]['time_find'] = time_find
                            all_large_orders[ticker][j]['id'] = id
                            is_exist = True
                        elif float(temp_all_large_orders[ticker][k]['price']) not in ticker_price:
                            all_large_orders[ticker].append(temp_all_large_orders[ticker][k])
                            ticker_price.append(float(temp_all_large_orders[ticker][k]['price']))
                            is_exist = True
                            break
                        elif float(temp_all_large_orders[ticker][k]['price']) == float(all_large_orders[ticker][j]['price']) and temp_all_large_orders[ticker][k]['side'] != all_large_orders[ticker][j]['side']:
                            all_large_orders[ticker].append(temp_all_large_orders[ticker][k])
                            is_exist = False
                            break
                    if not is_exist:
                        del_list.append(j)
                del_list.reverse()
                for j in range(len(del_list)):
                    for client, ids in msg_id_alerted_large_orders.items():
                        if all_large_orders[ticker][del_list[j]]['id'] in ids.keys():
                            print("msggggid", msg_id_alerted_large_orders[client][all_large_orders[ticker][del_list[j]]['id']].message_id, all_large_orders[ticker][del_list[j]]['id'])
                            bot.send_message(client, """_ ^ not found_""", reply_to_message_id=msg_id_alerted_large_orders[client][all_large_orders[ticker][del_list[j]]['id']].message_id, parse_mode="Markdown")
                            del msg_id_alerted_large_orders[client][all_large_orders[ticker][del_list[j]]['id']]
                            del clients[client][all_large_orders[ticker][del_list[j]]['id']]
                    all_large_orders[ticker].pop(del_list[j])
            else:
                for k in range(len(temp_all_large_orders[ticker])):
                    all_large_orders[ticker].append(temp_all_large_orders[ticker][k])
        else:
            if len(all_large_orders[ticker]) > 0:
                print("center off ass ", all_large_orders[ticker])
                for j in range(len(all_large_orders[ticker])):
                    for client, ids in msg_id_alerted_large_orders.items():
                        if all_large_orders[ticker][0]['id'] in ids.keys():
                            print("msggggid2222", msg_id_alerted_large_orders[client][all_large_orders[ticker][0]['id']].message_id, all_large_orders[ticker][0]['id'])
                            bot.send_message(client, """_ ^ not found_""", reply_to_message_id=msg_id_alerted_large_orders[client][all_large_orders[ticker][0]['id']].message_id, parse_mode="Markdown")
                            del msg_id_alerted_large_orders[client][all_large_orders[ticker][0]['id']]
                            del clients[client][all_large_orders[ticker][0]['id']]
                    all_large_orders[ticker].pop(0)
        return all_large_orders[ticker]


def alert(all_large_orders):
    print("alert")
    for i in all_large_orders:
        for j in range(len(all_large_orders[i])):
            ticker = all_large_orders[i][j]['ticker']
            id = all_large_orders[i][j]['id']
            duration = (time.time() - all_large_orders[i][j]["time_find"]) / 60
            if (abs(all_large_orders[i][j]['diff']) < 0.33) and duration > 11 and ticker != "USDCUSDT":
                print("durdom2", duration, ticker)
                for client, ids in clients.items():
                    if id not in ids.keys() or time.time() - ids[id] > 2400:
                        str_msg = f"""
                        _limit order on_ {"* #БЦХ - лайфчендж" if ticker == 'BCHUSDT' else f"_#{ticker}_"}{"*" if ticker == 'BCHUSDT' else ""}:
_side: {all_large_orders[i][j]["side"]}_  
_price: {all_large_orders[i][j]["price"]}_ 
{"*" if all_large_orders[i][j]["volume"] > 1000000 else "_"}amount: {all_large_orders[i][j]["volume"]}${"*" if all_large_orders[i][j]["volume"] > 1000000 else "_"}
_difference: {all_large_orders[i][j]["diff"]}%_ 
{"*" if all_large_orders[i][j]["time_corr"] > 30 else "_"}eating time: {all_large_orders[i][j]["time_corr"]}m{"*" if all_large_orders[i][j]["time_corr"] > 30 else "_"}
{"*" if duration > 600 else "_"}duration: {round(duration, 2)}m{"*" if duration > 600 else "_"}
                            """
                            #natr: {round(all_large_orders[i][j]["natr"], 2)} %
                        msg_id_alerted_large_orders[client][id] = bot.send_message(client, str_msg, parse_mode="Markdown")
                        print("msg id ", msg_id_alerted_large_orders[client][id].message_id)
                        clients[client][id] = time.time()
                        print("client: ", clients)


secret_key = b"2b51734631bb4c0fa540a20022637fb8"
token = "6823403772:AAEPz_wb28e5ruIAPilQSyrHYNsH_416kWU"
bot = telebot.TeleBot(token)
clients = dict()


@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, "_Let's do real shit bro!_", parse_mode="Markdown")
    clients[message.chat.id] = dict()
    msg_id_alerted_large_orders[message.chat.id] = dict()


def process():
    global lastUpdateId, access_confirmed, isDebut, great_count, volume_great_count, all_large_orders, temp_all_large_orders, missed_messages, super_ticker, start_pizdec, average_volume, tickers
    lastUpdateId = {}
    access_confirmed = []
    isDebut = {}
    great_count = 0
    volume_great_count = 1
    try:
        Thread(target=speedtester).start()
    except:
        print("speedtest error")
    n = 0
    print("1: ", time.time())
    print(len(tickers), tickers)
    update_tickers()
    print(all_large_orders)
    for ticker in tickers:
        #print(all_large_orders)
        if ticker not in all_large_orders.keys():
            all_large_orders[ticker] = []
            print(ticker)
    for i in range(len(tickers)):
        missed_messages[tickers[i]] = []
    average_volume = calculate_average_volume()
    print(average_volume)
    start_pizdec = False
    super_ticker = tickers[0]
    print(super_ticker)
    Thread(target=gem).start()
    while not start_pizdec:
        pass
    print("start_pizdec")
    time.sleep(5)
    for i in range(len(tickers)):
        isDebut[tickers[i]] = True
        temp_all_large_orders[tickers[i]] = list(find_large_orders(tickers[i]))[1]


Thread(target=process).start()


#telebot.apihelper.RETRY_ON_ERROR = True
while True:
    try:
        print('start')
        bot.infinity_polling(timeout=10, long_polling_timeout=5)
    except Exception as Argument:
        print(str(Argument))
        continue
