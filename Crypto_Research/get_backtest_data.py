import pandas as pd
from datetime import datetime, timedelta
from binance.spot import Spot as Client
from binance.spot import futures as cf
from binance.futures import Futures as f


def binance_get_kline(pair: str, start: int = None, end: int = None):
    spot_client = Client(base_url="https://api.binance.com")
    raw_data = spot_client.klines(symbol=pair,
                                  interval='8h',
                                  limit=10,
                                  startTime=(start * 1000),
                                  endTime=(end * 1000))

    column_names = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume',
                    'Close_time', 'Quote asset volumn', 'Number of trades',
                    'Taker buy base asset volume', 'Taker buy quote asset volume',
                    'Ignore']
    data = pd.DataFrame(raw_data, columns=column_names).drop(columns='Ignore')
    data['Open_time'] = [datetime.fromtimestamp(i / 1000) for i in data['Open_time']]

    return data


def binance_output_data(symbol: str, start: datetime, end: datetime):
    time_list = process_time(start, end)

    output = []
    for interval in time_list:
        curr_start = interval[0]
        curr_end = interval[1]
        print(curr_start)
        per_interval = binance_get_kline(pair=symbol,
                                         start=int(curr_start.timestamp()),
                                         end=int(curr_end.timestamp()))
        output.append(per_interval)
    data = pd.concat(output).drop_duplicates(subset=['Open_time'], keep='last')

    # TODO: this is NY time UTC-4, need to be changed
    data['Open_time'] = data['Open_time']
    # + timedelta(hours=4)
    return data


def process_time(start_time: datetime, end_time: datetime) -> list:
    pointer_date = start_time
    if end_time is None:
        current = datetime.utcnow()
    else:
        current = end_time

    output = []
    while pointer_date < current:
        new_pointer = pointer_date + timedelta(days=1)
        if new_pointer >= current:
            current_pair = [pointer_date, current]
        else:
            current_pair = [pointer_date, new_pointer]
        output.append(current_pair)
        pointer_date = new_pointer

    return output


symbols = ['ETHUSDT']

for symbol in symbols:
    out = binance_output_data(symbol=symbol, start=datetime(2021, 1, 1),
                              end=datetime(2022, 1, 22))
    out.to_csv(str(symbol.replace('-PERP', 'USDT')) + '_8h.csv')
