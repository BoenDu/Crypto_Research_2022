# from binance.spot.account import account
import pandas as pd
from FTXUtils import FtxClient
from datetime import datetime, timedelta
from acct_config import account_info


def get_funding_rate(start_time: str, symbol: str, api_key: str, secret_key: str, end_time: str=None):
    """ Get funding fees for give trading pair and time interval
    """
    client = FtxClient(api_key=api_key,
                       api_secret=secret_key)
    test = client.get_funding_rate(pair=symbol,
                                   start=start_time,
                                   end=end_time)
    data = pd.DataFrame(test)
    data = data.rename(columns={'rate': symbol})

    if data.empty:
        return pd.DataFrame([])
    return data[['time', str(symbol)]]


def process_time(start_time: datetime) -> list:
    """ Divide the whole time interval from start_time to current into small
        time intervals, such that each time interval has less than 500 lines
        (20 days * 24 lines per day = 480 lines in this case)
    """
    pointer_date = start_time
    current = datetime.utcnow()

    output = []
    while pointer_date < current:
        new_pointer = pointer_date + timedelta(days=20)
        if new_pointer >= current:
            current_pair = [str(pointer_date.timestamp()), str(current.timestamp())]
        else:
            current_pair = [str(pointer_date.timestamp()), str(new_pointer.timestamp())]
        output.append(current_pair)
        pointer_date = new_pointer
    
    return output


def main(start: datetime, api_key: str, secret_key: str, symbols: list):
    time_list = process_time(start_date)

    output = pd.DataFrame([])

    for each_pair in symbols:
        single_pair_output = []
        for interval in time_list:
            curr_start = interval[0]
            curr_end = interval[1]
            print(each_pair, curr_start)
            per_interval = get_funding_rate(start_time=curr_start,
                                            end_time=curr_end,
                                            symbol=each_pair,
                                            api_key=api_key,
                                            secret_key=secret_key)
            single_pair_output.append(per_interval.fillna(0))
        one_pair_data = pd.concat(single_pair_output).drop_duplicates(subset=['time'], keep='last')

        if symbols.index(each_pair) == 0:
            output = one_pair_data
        else:
            output = output.merge(one_pair_data, on='time', how='outer')

    return output.sort_values('time').reset_index(drop=True)


if __name__ == '__main__':
    # TODO: guhanji000@gmail.com FTX read-only apikeys
    api_key = account_info['ftx']['api_key']
    secret_key = account_info['ftx']['secret_key']

    symbols = ['LTC-PERP', 'BTC-PERP', 'DOGE-PERP',
               'ETH-PERP', 'ADA-PERP', 'DOT-PERP', 'XRP-PERP', 'LINK-PERP']

    start_date = datetime(2015, 3, 1)

    test = main(start=start_date,
                api_key=api_key,
                secret_key=secret_key,
                symbols=symbols)

    test.to_csv('2_years_funding.csv')
