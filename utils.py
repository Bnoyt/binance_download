import pandas as pd
import math
import os.path
import time
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
from tqdm import tqdm #(Optional, used for progress-bars)
import matplotlib.pyplot as plt
from binance.exceptions import BinanceAPIException


binance_api_key = 'API_KEY'    #Enter your own API-key here
binance_api_secret = 'API_SECRET' #Enter your own API-secret here

### CONSTANTS

binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)


def to_ms(t):
    t = pd.to_datetime(t).to_datetime64()

    return int(str(int(t))[:13])


def get_data_coin_future_mark(start,end,symbol,interval="1h"):
    """
    get future mark data
    """
    date_range = list(pd.date_range(start,end,freq="15D"))
    date_range.append(end)
    
    begins = date_range[:-1]
    ends = date_range[1:]
    
    dfs = []
    
    for s,e in tqdm(list(zip(begins,ends))):
        
        
        klines = binance_client.futures_coin_mark_price_klines(symbol=symbol,interval= interval, startTime=to_ms(s), endTime=to_ms(e),limit=500)
        dfs.append(pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ]))
    
    df = pd.concat(dfs)
    
    df["symbol"] = symbol
    df['timestamp'] = pd.to_datetime(df.timestamp,unit='ms')
    df['close_time'] = pd.to_datetime(df.close_time,unit='ms')

    
    return df.drop_duplicates('timestamp')


def get_data_coin_future_index(start,end,symbol,interval="1h"):
    """
    get current future settlement index
    """
    date_range = list(pd.date_range(start,end,freq="15D"))
    date_range.append(end)
    
    begins = date_range[:-1]
    ends = date_range[1:]
    
    dfs = []
    
    for s,e in tqdm(list(zip(begins,ends))):
        
        
        klines = binance_client.futures_coin_index_price_klines(pair=symbol,interval= interval, startTime=to_ms(s), endTime=to_ms(e),limit=500)
        dfs.append(pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ]))
    
    df = pd.concat(dfs)
    
    df["symbol"] = symbol
    df['timestamp'] = pd.to_datetime(df.timestamp,unit='ms')
    df['close_time'] = pd.to_datetime(df.close_time,unit='ms')

    
    return df.drop_duplicates('timestamp')


def get_data_funding_rate(start,end,symbol):
    """
    Get perpetual future funding rate 
    """
    date_range = list(pd.date_range(start,end,freq="20D"))
    date_range.append(end)
    
    begins = date_range[:-1]
    ends = date_range[1:]
    
    dfs = []
    
    for s,e in tqdm(list(zip(begins,ends))):
        
        
        klines = binance_client.futures_coin_funding_rate(symbol=symbol, startTime=to_ms(s), endTime=to_ms(e),limit=500)
        
        
        
        
        dfs.append(pd.DataFrame(klines))
    
    df = pd.concat(dfs)
    
    df['fundingTime'] = pd.to_datetime(df.fundingTime,unit='ms')

    
    return df.drop_duplicates('fundingTime')



def get_data_coin(start,end,symbol,interval="1h"):
    """
    get coin data (spot market)
    """
    
    
    date_range = list(pd.date_range(start,end,freq="15D"))
    date_range.append(end)
    
    begins = date_range[:-1]
    ends = date_range[1:]
    
    dfs = []
    
    for s,e in tqdm(list(zip(begins,ends))):
        
        
        klines = binance_client.get_klines(symbol=symbol,interval= interval, startTime=to_ms(s), endTime=to_ms(e),limit=500)
        dfs.append(pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ]))
    
    df = pd.concat(dfs)
    
    df["symbol"] = symbol
    df['timestamp'] = pd.to_datetime(df.timestamp,unit='ms')
    df['close_time'] = pd.to_datetime(df.close_time,unit='ms')

    
    return df.drop_duplicates('timestamp')


def get_available_coin_symbols():
    info = binance_client.get_exchange_info()
    return info['symbols']

def get_available_future_usdt_symbols():
    info = binance_client.futures_exchange_info()
    return info['symbols']
    
def get_available_future_coin_symbols():
    info = binance_client.futures_coin_exchange_info()
    return info['symbols']


def get_rolling_future_coin_data(start,end,symbol,freq='1h'):
    """
    get rolling coin futures price data (historical + forward)
    """
    
    years = ['20','21','22']
    
    months = ['03','06','09','12']
    
    days = ['22','23','24','25','26','27','28']
    
    
    delivery_dates = [f"{y}{m}{d}" for y,m,d in product(years,months,days)]
    
    dfs = []
    
    
    for d in delivery_dates:
        tmp_symbol = f"{symbol}_{d}"
        try:
            tmp = get_data_coin_future_mark(start,end,tmp_symbol,freq)
            tmp['fullsym'] = tmp_symbol
            tmp['delivery_date'] = pd.Timestamp(f"20{d}")
            print(f"Getting data for symbol {tmp_symbol}")
            dfs.append(tmp.copy())
        except BinanceAPIException:
            print(f"Dropping data fetching for symbol {tmp_symbol}")
            
    return pd.concat(dfs)




    
    
### Examples

rates = get_data_funding_rate(pd.Timestamp('2020-01-01',tz='UTC'),pd.Timestamp.utcnow(),'BTCUSD_PERP')
index = get_data_coin_future_index(pd.Timestamp('2020-01-01',tz='UTC'),pd.Timestamp.utcnow(),'BTCUSD')
marks = get_data_coin_future_mark(pd.Timestamp('2020-01-01',tz='UTC'),pd.Timestamp.utcnow(),'BTCUSD_PERP')
spot = get_data_coin(pd.Timestamp('2020-01-01',tz='UTC'),pd.Timestamp.utcnow(),'BTCUSDT')
rolling_fut = get_rolling_future_coin_data(pd.Timestamp('2020-01-01',tz='UTC'),pd.Timestamp.utcnow(),'BTCUSD')


