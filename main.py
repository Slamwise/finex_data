import dns
import pymongo
import pandas as pd
import time
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import secrets
from requests import request

def ftxcall(method, endpoint):
    method = method.upper()
    url = "https://ftx.com/api"
    response = request(method, url+endpoint)
    return response.json()

assets = ['tBTCDOMF0:USTF0', 'tBTCF0:USTF0', 'tBTCUSD', 'tETHF0:USTF0', 'tETHUSD']
ftx_asset = 'BTC/USD'

client = pymongo.MongoClient(secrets.mongo)
db = client.finex_trades

collection = db[assets[1]]
cursor = collection.find({})

df = pd.DataFrame(columns=['time', 'amount', 'oi'])
df.set_index('time', inplace=True)
for doc in cursor:
    ddf = pd.DataFrame(doc['trades'], columns=['time', 'amount'])
    ddf.set_index('time', inplace=True)
    ddf = ddf.resample('5T').sum().bfill()
    idf = pd.DataFrame(doc['oi'], columns=['time', 'oi'])
    idf.set_index('time', inplace=True)
    idf = idf.resample('5T').mean().bfill()
    adf = ddf.join(idf)
    df = pd.concat([df, adf])

prices = ftxcall('get',
    f'/markets/{ftx_asset}/candles?resolution=300&start_time={df.first_valid_index().timestamp()}&end_time={df.last_valid_index().timestamp()}')['result']

fdf = pd.DataFrame(prices, columns=['startTime', 'time', 'open', 'high', 'low', 'close', 'volume'])
fdf.set_index('time', inplace=True)

while datetime.fromtimestamp(fdf.first_valid_index()/1000) > df.first_valid_index():
    prices = ftxcall('get',
        f'/markets/{ftx_asset}/candles?resolution=300&start_time={df.first_valid_index().timestamp()}&end_time={fdf.first_valid_index()/1000}')['result']

    ffdf = pd.DataFrame(prices, columns=['startTime', 'time', 'open', 'high', 'low', 'close', 'volume'])
    ffdf.set_index('time', inplace=True)
    fdf = pd.concat([ffdf, fdf])
    time.sleep(0.5)

fdf.set_index('startTime', inplace=True)
fdf.index = pd.to_datetime(fdf.index)
fdf.index.name = 'time'

df.index = pd.to_datetime(df.index).tz_localize('UTC')

df = df.join(fdf)

df['cvd'] = df.amount.cumsum()

df.loc[(df['cvd'] > df['cvd'].shift(1)) & (df['oi'] > df['oi'].shift(1)), 'long'] = 1
df.loc[(df['cvd'] < df['cvd'].shift(1)) & (df['oi'] > df['oi'].shift(1)), 'short'] = 1
df.loc[(df['cvd'] > df['cvd'].shift(1)) & (df['oi'] < df['oi'].shift(1)), 'short'] = -1
df.loc[(df['cvd'] < df['cvd'].shift(1)) & (df['oi'] < df['oi'].shift(1)), 'long'] = -1
df.long = df.long.fillna(0)
df.short = df.short.fillna(0)
df['longs'] = df.long.cumsum()
df['shorts'] = df.short.cumsum()
df['long_short_ratio'] = df.longs / df.shorts

print(df)

fig = make_subplots(specs=[[{"secondary_y": True}]])

# Add traces
fig.add_trace(
    go.Scatter(x=df.index, y=df.close, name="btc close"),
    secondary_y=False,
)

fig.add_trace(
    go.Scatter(x=df.index, y=df.long_short_ratio, name="long/short ratio"),
    secondary_y=True,
)

fig.show()