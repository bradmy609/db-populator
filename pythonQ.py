import pandas as pd
import requests
from sqlalchemy import *
import json

# "c:/Users/Brad/OneDrive/Documents/Projects/databases/Python MySQL/checker/config.json"

with open('config.json', 'r') as file:
    config = json.load(file)

username = config['username']
password = config['password']
host = config['host']
port = config['port']
dbname = config['dbname']

print(username, password, host, port, dbname)

db_url = 'mysql://{}:{}@{}:{}/{}'.format(username, password, host, port, dbname)
# db_url = 'mysql://admin:vertical@database-2.cood7ompdfrc.us-east-2.rds.amazonaws.com:3306/kucoin'

engine = create_engine(db_url)

marginTickers = requests.get('https://api.kucoin.com/api/v1/margin/config').json()['data']['currencyList']

coinData = requests.get('https://api.kucoin.com/api/v1/market/allTickers').json()['data']

def toTicker(symbol):
    dex = symbol.index('-')
    ticker = symbol[:dex]
    return ticker

time = coinData['time']
coinData = coinData['ticker']

marginData = []
for d in coinData:
    ticker = toTicker(d['symbol'])
    d['ticker'] = ticker
    if ticker in marginTickers:
        keyData = {}
        keyData['ticker'] = d['ticker']
        keyData['symbol'] =d['symbol']
        keyData['last'] = d['last']
        keyData['volValue'] = d['volValue']
        marginData.append(keyData)

usdt_data = []
btc_data = []
eth_data = []
kcs_data = []

for item in marginData:
    if '-USDT' in item['symbol']:
        usdt_data.append(item)
    if '-BTC' in item['symbol']:
        btc_data.append(item)
    if '-ETH' in item['symbol']:
        eth_data.append(item)
    if '-KCS' in item['symbol']:
        kcs_data.append(item)

usdt_last = {}

for d in usdt_data:
    usdt_last[d['ticker']] = [d['last']]

current_ts = int(time/1000)

usdt_last_df = pd.DataFrame(usdt_last, index=[current_ts])

def add_column(engine, table_name, column):
    column_name = column.compile(dialect=engine.dialect)
    column_type = column.type.compile(engine.dialect)
    engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (table_name, column_name, column_type))
    print("New column: {} added successfully!".format(column_name))

updated = False
while updated == False:
    try:
        usdt_last_df.to_sql('usdt_last', con=engine, if_exists='append')
        print("Database has been updated!")
        updated = True
    except BaseException as err:
        str_err = str(err)
        print(str_err)
        # i = 0
        # dex_list = []
        # while i < len(str_err):
        #     try:
        err_lst = str_err.split(' ')
        updated = True
                # if str_err.index(' ', i) > str_err.index('\n', i):
                #     dex = str_err.index('\n', i)
                # else:
                #     dex = str_err.index(' ', i)
            # except:
            #     break

            # dex_list.append(dex)
            # i = dex + 1
        new_symbol = err_lst[4][1:-1]
        print('Adding new symbol: {} to usdt_last db'.format(err_lst[4]))
        column = Column(str(new_symbol), String(100), primary_key=True)
        add_column(engine, 'usdt_last', column)

# usdt_last_df.to_sql('usdt_last', con=engine, if_exists='append')
