import streamlit as st
import pandas as pd
import ccxt
import sqlalchemy
import time


exchange=ccxt.currencycom()
markets= exchange.load_markets()

sp500=pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0].Symbol.to_list()

sp500mod=[]
for i in sp500:
    i=i+'/USD'
    sp500mod.append(i)
    
euro1=pd.read_html("https://en.wikipedia.org/wiki/EURO_STOXX_50")[2].Ticker.to_list()

euro2 = [ s[:s.find(".",s.find(". ")+1)] for s in euro1]

euro3=[]
for i in euro2:
    i=i+'/EUR'
    euro3.append(i)

dfc=pd.DataFrame(markets)
dfc2=dfc.T
dfc3=dfc2["info"].apply(pd.Series)

dfc4=dfc3[["name","status","assetType","country","marketType","quoteAssetId"]]
dfc4.reset_index(inplace=True)

dfc5=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&((dfc4.status=='TRADING')\
         |(dfc4.status=='BREAK'))]

jpn=pd.read_html('https://tradingeconomics.com/japan/stock-market')[0].iloc[:,0].to_list()

jpn1=[]
for i in jpn:
    i=str(i)+'/JPY'
    jpn1.append(i)
japanlist=dfc5[dfc5['index'].isin(jpn1)]
americalist=dfc5[dfc5['index'].isin(sp500mod)]
europelist=dfc5[dfc5['index'].isin(euro3)]
major_curr=['EUR/USD','USD/JPY','GBP/USD','USD/CHF','AUD/USD','USD/CAD','NZD/USD']
currencylist=dfc5[dfc5['index'].isin(major_curr)]
cryptolist=dfc5[dfc5['index'].isin(['ETH/USD', 'BTC/USD'])]

indices=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&(dfc4.assetType == 'INDEX')]

commodities1=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&(dfc4.assetType == 'COMMODITY')]
commodities=commodities1[commodities1['name'].str.contains('Futures')==False]

dfc6=pd.concat([americalist, europelist,currencylist,japanlist,cryptolist,indices,commodities], axis=0)
dfc6['fullname'] = dfc6.iloc[:,0]+'_'+dfc6.iloc[:,1]+'_'+dfc6.iloc[:,2]+'_'+dfc6.iloc[:,3]
dfc6.iloc[:, [0,6]].to_csv('csymbols.csv',header=False, index=False)
st.write(dfc6)
st.set_page_config(layout="wide")
st.title('Screener')
start = time.perf_counter()
@st.cache(suppress_st_warning=True)
def getdata():
    if os.path.exists("günlük.db"):
        os.remove("günlük.db")
    elif os.path.exists("haftalik.db"):
        os.remove("haftalik.db")
    exchange=ccxt.currencycom()
    markets= exchange.load_markets()    
    symbols1=pd.read_csv('csymbols.csv',header=None)
    symbols=symbols1.iloc[:,0].to_list()
    index = 0
    fullnames=symbols1.iloc[:,1].to_list()
    engine=sqlalchemy.create_engine('sqlite:///günlük.db')
    enginew=sqlalchemy.create_engine('sqlite:///haftalik.db')
    with st.empty():
        for ticker,fullname in zip(symbols,fullnames):
            index += 1
            try:
                data2 = exchange.fetch_ohlcv(ticker, timeframe='1d',limit=250) #since=exchange.parse8601('2022-02-13T00:00:00Z'))
                data3= exchange.fetch_ohlcv(ticker, timeframe='1w',limit=250)
                st.write(f"⏳ {index,ticker} downloaded")
            except Exception as e:
                print(e)
            else:
                header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
                dfc = pd.DataFrame(data2, columns=header)
                dfc['Date'] = pd.to_datetime(dfc['Date'],unit='ms')
                dfc['Date'] = dfc['Date'].dt.strftime('%d-%m-%Y')
                dfc.to_sql(fullname,engine, if_exists='replace')
                dfc2 = pd.DataFrame(data3, columns=header)
                dfc2['Date'] = pd.to_datetime(dfc2['Date'],unit='ms')
                dfc2['Date'] = dfc2['Date'].dt.strftime('%d-%m-%Y')
                dfc2.to_sql(fullname,enginew, if_exists='replace')

