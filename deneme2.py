import pandas as pd
import ccxt

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

jpn=pd.read_html('https://tradingeconomics.com/japan/stock-market')[0].iloc[:,0].to_list()

jpn1=[]
for i in jpn:
    i=str(i)+'/JPY'
    jpn1.append(i)
japanlist=dfc5[dfc5['index'].isin(jpn1)]
americalist=dfc5[dfc5['index'].isin(sp500mod)]
europelist=dfc5[dfc5['index'].isin(euro3)]

currency=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&(dfc4.assetType == 'CURRENCY')]
major_curr=['EUR/USD','USD/JPY','GBP/USD','USD/CHF','AUD/USD','USD/CAD','NZD/USD']

currencylist=currency[currency['index'].isin(major_curr)]

crypto=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&(dfc4.assetType == 'CRYPTOCURRENCY')]
cryptolist=crypto[crypto['index'].isin(['ETH/USD', 'BTC/USD'])]

indices=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&(dfc4.assetType == 'INDEX')]

commodities1=dfc4[(dfc4.marketType == 'SPOT')& (dfc4.assetType!='UTILITY_TOKENS')&(dfc4.quoteAssetId!='USDT')&(dfc4.assetType == 'COMMODITY')]
commodities=commodities1[commodities1['name'].str.contains('Futures')==False]

dfc6=pd.concat([americalist, europelist,currencylist,japanlist,cryptolist,indices,commodities], axis=0)
dfc6['fullname'] = dfc6.iloc[:,0]+'_'+dfc6.iloc[:,1]+'_'+dfc6.iloc[:,2]+'_'+dfc6.iloc[:,3]
dfc6.iloc[:, [0,6]].to_csv('csymbols.csv',header=False, index=False)
