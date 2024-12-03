from alpha_vantage.timeseries import TimeSeries
from dotenv import load_dotenv
from FetchStockData import parse_stock_json_to_df
from DBConnect import DataStore_to_DB
import os

load_dotenv()

alpha_api_key = os.getenv("ALPHA_API_KEY")
ts = TimeSeries(key=alpha_api_key,output_format="json")



def GetRawStockData(symbol="IBM",interval="1min",size="compact"):
   Rawdata = ts.get_intraday(symbol=symbol,interval=interval,outputsize=size)
   data = parse_stock_json_to_df(symbol,Rawdata)
   
   DataStore_to_DB(data)
   
   print(data.head(5))
   

GetRawStockData("IBM","1min")
