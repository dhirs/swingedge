from alpha_vantage.timeseries import TimeSeries
from dotenv import load_dotenv
from FetchStockData import parse_stock_json_to_df
from DBConnect import DataStore_to_DB
import os

load_dotenv()

alpha_api_key = os.getenv("ALPHA_API_KEY")
ts = TimeSeries(key=alpha_api_key,output_format="json")



def GetRawStockData(*,symbol="IBM",interval="1min",size="full",month):
   Rawdata = ts.get_intraday(symbol=symbol,interval=interval,outputsize=size,month=month)
   data = parse_stock_json_to_df(symbol,Rawdata)
   
   DataStore_to_DB(data)
   
   print(data.head(5))
   
for i in range(1,13):
   if i<=9:
      month = f"2024-0{i}"
      print(month)
   else:
      month = f"2024-{i}"
      print(month)
    
   GetRawStockData(month=month)
