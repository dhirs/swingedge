from alpha_vantage.timeseries import TimeSeries
from dotenv import load_dotenv
from FetchStockData import parse_stock_json_to_df
import os

load_dotenv()

alpha_api_key = os.getenv("ALPHA_API_KEY")
ts = TimeSeries(key=alpha_api_key,output_format="json")
table_name = "stocks_data"

def GetRawStockData(*,symbol="IBM",interval="1min",size="full",month):
   Rawdata = ts.get_intraday(symbol=symbol,interval=interval,outputsize=size,month=month)
   data = parse_stock_json_to_df(symbol,Rawdata)
 
   
   
fetch = input("Do you want to fetch stock data (yes/no)? ").strip().lower()

if fetch == "yes":
    symbol = input("Which stock ticker would you like to fetch data for? (Leave blank for default: IBM) ").strip().upper()
    
    if not symbol:
        symbol = "IBM"
    
    for i in range(1, 13):
        if i <= 9:
            month = f"2024-0{i}"
        else:
            month = f"2024-{i}"
        
        print(f"Fetching data for {symbol} for month {month}...")
        GetRawStockData(symbol=symbol, month=month)
else:
    print("No data will be fetched.")



