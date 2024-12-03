import pandas as pd
timestamp_list,symbol_list,open_list,close_list,high_list,low_list,symbol_list = [],[],[],[],[],[],[]

def parse_stock_json_to_df(symbol,raw_data):
    for key, value in raw_data[0].items():
        timestamp_list.append(key)
        symbol_list.append(symbol)
        open_list.append(value["1. open"])
        close_list.append(value["4. close"])
        high_list.append(value["2. high"])
        low_list.append(value["3. low"])
        
    new_data = {'Symbol':symbol_list,
                'Timestamp':timestamp_list,
                'Open':open_list,
                'Close':close_list,
                'High':high_list,
                'Low':low_list}
    
    data = pd.DataFrame(new_data)
    
    return data