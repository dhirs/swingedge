import os
import json
from alpha_vantage.timeseries import TimeSeries
from dotenv import load_dotenv
from Makedataframe import parse_stock_json_to_df
from sqs import PublishToSqs

# load_dotenv()

# alpha_api_key = os.getenv("ALPHA_API_KEY")
alpha_api_key = "F0HVGA83MVMS5OR3"
ts = TimeSeries(key=alpha_api_key, output_format="json")

batch_size = 60
last_timestamp_file = "last_timestamp.json"

def load_last_timestamp():
    if os.path.exists(last_timestamp_file):
        with open(last_timestamp_file, "r") as file:
            return json.load(file).get("last_timestamp")
    return None

def save_last_timestamp(timestamp):
    with open(last_timestamp_file, "w") as file:
        json.dump({"last_timestamp": timestamp}, file)

def GetRawStockData(*, symbol="IBM", interval="1min", month="2024-12",size="full"):
    global last_timestamp_file
    print("Performing Base Upload...")
    raw_data = ts.get_intraday(symbol=symbol, interval=interval, month=month,outputsize=size)
    data = parse_stock_json_to_df(symbol, raw_data)
    data = data.sort_values(by="Timestamp")  # Sort by timestamp (ascending)

    # Save the last timestamp after base upload
    last_timestamp = data.iloc[-1]["Timestamp"]
    save_last_timestamp(last_timestamp)
    print("Base Upload Completed. Last Timestamp:", last_timestamp)

    # Upload data in batches
    no_of_batches = data.shape[0] // batch_size
    for i in range(no_of_batches):
        batch_df = data.iloc[i * batch_size:(i + 1) * batch_size, :]
        PublishToSqs(data=batch_df, BatchNo=i + 1)

    if data.shape[0] % batch_size != 0:
        leftover_df = data.iloc[no_of_batches * batch_size:, :]
        PublishToSqs(data=leftover_df, BatchNo=no_of_batches + 1)

def GetIncrementalData(*, symbol="IBM", interval="1min",month="2024-12", size="full"):
    print("Fetching Incremental Data...")
    last_timestamp = load_last_timestamp()
    if last_timestamp is None:
        print("Error: No last timestamp found. Perform base upload first.")
        return

    raw_data = ts.get_intraday(symbol=symbol, interval=interval,month=month, outputsize=size)
    data = parse_stock_json_to_df(symbol, raw_data)
    data = data.sort_values(by="Timestamp")  # Sort by timestamp (ascending)

    # Filter only new data
    incremental_data = data[data["Timestamp"] > last_timestamp]

    if incremental_data.empty:
        print("No new data available.")
        return

    # Save the new last timestamp
    last_timestamp = incremental_data.iloc[-1]["Timestamp"]
    save_last_timestamp(last_timestamp)
    print("Incremental Data Upload Completed. Last Timestamp:", last_timestamp)

    # Upload data in batches
    no_of_batches = incremental_data.shape[0] // batch_size
    for i in range(no_of_batches):
        batch_df = incremental_data.iloc[i * batch_size:(i + 1) * batch_size, :]
        PublishToSqs(data=batch_df, BatchNo=i + 1)

    if incremental_data.shape[0] % batch_size != 0:
        leftover_df = incremental_data.iloc[no_of_batches * batch_size:, :]
        PublishToSqs(data=leftover_df, BatchNo=no_of_batches + 1)

# Main logic
if os.path.exists(last_timestamp_file):
    print("Incremental Data Upload")
    GetIncrementalData()
else:
    print("Base Data Upload")
    GetRawStockData()
