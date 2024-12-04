import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
db_url = os.getenv("TIMESCALE_SERVICE_URL")

engine = create_engine(db_url)

def InsertData_to_DB(*,df,table_name,if_exists="replace"):
    try:
        with engine.connect() as conn:
            df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            print(f"Data successfully inserted into {table_name}.")
    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        
        
         
def ShowDatafromDB(table_name):
    with engine.connect() as conn:
        query = text(f'SELECT * FROM "{table_name}";') 
        result = conn.execute(query)
        rows = result.fetchall()
        column_names = result.keys()  
        fetched_df = pd.DataFrame(rows, columns=column_names)
        return fetched_df
    
    
        







