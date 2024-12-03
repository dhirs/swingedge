import sqlite3

def DataStore_to_DB(df):
  conn = sqlite3.connect('stock_data.db')
  df.to_sql('stock_data', conn, if_exists='append', index=False)
  conn.close()