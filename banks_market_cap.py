# Importing the required libraries
import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlite3


def log_progess(message):
    #Write Message into log file with timestamp
    ts = datetime.now()
    ts_formatted = ts.strftime('%Y-%h-%d-%H:%M:%S')
    with open(log_file, 'a') as f:
        f.write(ts_formatted + " : " +message + "\n")
     

def extract(url, table_attribs):
    #Extract table from web and save data to a dataframe
    df = pd.DataFrame(columns = table_attribs)

    html_page = requests.get(url).text
    page_data = BeautifulSoup(html_page, 'html.parser')

    tables = page_data.find_all('tbody')
    rows = tables[0].find_all('tr') #Required table is the first on the page
    
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            row_dict = {table_attribs[0]: col[1].get_text().rstrip(),
                        table_attribs[1]: col[2].get_text()}
            df_row = pd.DataFrame(row_dict, index=[0])
            df = pd.concat([df, df_row], ignore_index=True)
    df[table_attribs[1]] = df[table_attribs[1]].astype(float)
    return df

def transform(df, csv_path):
    #Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    return df

def load_to_csv(df, output_path):
    #Load the transformed dataframe to an output CSV file. 
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    #Load the transformed dataframe to an SQL database server as a table.
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)    
    
def run_query(query_statement, sql_connection):
    #Run queries on the database table
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


#Initializing variables
data_url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_path = '/home/project/exchange_rate.csv'
ex_table_attributes = ['Name', 'MC_USD_Billion']
final_table_attributes = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_path = './Largest_banks_data.csv'
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'code_log.txt'

#Run ETL Process
log_progess('Preliminaries complete. Initiating ETL process')
extracted_data = extract(data_url, ex_table_attributes)
log_progess('Data extraction complete. Initiating Transformation process')

transformed_data = transform(extracted_data, exchange_rate_path)
log_progess('Data transformation complete. Initiating Loading process')

load_to_csv(transformed_data, output_path)
log_progess('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progess('SQL Connection initiated')
load_to_db(transformed_data, sql_connection, table_name)
log_progess('Data loaded to Database as a table, Executing queries')

run_query(f"SELECT * FROM Largest_banks", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks", sql_connection)
run_query(f"SELECT Name from Largest_banks LIMIT 5", sql_connection)
log_progess('Process Complete')

sql_connection.close()
log_progess('Server Connection closed')
