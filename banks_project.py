import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime

url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
csv_path = "exchange_rate.csv"
output_path = "banks.csv"
table_attribs = ["Bank_name","Market_cap"]

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format) 
    with open("./console_log.txt","a") as f:
        f.write(f"{timestamp} : {message} \n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    soup = BeautifulSoup(page,"html.parser")
    df = pd.DataFrame(columns=table_attribs)

    # Targeting the rows 
    table = soup.find_all("tbody")
    rows = table[1].find_all("tr")

    col_bank = []
    col_market = []

    for i in range(len(rows)-1):
        # We extrat the bank name column and the market value for each bak
        col_bank = rows[i+1].find_all("td")[1].text
        col_market = rows[i+1].find_all("td")[2].text

        #We remove le \n
        col_bank = col_bank.split("\n")[0]
        col_market = col_market.split("\n")[0]

        # Stocking the values in a dict
        data_dict = {"Bank_name": col_bank,
                    "Market_cap": col_market}
        
        # Transforming our datas into a dataframe
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)

    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    # Reading the csv file and loading the exchange rate in a dictionnary
    exchange_rate = pd.read_csv(csv_path)
    dict_exchange = exchange_rate.set_index('Currency').to_dict()['Rate']

    # Transforming the market cap values in a list
    Market_cap_list = df["Market_cap"].tolist()

    # Converting the market_cap type to float
    df["Market_cap"] = [float("".join(item.split(","))) for item in Market_cap_list]
    # Converting the market cap values according the different rates
    df['MC_EUR_Billion'] = [np.round(item*dict_exchange["EUR"],2) for item in df["Market_cap"]]
    df['MC_GBP_Billion'] = [np.round(item*dict_exchange["GBP"],2) for item in df["Market_cap"]]
    df['MC_INR_Billion'] = [np.round(item*dict_exchange["INR"],2) for item in df["Market_cap"]]

    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv('banks.csv', sep =",")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name,sql_connection,if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    print(query_statement)
    print(pd.read_sql(query_statement,sql_connection))

log_progress("Preliminaries complete. Initiating ETL process")
df = extract(url,table_attribs)
log_progress("Data extraction complete. Initiating Transformation process")
df = transform(df,csv_path)
log_progress("Data transformation complete. Initiating Loading process")
load_to_csv(df,output_path)
log_progress("Data saved to CSV file")
sql_connection = sqlite3.connect("Largest_banks")
log_progress("SQL Connection initiated")
load_to_db(df,sql_connection,"Largest_banks")
log_progress("Data loaded to Database as a table, Executing queries")
sql = f'SELECT * FROM Largest_banks'
run_query(sql,sql_connection)
sql = f'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
run_query(sql,sql_connection)
sql = f'SELECT Bank_name from Largest_banks LIMIT 5'
run_query(sql,sql_connection)
log_progress("Process Complete")
sql_connection.close()
log_progress("Server Connection closed")