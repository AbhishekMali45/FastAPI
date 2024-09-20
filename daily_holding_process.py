import mysql.connector
from tqdm import tqdm
import pandas as pd
from sqlalchemy import create_engine,text
import logging
import os
from datetime import datetime
import numpy as np
from sqlalchemy import text
from mysql.connector import Error
from utils.config import get_settings
from datetime import datetime
from connectivity.connectdb import connectServer
from daily_holding_process_equity import generate_equity_holdings, generate_equity_holdings_without_fifo
from daily_holding_process_debt import generate_debt_holdings, generate_debt_holdings_without_fifo
from daily_holding_process_mutual_funds import generate_mf_holdings, generate_mf_holdings_without_fifo
from daily_holding_process_other_products import generate_other_prod_holdings, generate_other_prod_holdings_without_fifo
from custom_logger import hap_logger
from datetime import datetime, timedelta



#connect to FinWiz_Migration schema
engine = connectServer('alchemy_mysql_migration')
# print('hello')
CLIENTCODE = 132
CLIENT_TYPE = 'A'
TILL_DATE = '2023-12-31'

# CREATE PROCEDURE EQUITY_HOLDING_V1(IN CLIENTCODE INT, IN CLIENT_TYPE CHAR(1), IN TILL_DATE DATE)

def get_client_list(till_date):
    sql_query = f"""SELECT DISTINCT ACCOUNT_CODE FROM FT_TRANSACTION WHERE DATE(TRAN_DATE) = '{till_date}';"""
    client_list = pd.read_sql(sql_query, con=engine)
    return client_list


def get_updated_backedated_data_equity(till_date):
    sql_query = f"""SELECT ACCOUNT_CODE, SCRIP_CODE, TRADE_DATE FROM FinWiz_Migration.FT_EQUITY_TRADE 
        WHERE DATE(TRADE_DATE) < '{till_date}' AND DATE(UPDATED_ON) = '{till_date}';"""
    EQUITYU_SCRIP_CODE_MAX_DATE_df = pd.read_sql(sql_query, con=engine)
    return EQUITYU_SCRIP_CODE_MAX_DATE_df


def get_updated_backedated_data_debt(till_date):
    sql_query = f""""SELECT ACCOUNT_CODE, DEBT_CODE, TRAN_DATE 
        FROM FinWiz_Migration.FT_DEBT_TRANSACTION
        WHERE 
        DATE(TRAN_DATE) < '{till_date}' 
        AND 
        DATE(UPDATED_ON) = '{till_date}';
    """ 
    EQUITYU_SCRIP_CODE_MAX_DATE_df = pd.read_sql(sql_query, con=engine)
    return EQUITYU_SCRIP_CODE_MAX_DATE_df


def get_updated_backedated_data_mf(till_date):
    sql_query = f""""SELECT ACCOUNT_CODE, SCRIP_CODE, TRADE_DATE 
        FROM FinWiz_Migration.FT_MF_TRANSACTION
        WHERE 
        DATE(TRADE_DATE) < '{till_date}' 
        AND 
        DATE(UPDATED_ON) = '{till_date}';
    """ 
    EQUITYU_SCRIP_CODE_MAX_DATE_df = pd.read_sql(sql_query, con=engine)
    return EQUITYU_SCRIP_CODE_MAX_DATE_df


def get_updated_backedated_data_other_product(till_date):
    sql_query = f""""SELECT ACCOUNT_CODE, SCRIP_CODE, TRADE_DATE 
        FROM FinWiz_Migration.FT_MF_TRANSACTION
        WHERE 
        DATE(TRADE_DATE) < '{till_date}' 
        AND 
        DATE(UPDATED_ON) = '{till_date}';
    """ 
    EQUITYU_SCRIP_CODE_MAX_DATE_df = pd.read_sql(sql_query, con=engine)
    return EQUITYU_SCRIP_CODE_MAX_DATE_df


def get_client_list_without_fifo():
    sql_query = f"""SELECT DISTINCT ACCOUNT_CODE FROM FT_TRANSACTION;"""
    client_list = pd.read_sql(sql_query, con=engine)
    return client_list

##########################################################################################################################
import time



from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor

def process_holdings(account_code, till_date):
    with ThreadPoolExecutor() as executor:
        equity_future = executor.submit(generate_equity_holdings, account_code=account_code, till_date=till_date)
        debt_future = executor.submit(generate_debt_holdings, account_code=account_code, till_date=till_date)
        mf_future = executor.submit(generate_mf_holdings, account_code=account_code, till_date=till_date)
        other_prod_future = executor.submit(generate_other_prod_holdings, account_code=account_code, till_date=till_date)
        
        equity_df = equity_future.result()
        debt_df = debt_future.result()
        mf_df = mf_future.result()
        other_prod_df = other_prod_future.result()
    
    return [equity_df, debt_df, mf_df, other_prod_df]


def process_client(account_code):
    results = process_holdings(account_code, '2023-12-31')
    client_df = pd.concat(results, axis=0, ignore_index=True)
    return client_df


def get_holding_cost_and_value(account_list, till_date):
    till_yesterday_date = datetime.strptime(till_date, '%Y-%m-%d') - timedelta(days=1)
    sql_query = f"""SELECT * FROM FT_HOLDING WHERE ACCOUNT_CODE IN {account_list} AND HOLDING_DATE = '{till_yesterday_date.strftime('%Y-%m-%d')}';"""
    holding_cost_and_value = pd.read_sql(sql_query, con=engine)
    return holding_cost_and_value


def process_holdings_without_fifo(account_code, till_date):
    with ThreadPoolExecutor() as executor:
        equity_future = executor.submit(generate_equity_holdings_without_fifo, account_code=account_code, till_date=till_date)
        debt_future = executor.submit(generate_debt_holdings_without_fifo, account_code=account_code, till_date=till_date)
        mf_future = executor.submit(generate_mf_holdings_without_fifo, account_code=account_code, till_date=till_date)
        other_prod_future = executor.submit(generate_other_prod_holdings_without_fifo, account_code=account_code, till_date=till_date)
        
        equity_df = equity_future.result()
        debt_df = debt_future.result()
        mf_df = mf_future.result()
        other_prod_df = other_prod_future.result()
    
    return [equity_df, debt_df, mf_df, other_prod_df]


def process_client_without_fifo(account_code):
    results = process_holdings_without_fifo(account_code, '2023-12-31')
    client_df = pd.concat(results, axis=0, ignore_index=True)
    return client_df


def calculate_fifo_holdings():
    start_time = time.time()
    client_list = get_client_list(till_date='2023-12-31')
    client_list = client_list['ACCOUNT_CODE'].to_list()
    
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_client, client_list))
    
    max_client_df = pd.concat(results, axis=0, ignore_index=True)
    if not max_client_df.empty:
        if "VALUE_DATE" in max_client_df.columns:
            max_client_df.drop(columns=['VALUE_DATE'], inplace=True)
        if "CLOSING_PRICE" in max_client_df.columns:
            max_client_df.rename(columns={'CLOSING_PRICE':'CURRENT_MARKET_PRICE'}, inplace=True)
        max_client_df["HOLDING_QUANTITY"].fillna(0, inplace=True)
        max_client_df["CURRENT_MARKET_PRICE"].fillna(0, inplace=True)
        max_client_df["REPORTING_HOLDING_COST"] = max_client_df["HOLDING_COST"]
        max_client_df["REPORTING_HOLDING_VALUE"] = max_client_df["HOLDING_VALUE"]
        max_client_df["REPORTING_UNREALIZED_GAINLOSS"] = max_client_df["UNREALIZED_GAINLOSS"]
        max_client_df["BASE_HOLDING_COST"] = max_client_df["HOLDING_COST"]
        max_client_df["BASE_HOLDING_VALUE"] = max_client_df["HOLDING_VALUE"]
        max_client_df["BASE_UNREALIZED_GAINLOSS"] = max_client_df["UNREALIZED_GAINLOSS"]
        max_client_df["HOLDING_DATE"] = pd.to_datetime(max_client_df["HOLDING_DATE"])
        max_client_df["CREATED_ON"] = datetime.now()
        max_client_df["UPDATED_ON"] = datetime.now()
        # if max_client_df.shape[0] > 100000:
            # max_client_df[""]
        try:    
            # max_client_df.to_sql('FT_HOLDING', con=engine, if_exists='append', index=False)
            max_client_df.to_csv('FT_HOLDING.csv', index=False)
        except Exception as e:
            hap_logger.error(f"Error in inserting data into FT_HOLDING: {e}")
            
    # max_client_df.to_csv('max_client_df.csv')
        # max_client_df = pd.DataFrame()
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")


def calculate_fifo_holdings_without_fifo():
    start_time = time.time()
    client_list = get_client_list(till_date='2023-12-31')
    client_list_all = get_client_list_without_fifo()
    client_list = client_list['ACCOUNT_CODE'].to_list()
    client_list_all = client_list_all['ACCOUNT_CODE'].to_list()
    client_list = list(set(client_list_all) - set(client_list))
    print(len(client_list))
    client_list = client_list[:10]
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_client_without_fifo, client_list))
    
    max_client_df = pd.concat(results, axis=0, ignore_index=True)
    if not max_client_df.empty:
        # if "VALUE_DATE" in max_client_df.columns:
        #     max_client_df.drop(columns=['VALUE_DATE'], inplace=True)
        # if "CLOSING_PRICE" in max_client_df.columns:
        #     max_client_df.rename(columns={'CLOSING_PRICE':'CURRENT_MARKET_PRICE'}, inplace=True)
        # max_client_df["HOLDING_QUANTITY"].fillna(0, inplace=True)
        # max_client_df["CURRENT_MARKET_PRICE"].fillna(0, inplace=True)
        # max_client_df["REPORTING_HOLDING_COST"] = max_client_df["HOLDING_COST"]
        # max_client_df["REPORTING_HOLDING_VALUE"] = max_client_df["HOLDING_VALUE"]
        # max_client_df["REPORTING_UNREALIZED_GAINLOSS"] = max_client_df["UNREALIZED_GAINLOSS"]
        # max_client_df["BASE_HOLDING_COST"] = max_client_df["HOLDING_COST"]
        # max_client_df["BASE_HOLDING_VALUE"] = max_client_df["HOLDING_VALUE"]
        # max_client_df["BASE_UNREALIZED_GAINLOSS"] = max_client_df["UNREALIZED_GAINLOSS"]
        max_client_df["HOLDING_DATE"] = pd.to_datetime(max_client_df["HOLDING_DATE"])
        max_client_df["CREATED_ON"] = datetime.now()
        max_client_df["UPDATED_ON"] = datetime.now()
        # if max_client_df.shape[0] > 100000:
            # max_client_df[""]
        try:    
            # max_client_df.to_sql('FT_HOLDING', con=engine, if_exists='append', index=False)
            max_client_df.to_csv('FT_HOLDING_without_fifo.csv', index=False)
        except Exception as e:
            hap_logger.error(f"Error in inserting data into FT_HOLDING: {e}")
            
    # max_client_df.to_csv('max_client_df.csv')
        # max_client_df = pd.DataFrame()
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.2f} seconds")



if __name__ == '__main__':

    # Set today's date as till_date
    till_date = datetime.now().strftime('%Y-%m-%d')

    # Set yesterday's date as yesterday_date
    yesterday_date = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    calculate_fifo_holdings()
    calculate_fifo_holdings_without_fifo()



    sql_query = f"""CALL CASH_HOLDING('{yesterday_date}','{till_date}')"""
    pd.read_sql(sql_query, con=engine)

# if __name__ == '__main__':
#     start_time = time.time()
#     client_list = get_client_list()
#     client_list = client_list['ACCOUNT_CODE'].to_list()
#     max_client_df = pd.DataFrame()
#     for i in client_list:
#         eqity_df = generate_equity_holdings(account_code=i, till_date='2023-12-31')
#         debt_df = generate_debt_holdings(account_code=i, till_date='2023-12-31')
#         mf_df = generate_mf_holdings(account_code=i, till_date='2023-12-31')
#         other_prod_df = generate_other_prod_holdings(account_code=i, till_date='2023-12-31')
#         client_df = pd.concat([eqity_df, debt_df, mf_df, other_prod_df], axis=0, ignore_index=True)
#         max_client_df = pd.concat([max_client_df, client_df], axis=0, ignore_index=True)
#         if max_client_df.shape[0] > 100000:
#             max_client_df.to_csv('max_client_df.csv')
#             max_client_df = pd.DataFrame()
#     max_client_df.to_csv('max_client_df.csv')
#     end_time = time.time()
    
#     execution_time = end_time - start_time
#     print(f"Execution time: {execution_time:.2f} seconds")
#     # generate_equity_holdings(account_code=1132, till_date='2023-12-31')



    
