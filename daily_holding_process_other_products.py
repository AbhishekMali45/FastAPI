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
from custom_logger import hap_logger

#connect to FinWiz_Migration schema
engine = connectServer('alchemy_mysql_migration')
print('hello')
CLIENTCODE = 132
CLIENT_TYPE = 'A'
TILL_DATE = '2023-12-31'

# CREATE PROCEDURE EQUITY_HOLDING_V1(IN CLIENTCODE INT, IN CLIENT_TYPE CHAR(1), IN TILL_DATE DATE)

def cancel_out_transactions(input_df):
    # Initialize new DataFrame to store the results
    result_df = pd.DataFrame(columns=input_df.columns)
    input_df["TRANSACTION_TYPE"] = input_df["TRANSACTION_TYPE"].str.strip().str.upper() 
    
    # Group by SCRIP_CODE and TRADE_DATE
    grouped = input_df.groupby(["SCRIP_CODE", "TRADE_DATE"])
    
    for (scrip_code, date), group in grouped:
        purchases = group[group["TRANSACTION_TYPE"].str.upper() == "PURCHASE"].copy()
        sells = group[group["TRANSACTION_TYPE"].str.upper() == "SELL"].copy()
        
        # Skip if either purchases or sells are missing
        if purchases.empty or sells.empty:
            result_df = pd.concat([result_df, group])
            continue
        
        for i, purchase in purchases.iterrows():
            remaining_qty = purchase["DELIV_QUANTITY"]
            for j, sell in sells.iterrows():
                if remaining_qty == 0:
                    break
                if sell["DELIV_QUANTITY"] > 0:
                    if sell["DELIV_QUANTITY"] == remaining_qty:
                        remaining_qty = 0
                        sells.at[j, "DELIV_QUANTITY"] = 0
                    elif sell["DELIV_QUANTITY"] < remaining_qty:
                        remaining_qty -= sell["DELIV_QUANTITY"]
                        sells.at[j, "DELIV_QUANTITY"] = 0
                    else:
                        sells.at[j, "DELIV_QUANTITY"] -= remaining_qty
                        remaining_qty = 0
            purchases.at[i, "DELIV_QUANTITY"] = remaining_qty
            purchase["DELIV_QUANTITY"] = remaining_qty
            if remaining_qty > 0:
                result_df = pd.concat([result_df, purchase.to_frame().T])
        
        for j, sell in sells.iterrows():
            if sell["DELIV_QUANTITY"] > 0:
                result_df = pd.concat([result_df, sell.to_frame().T])

    return result_df.reset_index(drop=True)


def get_client_list():
    family_code = 5171
    sql_query = f"""SELECT ACCOUNT_CODE FROM DM_ACCOUNT WHERE FAMILY_CODE = {family_code};"""
    client_list = pd.read_sql(sql_query, con=engine)
    return client_list

def generate_other_prod_price_based_temp(account_code, till_date):
    """THIS IS TEMPORARY FUNCTION"""

    sql_query = f""" SELECT ACCOUNT_CODE, PRODUCT_CODE, VALUE_DATE, ACCOUNT_NO, INVEST_DATE, INVEST_AMOUNT, INVEST_VALUE  
	, CLIENT_PROSPECT_CODE, TRADE_CURRENCY, REPORTING_CURRENCY, BASE_CURRENCY  
	FROM (SELECT ROW_NUMBER () OVER (PARTITION BY V.ACCOUNT_CODE, V.PRODUCT_CODE, LTRIM(RTRIM(REPLACE(REPLACE(IFNULL(V.ACCOUNT_NO,''),CHAR(13),''),CHAR(10),''))), V.VALUE_DATE ORDER BY V.ENTRY_NO DESC) AS ID
		, V.ENTRY_NO, V.ACCOUNT_CODE, V.PRODUCT_CODE, V.VALUE_DATE, LTRIM(RTRIM(REPLACE(REPLACE(IFNULL(V.ACCOUNT_NO,''),CHAR(13),''),CHAR(10),''))) AS ACCOUNT_NO, V.INVEST_DATE, V.INVEST_AMOUNT, V.INVEST_VALUE
		, V.CLIENT_PROSPECT_CODE, V.TRADE_CURRENCY, V.REPORTING_CURRENCY, V.BASE_CURRENCY
		FROM FinWiz_Migration.FT_OTHER_PROD_CURR_VALUE V
		LEFT JOIN FinWiz_Migration.DM_OTHER_PRODUCT D ON V.PRODUCT_CODE = D.PRODUCT_CODE
		WHERE D.UNIT_BASED = 0
		AND V.ACCOUNT_CODE IN {account_code}) A
	WHERE ID = 1;"""


def generate_other_prod_price_based(account_code, till_date):
    sql_query = f""" SELECT ACCOUNT_CODE, PRODUCT_CODE as SCRIP_CODE, VALUE_DATE as HOLDING_DATE, ACCOUNT_NO as FOLIO_NO, 
    INVEST_DATE, INVEST_AMOUNT as HOLDING_COST, INVEST_VALUE as HOLDING_VALUE  
	FROM (SELECT ROW_NUMBER () OVER (PARTITION BY V.ACCOUNT_CODE, V.PRODUCT_CODE, LTRIM(RTRIM(REPLACE(REPLACE(IFNULL(V.ACCOUNT_NO,''),CHAR(13),''),CHAR(10),''))), V.VALUE_DATE ORDER BY V.ENTRY_NO DESC) AS ID
		, V.ENTRY_NO, V.ACCOUNT_CODE, V.PRODUCT_CODE, V.VALUE_DATE, LTRIM(RTRIM(REPLACE(REPLACE(IFNULL(V.ACCOUNT_NO,''),CHAR(13),''),CHAR(10),''))) AS ACCOUNT_NO, V.INVEST_DATE, V.INVEST_AMOUNT, V.INVEST_VALUE
		, V.CLIENT_PROSPECT_CODE, V.TRADE_CURRENCY, V.REPORTING_CURRENCY, V.BASE_CURRENCY
		FROM FinWiz_Migration.FT_OTHER_PROD_CURR_VALUE V
		LEFT JOIN FinWiz_Migration.DM_OTHER_PRODUCT D ON V.PRODUCT_CODE = D.PRODUCT_CODE
		WHERE D.UNIT_BASED = 0
		AND V.ACCOUNT_CODE = {account_code}) A
	WHERE ID = 1;"""

    try:
        other_products_df = pd.read_sql(sql_query, con=engine)
    except Exception as e:
        hap_logger.error(f"Error in fetching other products value based for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()
    if "VALUE_DATE" in other_products_df.columns:
        other_products_df['HOLDING_DATE'] = other_products_df['VALUE_DATE']
        other_products_df.drop(columns=['VALUE_DATE'], inplace=True)
    other_products_df['HOLDING_DATE'] = pd.to_datetime(other_products_df['HOLDING_DATE'])
    other_products_df = other_products_df[other_products_df['HOLDING_DATE'] <= till_date].copy()
    other_products_df = other_products_df.loc[other_products_df.groupby('FOLIO_NO')['HOLDING_DATE'].idxmax()].copy()
    other_products_df = other_products_df[other_products_df['HOLDING_VALUE'] > 0].copy()
    return other_products_df


def generate_other_prod_holdings_without_fifo(account_code, till_date):    
    sql_query = f"""SELECT T.ACCOUNT_CODE, T.PRODUCT_CODE AS SCRIP_CODE, 
        LTRIM(RTRIM(T.FOLIO_NO)) AS FOLIO_NO, T.TRADE_DATE AS TRADE_DATE 
        , (T.UNITS) AS DELIV_QUANTITY, T.RATE AS TRADE_PRICE
        , TRAN_TYPE
        , (T.TOTAL_AMOUNT) AS HOLDING_COST
        ,T.TRANSACTION_TYPE
        FROM FinWiz_Migration.FT_OTHER_PROD_TRANSACTION T
        LEFT JOIN FinWiz_Migration.DM_OTHER_PRODUCT D ON T.PRODUCT_CODE = D.PRODUCT_CODE
        WHERE T.TRADE_DATE <= '{till_date}' AND D.UNIT_BASED = 1
        AND T.ACCOUNT_CODE = {account_code}; 
        """
    ##############################################################################
    try:
        MF_TRANSACTION_df = pd.read_sql(sql_query, con=engine)
    except Exception as e:
        hap_logger.error(f"Error in fetching other products transactions for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()

    if MF_TRANSACTION_df.empty:
        return pd.DataFrame()
    # SELECT '1 COMPLETED';
    print('MF_TRANSACTION DONE') #1618 rows

    MF_TRANSACTION_list = MF_TRANSACTION_df['SCRIP_CODE'].unique()
    MF_TRANSACTION_list = ', '.join(map(str, MF_TRANSACTION_list))

    sql_query = f"""
                    SELECT A.OTHER_PRODUCT_CODE AS SCRIP_CODE, P.CLOSE AS CLOSE_PRICE, A.CLOSE_DATE
                    FROM (SELECT OTHER_PRODUCT_CODE, MAX(DATE) AS CLOSE_DATE FROM FT_OTHER_PROD_CLOSING_PRICE
                                WHERE DATE <= '{till_date}' AND OTHER_PRODUCT_CODE IN ({MF_TRANSACTION_list})
                    GROUP BY OTHER_PRODUCT_CODE) A
                    LEFT JOIN FT_OTHER_PROD_CLOSING_PRICE P ON A.OTHER_PRODUCT_CODE = P.OTHER_PRODUCT_CODE AND A.CLOSE_DATE = P.DATE;
                """ 

    results = []
    try:
        MF_CLOSING_PRICE_df = pd.read_sql(sql_query, con=engine)
    except Exception as e:
        hap_logger.error(f"Error in fetching other products closing price for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()
    
    # MF_TRANSACTION_df.to_csv('MF_TRANSACTION_df.csv')
    # MF_TRANSACTION_df = cancel_out_transactions(MF_TRANSACTION_df) 
    MF_TRANSACTION_df['FINAL_QUANTITY'] = np.where(MF_TRANSACTION_df["TRAN_TYPE"] == "S", -MF_TRANSACTION_df["DELIV_QUANTITY"], MF_TRANSACTION_df["DELIV_QUANTITY"])
    MF_TRANSACTION_df['FOLIO_NO'] = MF_TRANSACTION_df['FOLIO_NO'].astype(str) 
    MF_TRANSACTION_df['FOLIO_NO'] = MF_TRANSACTION_df['FOLIO_NO'].str.strip() 
    grouped_scrip_folio = MF_TRANSACTION_df.groupby(['SCRIP_CODE', 'FOLIO_NO'])
    try:
        for (scrip_code, folio_no), group in grouped_scrip_folio:
            cumulative_quantity = 0
            purchase_rows_df = pd.DataFrame()
            temp_debt_df = group.copy()
            temp_holding_quantity = temp_debt_df['FINAL_QUANTITY'].sum()
            if temp_holding_quantity > 1e-4:
                closing_price = MF_CLOSING_PRICE_df.loc[MF_CLOSING_PRICE_df['SCRIP_CODE'] == scrip_code, 'CLOSE_PRICE'].values
                if len(closing_price) > 0:
                    closing_price = closing_price[0]
                    closing_holding_value = temp_holding_quantity * closing_price
                else:
                    closing_price = None
                    closing_holding_value = None
                

                results.append({
                    'ACCOUNT_CODE': account_code,
                    'SCRIP_CODE': scrip_code,
                    'HOLDING_QUANTITY': temp_holding_quantity,
                    'CLOSING_PRICE': closing_price,
                    'HOLDING_VALUE': closing_holding_value,
                    'FOLIO_NO': folio_no
                })
    except Exception as e:
        hap_logger.error(f"Error in processing other products holdings for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()
    value_based =  generate_other_prod_price_based(account_code=account_code, till_date=till_date)
    results_df = pd.DataFrame(results)
    if not value_based.empty:
        try:
            results_df = pd.concat([results_df, value_based], ignore_index=True)
        except Exception as e:
            hap_logger.error(f"Error in concatenating value based data for account code {account_code} till date {till_date} error: {e}")
            return pd.DataFrame()
    # value_based.to_csv("value_based_data.csv")
    if results:
        # results_df.fillna({"HOLDING_VALUE": results_df["HOLDING_COST"]}, inplace=True)
        # results_df["UNREALIZED_GAINLOSS"] = results_df["HOLDING_VALUE"] - results_df["HOLDING_COST"]
        results_df["SOURCE"] = "OTHER PRODUCT"
        results_df["HOLDING_DATE"] = till_date
        results_df["SOURCE_ID"] = 5
        return results_df
    else:
        hap_logger.error(f"No results found for account code {account_code} till date {till_date}")
        return pd.DataFrame()



def generate_other_prod_holdings(account_code, till_date):
    
    sql_query = f"""SELECT T.ACCOUNT_CODE, T.PRODUCT_CODE AS SCRIP_CODE, 
        LTRIM(RTRIM(T.FOLIO_NO)) AS FOLIO_NO, T.TRADE_DATE AS TRADE_DATE 
        , (T.UNITS) AS DELIV_QUANTITY, T.RATE AS TRADE_PRICE
        , TRAN_TYPE
        , (T.TOTAL_AMOUNT) AS HOLDING_COST
        ,T.TRANSACTION_TYPE
        FROM FinWiz_Migration.FT_OTHER_PROD_TRANSACTION T
        LEFT JOIN FinWiz_Migration.DM_OTHER_PRODUCT D ON T.PRODUCT_CODE = D.PRODUCT_CODE
        WHERE T.TRADE_DATE <= '{till_date}' AND D.UNIT_BASED = 1
        AND T.ACCOUNT_CODE = {account_code}; 
        """
    ##############################################################################
    try:
        MF_TRANSACTION_df = pd.read_sql(sql_query, con=engine)
    except Exception as e:
        hap_logger.error(f"Error in fetching other products transactions for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()

    if MF_TRANSACTION_df.empty:
        return pd.DataFrame()
    # SELECT '1 COMPLETED';
    print('MF_TRANSACTION DONE') #1618 rows

    MF_TRANSACTION_list = MF_TRANSACTION_df['SCRIP_CODE'].unique()
    MF_TRANSACTION_list = ', '.join(map(str, MF_TRANSACTION_list))

    # sql_query = f"""SELECT SCRIP_CODE, MAX(CLOSE_DATE) AS CLOSE_DATE FROM FT_EQUITY_CLOSING_PRICE
    # 	WHERE CLOSE_DATE <= '{TILL_DATE}' AND SCRIP_CODE IN ({MF_TRANSACTION_list}) GROUP BY SCRIP_CODE;"""

    # EQUITYU_SCRIP_CODE_MAX_DATE_df = pd.read_sql(sql_query, con=engine)
    # SELECT '2 COMPLETED';

    sql_query = f"""
                    SELECT A.OTHER_PRODUCT_CODE AS SCRIP_CODE, P.CLOSE AS CLOSE_PRICE, A.CLOSE_DATE
                    FROM (SELECT OTHER_PRODUCT_CODE, MAX(DATE) AS CLOSE_DATE FROM FT_OTHER_PROD_CLOSING_PRICE
                                WHERE DATE <= '{till_date}' AND OTHER_PRODUCT_CODE IN ({MF_TRANSACTION_list})
                    GROUP BY OTHER_PRODUCT_CODE) A
                    LEFT JOIN FT_OTHER_PROD_CLOSING_PRICE P ON A.OTHER_PRODUCT_CODE = P.OTHER_PRODUCT_CODE AND A.CLOSE_DATE = P.DATE;
                """ 
# "9887", "62498", "36777", "9886", "10546", "5460", "10661", "5271", "5460", "10784", "5271", "6763",

    results = []
    try:
        MF_CLOSING_PRICE_df = pd.read_sql(sql_query, con=engine)
    except Exception as e:
        hap_logger.error(f"Error in fetching other products closing price for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()
    
    # MF_TRANSACTION_df.to_csv('MF_TRANSACTION_df.csv')
    # MF_TRANSACTION_df = cancel_out_transactions(MF_TRANSACTION_df) 
    MF_TRANSACTION_df['FINAL_QUANTITY'] = np.where(MF_TRANSACTION_df["TRAN_TYPE"] == "S", -MF_TRANSACTION_df["DELIV_QUANTITY"], MF_TRANSACTION_df["DELIV_QUANTITY"])
    MF_TRANSACTION_df['FOLIO_NO'] = MF_TRANSACTION_df['FOLIO_NO'].astype(str) 
    MF_TRANSACTION_df['FOLIO_NO'] = MF_TRANSACTION_df['FOLIO_NO'].str.strip() 
    grouped_scrip_folio = MF_TRANSACTION_df.groupby(['SCRIP_CODE', 'FOLIO_NO'])
    try:
        for (scrip_code, folio_no), group in grouped_scrip_folio:
            cumulative_quantity = 0
            purchase_rows_df = pd.DataFrame()
            temp_debt_df = group.copy()
            temp_holding_quantity = temp_debt_df['FINAL_QUANTITY'].sum()
            if temp_holding_quantity > 1e-4:
                closing_price = MF_CLOSING_PRICE_df.loc[MF_CLOSING_PRICE_df['SCRIP_CODE'] == scrip_code, 'CLOSE_PRICE'].values
                if len(closing_price) > 0:
                    closing_price = closing_price[0]
                    closing_holding_value = temp_holding_quantity * closing_price
                else:
                    closing_price = None
                    closing_holding_value = None
                
                temp_debt_df = temp_debt_df[temp_debt_df["DELIV_QUANTITY"] != 0].copy()
                for index, row in temp_debt_df.iterrows():
                    if row["TRAN_TYPE"] == 'B':
                        cumulative_quantity += float(row["DELIV_QUANTITY"])
                        # temp_debt_df.at[index, 'actual_quantity'] = cumulative_quantity
                        purchase_rows_dict = {
                            'filtered_index':index,
                            "tran_date": row["TRADE_DATE"],
                            "quantity": float(row["DELIV_QUANTITY"]),
                            "rate": float(row['TRADE_PRICE']),
                            "folio_no": row['FOLIO_NO']
                        }
                        purchase_row = pd.DataFrame([purchase_rows_dict])
                        purchase_rows_df = pd.concat([purchase_rows_df, purchase_row], ignore_index=True)
                    
                    elif row['TRAN_TYPE'] == 'S':
                        if cumulative_quantity > 0.0:
                            
                            sell_quantity = float(row["DELIV_QUANTITY"])
                            remaining_sell_quantity = sell_quantity

                            if sell_quantity == cumulative_quantity:
                                cumulative_quantity = 0
                                purchase_rows_df = pd.DataFrame()
                            elif sell_quantity < cumulative_quantity:
                                for purchase_index, purchase_row in purchase_rows_df.iterrows():
                                    if remaining_sell_quantity <= 0:
                                        break
                                    purchase_quantity = purchase_row['quantity']
                                    if remaining_sell_quantity >= purchase_quantity:
                                        remaining_sell_quantity -= purchase_quantity
                                        if purchase_row['filtered_index'] in temp_debt_df.index:
                                            temp_debt_df.drop(purchase_row['filtered_index'], inplace=True)
                                            purchase_rows_df.drop(purchase_index, inplace=True)
                                    else:
                                        purchase_rows_df.at[purchase_index, 'quantity'] -= remaining_sell_quantity
                                        remaining_sell_quantity = 0
                                        temp_debt_df.at[purchase_row['filtered_index'], 'actual_quantity'] = purchase_rows_df.at[purchase_index, 'quantity']
                            else:
                                hap_logger.error(f"Error in debt transaction processing for account code {account_code} scrip_code {scrip_code}")
                    if index == temp_debt_df.index[-1]:
                        # Code to execute if the index is the last of the temp_debt_df.iterrows
                        temp_debt_df = temp_debt_df[temp_debt_df['FINAL_QUANTITY'] != 0.0]

                reporting_holding_cost = 0
                if not purchase_rows_df.empty:
                    # filtered_df['actual_quantity'] = np.where(filtered_df['tran_type'] == 'S', -filtered_df['actual_quantity'], filtered_df['actual_quantity'])
                    purchase_rows_df['rate'] = purchase_rows_df['rate'].astype(float)
                    purchase_rows_df['quantity'] = purchase_rows_df['quantity'].astype(float)
                    purchase_rows_df['actual_cost'] = purchase_rows_df['quantity'] * purchase_rows_df['rate']
                    reporting_holding_cost =  purchase_rows_df['actual_cost'].sum() 
                # print(scrip_code, temp_holding_quantity,reporting_holding_cost )
                # print(f"SCRIP_CODE: {scrip_code}, HOLDING_QUANTITY: {temp_holding_quantity}, CLOSING_PRICE: {closing_price}, CLOSING_HOLDING_VALUE: {closing_holding_value}")
                results.append({
                    'ACCOUNT_CODE': account_code,
                    'SCRIP_CODE': scrip_code,
                    'HOLDING_QUANTITY': temp_holding_quantity,
                    'CLOSING_PRICE': closing_price,
                    'HOLDING_VALUE': closing_holding_value,
                    'HOLDING_COST': reporting_holding_cost,
                    'FOLIO_NO': folio_no
                })
    except Exception as e:
        hap_logger.error(f"Error in processing other products holdings for account code {account_code} till date {till_date} error: {e}")
        return pd.DataFrame()
    value_based =  generate_other_prod_price_based(account_code=account_code, till_date=till_date)
    results_df = pd.DataFrame(results)
    if not value_based.empty:
        try:
            results_df = pd.concat([results_df, value_based], ignore_index=True)
        except Exception as e:
            hap_logger.error(f"Error in concatenating value based data for account code {account_code} till date {till_date} error: {e}")
            return pd.DataFrame()
    # value_based.to_csv("value_based_data.csv")
    if results:
        results_df.fillna({"HOLDING_VALUE": results_df["HOLDING_COST"]}, inplace=True)
        results_df["UNREALIZED_GAINLOSS"] = results_df["HOLDING_VALUE"] - results_df["HOLDING_COST"]
        results_df["SOURCE"] = "OTHER PRODUCT"
        results_df["HOLDING_DATE"] = till_date
        results_df["SOURCE_ID"] = 5
        return results_df
    else:
        hap_logger.error(f"No results found for account code {account_code} till date {till_date}")
        return pd.DataFrame()
    # SELECT '3 COMPLETED';    

if __name__ == '__main__':

    client_df = generate_other_prod_holdings(account_code=132, till_date='2023-12-31')
    #     max_client_df = pd.concat([max_client_df, client_df], ignore_index=True)
    # max_client_df.to_csv('max_client_df.csv')


    
