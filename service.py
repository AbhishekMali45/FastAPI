from models.models import AnswerList
import pandas as pd
import numpy as np
from utils.services import app_logger
import traceback
from sqlalchemy.orm import session
from models.models import DMSecurityMaster, DmAccount
from db_module.connection import engine
import warnings
warnings.filterwarnings("ignore")


# def match_transactions(transaction_df):
#     grouped = transaction_df.groupby("TRADE_DATE")
#     for date, group in grouped:
#         purchases = group[group["TRAN_TYPE"] == "Purchase"]
#         sells = group[group["TRAN_TYPE"] == "Sell"]
        
#         for i, purchase in purchases.iterrows():
#             remaining_qty = purchase["QUANTITY_CAL"]
#             for j, sell in sells.iterrows():
#                 if remaining_qty == 0:
                    
#                     break
#                 if transaction_df.at[j, "CAL_STAT"] == True:  # Only consider unmatched sells
#                     if sell["QUANTITY_CAL"] <= remaining_qty:
#                         remaining_qty -= sell["QUANTITY_CAL"]
#                         transaction_df.at[j, "CAL_STAT"] = False
#                     else:
#                         transaction_df.at[j, "QUANTITY_CAL"] -= remaining_qty
#                         remaining_qty = 0
#             transaction_df.at[i, "QUANTITY_CAL"] = remaining_qty
#             transaction_df.at[i, "CAL_STAT"] = remaining_qty != 0
#     return transaction_df


# def match_transactions(transaction_df):
#     grouped = transaction_df.groupby("TRADE_DATE")
#     for date, group in grouped:
#         purchases = group[group["TRAN_TYPE"] == "Purchase"]
#         sells = group[group["TRAN_TYPE"] == "Sell"]
#         for i, purchase in purchases.iterrows():
#             remaining_qty = purchase["QUANTITY_CAL"]
#             for j, sell in sells.iterrows():
#                 if remaining_qty == 0:
#                     break
#                 if transaction_df.at[j, "CAL_STAT"] == True:  # Only consider unmatched sells
#                     if sell["QUANTITY_CAL"] <= remaining_qty:
#                         remaining_qty -= sell["QUANTITY_CAL"]
#                         transaction_df.at[j, "CAL_STAT"] = True
#                         transaction_df.at[i, "CAL_STAT"] = False
#                         purchases.at[i, "QUANTITY_CAL"] = remaining_qty
#                         transaction_df.at[i, "QUANTITY_CAL"] = remaining_qty
#                     else:
#                         transaction_df.at[j, "QUANTITY_CAL"] -= remaining_qty
#                         sells.at[j, "QUANTITY_CAL"] = transaction_df.at[j, "QUANTITY_CAL"]
#     return transaction_df


# # Define a function to update the DataFrame
# def match_transactions(input_df):
#     # Initialize new columns
#     input_df['CAL_STAT'] = False
#     input_df['NEW_TEMP'] = input_df['TEMP']
    
#     # Iterate through each row
#     for idx, row in input_df.iterrows():
#         if row['TRAN_TYPE'] == 'Sell':
#             # Find the corresponding purchase(s) on the same day
#             same_day_purchase = input_df[(input_df['TRADE_DATE'] == row['TRADE_DATE']) & (input_df['TRAN_TYPE'] == 'Purchase')]
#             remaining_sell_temp = row['TEMP']
#             for _, purchase in same_day_purchase.iterrows():
#                 if remaining_sell_temp > 0:
#                     # Update the temperature and status
#                     if purchase['NEW_TEMP'] == remaining_sell_temp:
#                         input_df.at[idx, 'CAL_STAT'] = True
#                         input_df.at[purchase.name, 'CAL_STAT'] = True
#                         remaining_sell_temp = 0

#                     elif purchase['NEW_TEMP'] > remaining_sell_temp:
#                         input_df.at[idx, 'CAL_STAT'] = True
#                         input_df.at[purchase.name, 'CAL_STAT'] = False
#                         input_df.at[purchase.name, 'NEW_TEMP'] -= remaining_sell_temp
#                         remaining_sell_temp = 0
#                     else:
#                         remaining_sell_temp -= purchase['NEW_TEMP']
#                         input_df.at[purchase.name, 'CAL_STAT'] = True
#                         input_df.at[purchase.name, 'NEW_TEMP'] = 0
#             if remaining_sell_temp > 0:
#                 input_df.at[idx, 'CAL_STAT'] = False  # Mark sell as successful
#                 input_df.at[idx, 'NEW_TEMP'] = remaining_sell_temp  # Set remaining temp for sell

#     return input_df

# Define a function to update the DataFrame
def match_transactions(input_df):
    # Initialize new columns
    input_df['CAL_STAT'] = False
    input_df['NEW_TEMP'] = input_df['TEMP']
    
    # Iterate through each row
    for idx, row in input_df.iterrows():
        if row['TRAN_TYPE'] == 'Sell':
            # Find the corresponding purchase(s) on the same day
            same_day_purchase = input_df[(input_df['TRADE_DATE'] == row['TRADE_DATE']) & (input_df['TRAN_TYPE'] == 'Purchase')]
            remaining_sell_temp = row['TEMP']
            for _, purchase in same_day_purchase.iterrows():
                if remaining_sell_temp > 0:
                    # Update the temperature and status
                    if purchase['NEW_TEMP'] == remaining_sell_temp:
                        input_df.at[idx, 'CAL_STAT'] = True
                        input_df.at[purchase.name, 'CAL_STAT'] = True
                        input_df.at[purchase.name, 'NEW_TEMP'] = 0
                        remaining_sell_temp = 0
                    elif purchase['NEW_TEMP'] > remaining_sell_temp:
                        input_df.at[idx, 'CAL_STAT'] = True
                        input_df.at[purchase.name, 'CAL_STAT'] = False
                        input_df.at[purchase.name, 'NEW_TEMP'] -= remaining_sell_temp
                        remaining_sell_temp = 0
                    else:
                        remaining_sell_temp -= purchase['NEW_TEMP']
                        input_df.at[purchase.name, 'CAL_STAT'] = True
                        input_df.at[purchase.name, 'NEW_TEMP'] = 0
            if remaining_sell_temp > 0:
                input_df.at[idx, 'CAL_STAT'] = False
                input_df.at[idx, 'NEW_TEMP'] = remaining_sell_temp

    return input_df


async def geneate_date_range(purchase_date, interval_period, amount, interest_percentage, interest_period):
    end_date = purchase_date + pd.DateOffset(months=interval_period)
    date_range = pd.date_range(start=purchase_date, end=end_date, freq='D')
    calculated_amount = ((float(amount * interest_percentage) / 100) / interest_period)
    return date_range, calculated_amount

async def cancel_out_transactions(input_df):
    # Initialize new DataFrame to store the results
    result_df = pd.DataFrame(columns=input_df.columns)
    input_df["TRANSACTION_TYPE"] = input_df["TRANSACTION_TYPE"].str.strip().str.upper() 
    
    # Group by SCRIP_CODE and TRADE_DATE
    grouped = input_df.groupby(["ISIN", "TRADE_DATE"])
    
    for (isin, date), group in grouped:
        purchases = group[group["TRANSACTION_TYPE"] == "PURCHASE"].copy()
        sells = group[group["TRANSACTION_TYPE"] == "SELL"].copy()
        
        # Skip if either purchases or sells are missing
        if purchases.empty or sells.empty:
            result_df = pd.concat([result_df, group])
            continue
        
        for i, purchase in purchases.iterrows():
            remaining_qty = purchase["QUANTITY"]
            for j, sell in sells.iterrows():
                if remaining_qty == 0:
                    break
                if sell["QUANTITY"] > 0:
                    if sell["QUANTITY"] == remaining_qty:
                        remaining_qty = 0
                        sells.at[j, "QUANTITY"] = 0
                    elif sell["QUANTITY"] < remaining_qty:
                        remaining_qty -= sell["QUANTITY"]
                        sells.at[j, "QUANTITY"] = 0
                    else:
                        sells.at[j, "QUANTITY"] -= remaining_qty
                        remaining_qty = 0
            purchases.at[i, "QUANTITY"] = remaining_qty
            purchase["QUANTITY"] = remaining_qty
            if remaining_qty > 0:
                result_df = pd.concat([result_df, purchase.to_frame().T])
        
        for j, sell in sells.iterrows():
            if sell["QUANTITY"] > 0:
                result_df = pd.concat([result_df, sell.to_frame().T])

    return result_df.reset_index(drop=True)