from fastapi import FastAPI, APIRouter, HTTPException, Request, Response
from fastapi.params import Depends
from datetime import date
from fastapi.responses import FileResponse
from pyxirr import xirr
import tempfile, warnings
warnings.filterwarnings("ignore")
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
from fastapi import Form
# from utils.services import app_logger 
# from utils.config import get_settings
import numpy as np
import os, sys
import io
from sqlalchemy import or_, and_
from authorization.services import check_if_permission
from db_module.connection import get_db
import asyncio
from copy import deepcopy
from models.models import (
    DmAccount, 
    DmMutualFund,
    ProspectFamilyMaster,
    HDRBondGsecMaster,
    ProspectFtTransaction,
    FtMfClosingPrice,
    ProspectMFCorporateActionMaster,
    ProspectEquityCorporateActionMaster,
    FTEquityClosingPrice,
    DMDebtInstrument,
    ProspectClientMaster,
    ClientStatusMaster,
    StageProspectFtTransaction,
    StageProspectFtDebtTransaction,
    StageProspectFtPmsTransaction,
    StageProspectFtAifTransaction,
    ProspectFtMfSipTransactionMaster,
    ProspectFtMfSwitchTransactionMaster,
    ProspectFtMfStpTransactionMaster,
    ProspectFtMfSwpTransactionMaster,
    ProspectSecurityMaster,
    DMSecurityMaster,
    StageProspectFtMfTransaction,
    ProspectFtMfCorporateAction
    )
# settings = get_settings()
from schemas.schemas import CreateAccount,EmailValidation
from io import BytesIO
from fastapi import File, HTTPException, UploadFile
from prospect_client.prospect_calculation.service import match_transactions, geneate_date_range, cancel_out_transactions
from fastapi.responses import StreamingResponse
import pandas as pd
from models.models import (SlideMaster)
from sqlalchemy import desc
from utils.services import app_logger
import traceback
from utils.config import get_settings
import boto3
import math
from jose import jwt
from typing import Optional, Tuple
from botocore.exceptions import NoCredentialsError
from db_module.create import upload_bulk_file 
from db_module.read import fetch_pms_rolling_return_table, fetch_pms_underlying_table, fetch_pms_rolling_return_all,fetch_pms_underlying_all
from io import BytesIO
settings = get_settings()
from utils.basic_auth import JWTBearer, decodeJWT
from ppt_creation.services import round_half_up

prospect_corporate_calculation = APIRouter(prefix="/prospect/corporate_calculation", tags=["prospect_client"], responses={422: {"description": "Not Found"}})

async def decodeJWT(token: str) -> Optional[dict]:
    try:
        decoded_token = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
        return decoded_token if decoded_token.get("exp") >= datetime.now().timestamp() else None
    except Exception as e:
        return None



async def get_equity_prospect_data(db : Session, client_id : int, closing_date : date):
    try:
        ft_trans_data = db.query(
                ProspectFtTransaction.ADVISOR_NAME,
                ProspectFtTransaction.SCHEME_CODE,
                ProspectFtTransaction.TRADE_DATE, 
                ProspectFtTransaction.TRANSACTION_TYPE, 
                ProspectFtTransaction.TRAN_TYPE,
                ProspectFtTransaction.QUANTITY, 
                ProspectFtTransaction.RATE, 
                ProspectFtTransaction.AMOUNT, 
                ProspectFtTransaction.ACTUAL_AMOUNT,
                ProspectFtTransaction.ISIN,
                DMSecurityMaster.SCRIP_KEY).join(
                DMSecurityMaster,
                DMSecurityMaster.ISIN == ProspectFtTransaction.ISIN
                             ).filter(
            ProspectFtTransaction.CLIENT_CODE == client_id, 
            ProspectFtTransaction.STATUS == "Success", 
            ProspectFtTransaction.TRADE_DATE <= closing_date,
            # ProspectFtTransaction.ISIN.in_(["INE028A01039", "INE705A01016", "INE077A01010"]),
            ProspectFtTransaction.SOURCE_ID == 2,
            DMSecurityMaster.SOURCE_ID == 2).all()
        ft_trans_df = pd.DataFrame(ft_trans_data, columns=["ADVISOR_NAME", "SCHEME_CODE", 'TRADE_DATE', 'TRANSACTION_TYPE', "TRAN_TYPE", 'QUANTITY', 'RATE', 'AMOUNT', 'ACTUAL_AMOUNT', "ISIN", "SCRIP_CODE"])
        ft_trans_df['TRADE_DATE'] = pd.to_datetime(ft_trans_df['TRADE_DATE'], errors="coerce")
        return ft_trans_df
    except Exception as e:
        app_logger.trace(f"ERROR IN GETTING CORPORATE ACTION DATA | | {traceback.format_exc()}")
        return pd.DataFrame()



async def get_prospect_equity_corporate_action_data(db, scrip_key_list:list, closing_date:date):
    try:
        corporate_transaction = db.query(
            ProspectEquityCorporateActionMaster.SCRIP_CODE,
            ProspectEquityCorporateActionMaster.ISIN,
            ProspectEquityCorporateActionMaster.DATE, 
            ProspectEquityCorporateActionMaster.F_RATIO, 
            ProspectEquityCorporateActionMaster.T_RATIO, 
            ProspectEquityCorporateActionMaster.NET_WORTH, 
            ProspectEquityCorporateActionMaster.NOOFSHARES, 
            ProspectEquityCorporateActionMaster.DMERGER_ISIN,
            ProspectEquityCorporateActionMaster.DMERGER_SCRIP_CODE,
            ProspectEquityCorporateActionMaster.DIVIDEND_PER_SHARE, 
            ProspectEquityCorporateActionMaster.ACTION_TYPE, 
            ProspectEquityCorporateActionMaster.NET_BK_VALUE, 
            ProspectEquityCorporateActionMaster.SPLIT_TYPE, 
            ProspectEquityCorporateActionMaster.MERGER_ISIN,
            ProspectEquityCorporateActionMaster.MERGER_SCRIP_CODE,
            ).filter(
                func.trim(ProspectEquityCorporateActionMaster.ISIN).in_(scrip_key_list),
                # ProspectEquityCorporateActionMaster.ISIN.in_(["INE028A01039", "INE705A01016", "INE077A01010"]),
                ProspectEquityCorporateActionMaster.DATE <= closing_date).all()
        corporate_df = pd.DataFrame(corporate_transaction, columns=['SCRIP_CODE', "ISIN", 'TRADE_DATE', 'F_RATIO', 'T_RATIO', 'NET_WORTH', 'NOOFSHARES', 
                        'DMERGER_ISIN', 'DMERGER_SCRIP_CODE', 'RATE','TRANSACTION_TYPE', 'NET_BK_VALUE', 'SPLIT_TYPE', 'MERGER_ISIN', 'MERGER_SCRIP_CODE'])
        scrip_code_list = corporate_df["SCRIP_CODE"].dropna().drop_duplicates().to_list()
        if corporate_df["DMERGER_SCRIP_CODE"].dropna().drop_duplicates().to_list():
            scrip_code_list += corporate_df["DMERGER_SCRIP_CODE"].dropna().drop_duplicates().to_list()
        if corporate_df["MERGER_SCRIP_CODE"].dropna().drop_duplicates().to_list():
            scrip_code_list += corporate_df["MERGER_SCRIP_CODE"].dropna().drop_duplicates().to_list()
        price_data = db.query(FTEquityClosingPrice.CLOSE_DATE,
                              FTEquityClosingPrice.CLOSE_PRICE,
                              FTEquityClosingPrice.SCRIP_CODE).filter(FTEquityClosingPrice.CLOSE_DATE <= closing_date,
                                FTEquityClosingPrice.SCRIP_CODE.in_(scrip_code_list)).all()
                                # FTEquityClosingPrice.SCRIP_CODE.in_([310])).all()
        price_data_df = pd.DataFrame(price_data, columns=['TRADE_DATE', 'CLOSE_PRICE', 'SCRIP_CODE'])
        price_data_df['TRADE_DATE'] = pd.to_datetime(price_data_df['TRADE_DATE'])
        corporate_df['TRADE_DATE'] = pd.to_datetime(corporate_df['TRADE_DATE'])
        merged_df = pd.merge(corporate_df, price_data_df, on=['SCRIP_CODE', 'TRADE_DATE'], how='left')
        merged_df.sort_values(by=['ISIN', 'TRADE_DATE'], inplace=True)
        merged_df['CLOSE_PRICE'] = merged_df.groupby('ISIN')['CLOSE_PRICE'].ffill()
        # merged_df = merged_df.drop(columns=['CLOSE_DATE'])
        merged_df.dropna(subset=['TRANSACTION_TYPE'], inplace=True)  # Keep only rows that were in the original corporate_df
        corporate_df = merged_df.copy()
        merger_df = corporate_df[corporate_df["TRANSACTION_TYPE"] == "MERGER"].copy()
        corporate_df = corporate_df[corporate_df["TRANSACTION_TYPE"] != "MERGER"].copy()
        merger_df.rename(columns={"ISIN":"FINAL_ISIN",'MERGER_ISIN':"ISIN"}, inplace=True)
        corporate_df = pd.concat([corporate_df, merger_df], ignore_index=True)
        return corporate_df
    except Exception as e:
        app_logger.trace(f"EQUITY CORPORATE ACTION | | {traceback.format_exc()}")
        return pd.DataFrame()


async def get_advisor_scheme_code_equity(db:Session, client_code):
    try:
        query = db.query(ProspectFtTransaction.ADVISOR_NAME,
                         ProspectFtTransaction.ISIN,
                         DMSecurityMaster.SCRIP_KEY
                         ).join(
                             DMSecurityMaster, 
                             func.trim(ProspectFtTransaction.ISIN) == func.trim(DMSecurityMaster.ISIN),
                             isouter = True)
        query = query.filter(DMSecurityMaster.SOURCE_ID == 2, ProspectFtTransaction.CLIENT_CODE == client_code)
        query = query.all()
        query_df = pd.DataFrame(query, columns=["ADVISOR_NAME", "ISIN", "SCHEME_CODE"])
        query_df.drop_duplicates(subset=["ISIN"], inplace=True)
        return query_df
    except Exception as e:
        app_logger.trace(f"ADVISOR AND SCHEME CODE | | {traceback.format_exc()}")
        return pd.DataFrame()


async def get_advisor_scheme_code_debt(db:Session, client_code):
    try:
        query = db.query(ProspectFtTransaction.ADVISOR_NAME,
                         ProspectFtTransaction.ISIN,
                         DMSecurityMaster.SCRIP_KEY
                         ).join(
                             DMSecurityMaster, 
                             func.trim(ProspectFtTransaction.ISIN) == func.trim(DMSecurityMaster.ISIN),
                             isouter = True)
        query = query.filter(DMSecurityMaster.SOURCE_ID == 4, ProspectFtTransaction.CLIENT_CODE == client_code)
        query = query.all()
        query_df = pd.DataFrame(query, columns=["ADVISOR_NAME", "ISIN", "SCHEME_CODE"])
        query_df.drop_duplicates(subset=["ISIN"], inplace=True)
        return query_df
    except Exception as e:
        app_logger.trace(f"ADVISOR AND SCHEME CODE | | {traceback.format_exc()}")
        return pd.DataFrame()



async def calculation_for_equity_corp_action(client_id:int, closing_date:date, db:Session, user_id:int):
    try:
        ft_trans_df_all = await get_equity_prospect_data(db, client_id, closing_date)
        if ft_trans_df_all.empty:
            app_logger.info(f"EQUITY CORPORATE ACTION | NO DATA FOUND | EQUITY TRANSACTION ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        corporate_df = await get_prospect_equity_corporate_action_data(db, ft_trans_df_all['ISIN'].unique().tolist(), closing_date)
        if corporate_df.empty:
            app_logger.info(f"EQUITY CORPORATE ACTION | NO DATA FOUND | CORPORATE ACTION ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        ft_trans_df_all["TRANSACTION_TYPE"] = ft_trans_df_all["TRANSACTION_TYPE"].str.strip() 
        ft_trans_df_all["TRANSACTION_TYPE"] = ft_trans_df_all["TRANSACTION_TYPE"].str.upper()
        ft_trans_df_all["ISIN"] = ft_trans_df_all["ISIN"].str.strip()
        corporate_df["DMERGER_ISIN"] = corporate_df["DMERGER_ISIN"].str.strip()
        corporate_df["ISIN"] = corporate_df["ISIN"].str.strip()
        ft_trans_df_all["TRADE_DATE"] = pd.to_datetime(ft_trans_df_all["TRADE_DATE"]).dt.date
        de_merger_scrips = corporate_df[
            (corporate_df['TRANSACTION_TYPE'] == 'DMERGER') & 
            (~corporate_df['DMERGER_ISIN'].isin(ft_trans_df_all['ISIN'].unique().tolist()))
        ].dropna(subset=['DMERGER_ISIN'])['DMERGER_ISIN'].unique().tolist()
        if de_merger_scrips:
            corporate_df_new = await get_prospect_equity_corporate_action_data(db, de_merger_scrips, closing_date)
            corporate_df = pd.concat([corporate_df,corporate_df_new], ignore_index=True)
        ft_trans_df = await cancel_out_transactions(ft_trans_df_all)
        ft_trans_df_all["ACTUAL_AMOUNT"] = np.where(ft_trans_df_all['TRANSACTION_TYPE'] == 'SELL',-ft_trans_df_all['AMOUNT'], ft_trans_df_all['AMOUNT'])
        ft_trans_df["FLAG"] = 2
        corporate_df["FLAG"] = 1
        advisor_df = await get_advisor_scheme_code_equity(db, client_code=client_id)
        final_corporate_action_df = pd.DataFrame()
        if corporate_df.empty:
            app_logger.info(f"EQUITY CORPORATE ACTION | NO DATA FOUND | CORPORATE ACTION ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        trans_df = pd.concat([ft_trans_df, corporate_df], ignore_index=True)
        trans_df["TRADE_DATE"] = pd.to_datetime(trans_df["TRADE_DATE"], errors="coerce")
        trans_df.sort_values(by=["TRADE_DATE", "SCRIP_CODE"], inplace=True)
        trans_df.reset_index(drop=True, inplace=True)
        merger_scrips = corporate_df[corporate_df['TRANSACTION_TYPE'] == 'MERGER'].dropna(subset=['ISIN'])['ISIN'].unique().tolist()
        de_merger_scrips = corporate_df[corporate_df['TRANSACTION_TYPE'] == 'DMERGER'].dropna(subset=['DMERGER_ISIN'])['ISIN'].unique().tolist()
        # Remove de_merger_scrips that are also in merger_scrips
        de_merger_scrips = [scrip for scrip in de_merger_scrips if scrip not in merger_scrips]
        rest_scrips = corporate_df[~corporate_df['ISIN'].isin(merger_scrips + de_merger_scrips)]['ISIN'].dropna().unique().tolist()
        rest_scrips_trans = ft_trans_df[~ft_trans_df['ISIN'].isin(rest_scrips + merger_scrips + de_merger_scrips)]['ISIN'].dropna().unique().tolist()
        isin_list = de_merger_scrips + merger_scrips + rest_scrips + rest_scrips_trans
        merger_records_df = pd.DataFrame(columns=["ISIN"])
        demerger_records_df = pd.DataFrame(columns=["ISIN"])
        trans_df["TRANSACTION_TYPE"] = trans_df["TRANSACTION_TYPE"].str.strip() 
        trans_df["TRANSACTION_TYPE"] = trans_df["TRANSACTION_TYPE"].str.upper()
        for isin_value in isin_list:
            purchase_rows_df = pd.DataFrame()
            temp_trans_df = trans_df[trans_df['ISIN'] == isin_value].copy()
            temp_trans_df['ISIN'] = temp_trans_df['ISIN'].ffill().bfill()
            if isin_value in merger_records_df["ISIN"].to_list():
                temp_merger_df = merger_records_df[merger_records_df["ISIN"] == isin_value].copy()
                temp_trans_df = pd.concat([temp_trans_df, temp_merger_df], ignore_index=True)
                temp_trans_df["TRADE_DATE"] = pd.to_datetime(temp_trans_df["TRADE_DATE"])
            if isin_value in demerger_records_df["ISIN"].to_list():
                temp_demerger_df = demerger_records_df[demerger_records_df["ISIN"] == isin_value].copy()
                temp_trans_df = pd.concat([temp_trans_df, temp_demerger_df], ignore_index=True)
                temp_trans_df["TRADE_DATE"] = pd.to_datetime(temp_trans_df["TRADE_DATE"])
            temp_trans_df['SORT_KEY'] = temp_trans_df['TRANSACTION_TYPE'].apply(lambda x: 1 if x in ["SELL", "REDEMPTION"] else 0)
            temp_trans_df['PRIORITY'] = temp_trans_df.apply(lambda row: 1 if row['ISIN'] == row['DMERGER_ISIN'] else 0, axis=1)
            temp_trans_df.sort_values(by=["TRADE_DATE", "FLAG", "SORT_KEY", "PRIORITY"], inplace=True)
            temp_trans_df.drop(columns=['SORT_KEY'], inplace=True)
            temp_trans_df["ADVISOR_NAME"] = temp_trans_df["ADVISOR_NAME"].ffill().bfill()
            temp_trans_df["SCHEME_CODE"] = temp_trans_df["SCHEME_CODE"].ffill().bfill()
            corporate_action_df = pd.DataFrame()
            cumulative_quantity = 0
            for index, row in temp_trans_df.iterrows():
                if row["TRANSACTION_TYPE"] in ["PURCHASE", "MERGER IN", "DEMERGER IN", "CORPUS IN", "IPO"]:
                    cumulative_quantity += float(row['QUANTITY'])
                    temp_trans_df.at[index, 'ACTUAL_QUANTITY'] = cumulative_quantity
                    purchase_rows_dict = {
                        "TRADE_DATE": row["TRADE_DATE"],
                        "QUANTITY": float(row['QUANTITY']),
                        "RATE": float(row['RATE']),
                        "TRANSACTION_TYPE": "PURCHASE"
                    }
                    purchase_row = pd.DataFrame([purchase_rows_dict])
                    purchase_rows_df = pd.concat([purchase_rows_df, purchase_row], ignore_index=True)
                elif row['TRANSACTION_TYPE'] in ["SELL", "REDEMPTION", "CORPUS OUT"]:
                    if cumulative_quantity > 0.0 and cumulative_quantity >= float(row['QUANTITY']):
                        sell_quantity = float(row['QUANTITY'])
                        if sell_quantity == cumulative_quantity:
                            cumulative_quantity = 0
                            purchase_rows_df = pd.DataFrame()
                        else:
                            cumulative_quantity -= sell_quantity
                            remaining_sell_quantity = sell_quantity
                            for purchase_index, purchase_row in purchase_rows_df.iterrows():
                                if remaining_sell_quantity <= 0:
                                    break
                                purchase_quantity = purchase_row['QUANTITY']
                                if remaining_sell_quantity >= purchase_quantity:
                                    remaining_sell_quantity -= purchase_quantity
                                    purchase_rows_df.drop(purchase_index, inplace=True)
                                else:
                                    purchase_rows_df.at[purchase_index, 'QUANTITY'] -= remaining_sell_quantity
                                    remaining_sell_quantity = 0
                    else:
                        temp_trans_df.at[index, 'STATUS'] = 'Failed'

                elif row['TRANSACTION_TYPE'] == 'DIVIDEND':
                    if cumulative_quantity > 0.0:
                        temp_purchase_df = purchase_rows_df[
                                ((purchase_rows_df['TRADE_DATE'] < row['TRADE_DATE']) |
                                ((purchase_rows_df['TRADE_DATE'] == row['TRADE_DATE']) & 
                                (purchase_rows_df['TRANSACTION_TYPE'] == 'SPLIT IN')))
                            ].copy()
                        temp_cumulative_quantity = temp_purchase_df['QUANTITY'].sum()
                        rate = float(row['RATE'])  # Ensure rate is a float
                        temp_cumulative_quantity = float(temp_cumulative_quantity)  # Ensure temp_cumulative_quantity is a float
                        amount = rate * temp_cumulative_quantity
                        new_row = {
                            "ISIN": isin_value,
                            "AMOUNT": amount,
                            "ACTUAL_AMOUNT": -amount,
                            "RATE": rate,
                            "QUANTITY": 0,
                            "TRAN_TYPE":"S",
                            "TRANSACTION_TYPE":"DIVIDEND",
                            "TRADE_DATE":row["TRADE_DATE"],
                            "ADVISOR_NAME":row["ADVISOR_NAME"],
                            "SCHEME_CODE":row["SCHEME_CODE"]
                        }
                        new_row_df = pd.DataFrame([new_row])
                        corporate_action_df = pd.concat([corporate_action_df, new_row_df], ignore_index=True)                    
                    else:
                        temp_trans_df.drop(index, inplace=True)
                elif row['TRANSACTION_TYPE'] == 'BONUS':
                    if cumulative_quantity > 0.0:
                        f_ratio = float(row['F_RATIO'])  # Ensure F_RATIO is a float
                        t_ratio = float(row['T_RATIO'])  # Ensure T_RATIO is a float
                        if f_ratio != 0.0 and t_ratio != 0.0:
                            temp_purchase_df = purchase_rows_df[
                                ((purchase_rows_df['TRADE_DATE'] < row['TRADE_DATE']) |
                                ((purchase_rows_df['TRADE_DATE'] == row['TRADE_DATE']) & 
                                (purchase_rows_df['TRANSACTION_TYPE'] == 'SPLIT IN')))
                            ].copy()
                            temp_cumulative_quantity = temp_purchase_df['QUANTITY'].sum()
                            calculated_value = float(temp_cumulative_quantity) * t_ratio / f_ratio
                            # Extract the decimal part
                            decimal_part = calculated_value - math.floor(calculated_value)
                            if decimal_part > 0.9:
                                rounded_value = math.floor(calculated_value) + 1
                            else:
                                rounded_value = math.floor(calculated_value)
                            new_row = {
                                "ISIN": isin_value,
                                "RATE": 0,
                                "AMOUNT": 0,
                                "TRAN_TYPE":"B",
                                "ACTUAL_AMOUNT": 0,
                                "QUANTITY": rounded_value,
                                "TRANSACTION_TYPE":"BONUS",
                                "TRADE_DATE":row["TRADE_DATE"],
                                "ADVISOR_NAME":row["ADVISOR_NAME"],
                                "SCHEME_CODE":row["SCHEME_CODE"]
                            }
                            new_row_df = pd.DataFrame([new_row])
                            corporate_action_df = pd.concat([corporate_action_df, new_row_df], ignore_index=True)
                            cumulative_quantity += rounded_value
                            purchase_rows_dict = {
                                "TRADE_DATE": row["TRADE_DATE"],
                                "QUANTITY": rounded_value,
                                "RATE": 0,
                                "TRANSACTION_TYPE": "BONUS"
                            }
                            purchase_row = pd.DataFrame([purchase_rows_dict])
                            purchase_rows_df = pd.concat([purchase_rows_df, purchase_row], ignore_index=True)
                        else:
                            temp_trans_df.drop(index, inplace=True)
                    else:
                        temp_trans_df.drop(index, inplace=True)
                
                elif row['TRANSACTION_TYPE'] == "SPLIT":                    
                    if cumulative_quantity > 0.0:
                        if row['SPLIT_TYPE'] == "S":
                            split_type_in = "SPLIT IN"
                            split_type_out = "SPLIT OUT"
                        elif row['SPLIT_TYPE'] == "A":
                            split_type_in = "CAPITAL APPRECIATION IN"
                            split_type_out = "CAPITAL APPRECIATION OUT"
                        elif row['SPLIT_TYPE'] == 'R':
                            split_type_in = "CAPITAL REDUCTION IN"
                            split_type_out = "CAPITAL REDUCTION OUT"
                        
                        f_ratio = float(row['F_RATIO'])  # Ensure F_RATIO is a float
                        t_ratio = float(row['T_RATIO'])  # Ensure T_RATIO is a float
                        temp_purchase_df = pd.DataFrame()
                        if f_ratio != 0.0 and t_ratio != 0.0:
                            for i, purchase_row in purchase_rows_df.iterrows():
                                curr_quantity = float(purchase_row['QUANTITY'])
                                curr_rate = float(purchase_row['RATE'])
                                new_row = {
                                    "ISIN": isin_value,
                                    "RATE": curr_rate,
                                    'TRANSACTION_TYPE' : split_type_out,
                                    "QUANTITY" : purchase_row['QUANTITY'],
                                    "ACTUAL_QUANTITY": purchase_row['QUANTITY'],
                                    "AMOUNT": curr_quantity * curr_rate,
                                    "ACTUAL_AMOUNT": -1 * curr_quantity * curr_rate,
                                    "TRAN_TYPE":"S",
                                    "TRADE_DATE":row["TRADE_DATE"],
                                    "ADVISOR_NAME":row["ADVISOR_NAME"],
                                    "SCHEME_CODE":row["SCHEME_CODE"]
                                    }
                                new_row_df = pd.DataFrame([new_row])
                                corporate_action_df = pd.concat([corporate_action_df, new_row_df], ignore_index=True)
                                new_quantity = float(curr_quantity) * f_ratio/t_ratio 
                                decimal_part = new_quantity - math.floor(new_quantity)
                                if decimal_part > 0.9:
                                    rounded_value = math.floor(new_quantity) + 1
                                else:
                                    rounded_value = math.floor(new_quantity)
                                
                                new_rate = curr_rate * t_ratio/f_ratio
                                new_row_2 = {
                                    "ISIN": isin_value,
                                    'TRANSACTION_TYPE' : split_type_in,
                                    "QUANTITY" : rounded_value,
                                    'RATE' : new_rate,
                                    "AMOUNT": new_rate * rounded_value,
                                    "ACTUAL_AMOUNT":  new_rate * rounded_value,
                                    "TRAN_TYPE":"B",
                                    "TRADE_DATE":row["TRADE_DATE"],
                                    "ADVISOR_NAME":row["ADVISOR_NAME"],
                                    "SCHEME_CODE":row["SCHEME_CODE"]
                                    }
                                new_row_df_2 = pd.DataFrame([new_row_2])
                                corporate_action_df = pd.concat([corporate_action_df, new_row_df_2], ignore_index=True)
                                temp_purchase_df = pd.concat([temp_purchase_df, new_row_df_2], ignore_index=True)
                            cumulative_quantity = cumulative_quantity * f_ratio / t_ratio
                            if not temp_purchase_df.empty:
                                purchase_rows_df = temp_purchase_df[["TRADE_DATE","QUANTITY","RATE", "TRANSACTION_TYPE"]].copy()
                            else:
                                app_logger.info(f"SPLIT | MISSING DATA | ISIN:{isin_value}")
                        else:
                            temp_trans_df.drop(index, inplace=True)
                    else:
                        temp_trans_df.drop(index, inplace=True)

                elif row['TRANSACTION_TYPE'] == "DMERGER":
                    if cumulative_quantity > 0.0:
                        if row['DMERGER_ISIN'] == isin_value:
                            f_ratio = float(row['F_RATIO'])  # Ensure F_RATIO is a float
                            t_ratio = float(row['T_RATIO'])
                            acquisition_score = float(row['NET_BK_VALUE'])
                            temp_purchase_df = pd.DataFrame()
                            worth_value = float(row['NET_WORTH'])  # Ensure T_RATIO is a float
                            if f_ratio != 0.0 and t_ratio != 0.0:
                                for i, purchase_row in purchase_rows_df.iterrows():
                                    curr_quantity = float(purchase_row['QUANTITY'])
                                    curr_rate = float(purchase_row['RATE'])
                                    new_row = {
                                        "ISIN": isin_value,
                                        'TRANSACTION_TYPE' : "DEMERGER OUT",
                                        "RATE": curr_rate,
                                        "QUANTITY" : curr_quantity,
                                        "ACTUAL_QUANTITY": -curr_quantity,
                                        "AMOUNT": float(curr_quantity) * curr_rate ,
                                        "ACTUAL_AMOUNT": -1 * float(curr_quantity) * curr_rate ,
                                        "TRAN_TYPE":"S",
                                        "TRADE_DATE":row["TRADE_DATE"],
                                        "ADVISOR_NAME":row["ADVISOR_NAME"],
                                        "SCHEME_CODE":row["SCHEME_CODE"]
                                        }
                                    new_row_df = pd.DataFrame([new_row])
                                    corporate_action_df = pd.concat([corporate_action_df, new_row_df], ignore_index=True)

                                    calculated_demerger_value = float(curr_quantity) * t_ratio / f_ratio
                                    decimal_part = calculated_demerger_value - math.floor(calculated_demerger_value)
                                    if decimal_part > 0.9:
                                        rounded_value = math.floor(calculated_demerger_value) + 1
                                    else:
                                        rounded_value = math.floor(calculated_demerger_value)

                                    new_row_2 = {
                                        "ISIN": isin_value,
                                        'TRANSACTION_TYPE' : "DEMERGER IN",
                                        "QUANTITY" : rounded_value,
                                        "ACTUAL_QUANTITY": rounded_value,
                                        'RATE' : ((curr_rate * acquisition_score)/worth_value),
                                        "AMOUNT": float(rounded_value) * ((curr_rate * acquisition_score)/worth_value),
                                        "ACTUAL_AMOUNT": float(rounded_value) * ((curr_rate * acquisition_score)/worth_value),
                                        "TRAN_TYPE":"B",
                                        "TRADE_DATE":row["TRADE_DATE"],
                                        "ADVISOR_NAME":row["ADVISOR_NAME"],
                                        "SCHEME_CODE":row["SCHEME_CODE"]
                                        }
                                    new_row_df_2 = pd.DataFrame([new_row_2])
                                    corporate_action_df = pd.concat([corporate_action_df, new_row_df_2], ignore_index=True)
                                    temp_purchase_df = pd.concat([temp_purchase_df, new_row_df_2], ignore_index=True)
                                if not temp_purchase_df.empty:
                                    purchase_rows_df = temp_purchase_df[["TRADE_DATE","QUANTITY","RATE", "TRANSACTION_TYPE"]].copy()
                                    cumulative_quantity = cumulative_quantity * f_ratio / t_ratio
                                else:
                                    app_logger.info(f"DEMERGER | MISSING DATA | ISIN:{isin_value}")
                        elif row['DMERGER_ISIN'] != isin_value:
                            f_ratio = float(row['F_RATIO'])  # Ensure F_RATIO is a float
                            t_ratio = float(row['T_RATIO'])  # Ensure T_RATIO is a float
                            acquisition_score = float(row['NET_BK_VALUE'])
                            worth_value = float(row['NET_WORTH'])
                            if f_ratio != 0.0 and t_ratio != 0.0:
                                for i, purchase_row in purchase_rows_df.iterrows():
                                    curr_quantity = float(purchase_row['QUANTITY'])
                                    curr_rate = float(purchase_row['RATE'])
                                    calculated_demerger_value = float(curr_quantity) * t_ratio / f_ratio
                                    decimal_part = calculated_demerger_value - math.floor(calculated_demerger_value)
                                    if decimal_part > 0.9:
                                        rounded_value = math.floor(calculated_demerger_value) + 1
                                    else:
                                        rounded_value = math.floor(calculated_demerger_value)
                                    new_row_2 = {
                                        "ISIN": row['DMERGER_ISIN'],
                                        'TRANSACTION_TYPE' : "DEMERGER IN",
                                        "QUANTITY" : rounded_value,
                                        "ACTUAL_QUANTITY": rounded_value,
                                        'RATE' : ((curr_rate * acquisition_score)/worth_value),
                                        "AMOUNT": float(rounded_value) * ((curr_rate * acquisition_score)/worth_value),
                                        "ACTUAL_AMOUNT": float(rounded_value) * ((curr_rate * acquisition_score)/worth_value),
                                        "TRAN_TYPE":"B",
                                        "TRADE_DATE":row["TRADE_DATE"],
                                        "ADVISOR_NAME":row["ADVISOR_NAME"],
                                        "SCHEME_CODE":row["DMERGER_SCRIP_CODE"]
                                        }
                                    new_row_df_2 = pd.DataFrame([new_row_2])
                                    corporate_action_df = pd.concat([corporate_action_df, new_row_df_2], ignore_index=True)
                                    demerger_records_df = pd.concat([demerger_records_df, new_row_df_2], ignore_index=True)
                        else:
                            temp_trans_df.drop(index, inplace=True)
                    else:
                        temp_trans_df.drop(index, inplace=True)
                elif row['TRANSACTION_TYPE'] == "MERGER":
                    ratio_str = row['NOOFSHARES']
                    numerator_str, denominator_str = ratio_str.split(":")
                    numerator = float(numerator_str)
                    denominator = float(denominator_str)
                    if cumulative_quantity > 0.0:
                        if numerator != 0.0 and denominator != 0.0:
                            for i, purchase_row in purchase_rows_df.iterrows():
                                curr_quantity = float(purchase_row['QUANTITY'])
                                curr_rate = float(purchase_row['RATE'])
                                new_row = {
                                    "ISIN": isin_value,
                                    'TRANSACTION_TYPE' : "MERGER OUT",
                                    "QUANTITY" : curr_quantity,
                                    "RATE": curr_rate,
                                    "ACTUAL_QUANTITY": -curr_quantity,
                                    "AMOUNT": float(curr_quantity) * curr_rate ,
                                    "ACTUAL_AMOUNT": -float(curr_quantity) * curr_rate ,
                                    "TRAN_TYPE":"S",
                                    "TRADE_DATE":row["TRADE_DATE"],
                                    "ADVISOR_NAME":row["ADVISOR_NAME"],
                                    "SCHEME_CODE":row["SCHEME_CODE"]
                                    }
                                new_row_df = pd.DataFrame([new_row])
                                corporate_action_df = pd.concat([corporate_action_df, new_row_df], ignore_index=True)

                                calculated_demerger_value = float(curr_quantity) * numerator / denominator
                                decimal_part = calculated_demerger_value - math.floor(calculated_demerger_value)
                                if decimal_part > 0.9:
                                    rounded_value = math.floor(calculated_demerger_value) + 1
                                else:
                                    rounded_value = math.floor(calculated_demerger_value)

                                new_row_2 = {
                                    "ISIN" : row["FINAL_ISIN"],
                                    'TRANSACTION_TYPE' : "MERGER IN",
                                    "QUANTITY" : rounded_value,
                                    "ACTUAL_QUANTITY": rounded_value,
                                    'RATE' : (curr_rate * (denominator/numerator)),
                                    "AMOUNT": float(rounded_value) * (curr_rate * (denominator/numerator)),
                                    "ACTUAL_AMOUNT": float(rounded_value) * (curr_rate * (denominator/numerator)),
                                    "TRAN_TYPE":"B",
                                    "TRADE_DATE":row["TRADE_DATE"],
                                    "ADVISOR_NAME":row["ADVISOR_NAME"],
                                    "SCHEME_CODE":row["SCRIP_CODE"]
                                    }
                                new_row_df_2 = pd.DataFrame([new_row_2])
                                corporate_action_df = pd.concat([corporate_action_df, new_row_df_2], ignore_index=True)
                                merger_records_df = pd.concat([merger_records_df, new_row_df_2], ignore_index=True)
                            cumulative_quantity = 0
            final_corporate_action_df = pd.concat([final_corporate_action_df, corporate_action_df], ignore_index=True)
            # if "SCRIP_CODE" in final_corporate_action_df.columns:
            #     final_corporate_action_df.drop(columns=["SCRIP_CODE"], inplace=True)
            if "ACTUAL_QUANTITY" in final_corporate_action_df.columns:
                final_corporate_action_df.drop(columns=["ACTUAL_QUANTITY"], inplace=True)
        if final_corporate_action_df.empty:
            app_logger.info(f"EQUITY CORPORATE ACTION | NO DATA FOUND | CORPORATE ACTION ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        final_corporate_action_df["CLIENT_CODE"] = client_id
        final_corporate_action_df["SOURCE_ID"] = 2
        final_corporate_action_df["IS_CORPORATE_ACTION"] = 1
        final_corporate_action_df["STATUS"] = "Success"
        final_corporate_action_df["CREATED_BY_ID"] = final_corporate_action_df["UPDATED_BY_ID"] = user_id
        final_corporate_action_df["CREATED_DATE"] = final_corporate_action_df["UPDATED_DATE"] = datetime.now()
            
            # final_corporate_action_df = pd.concat([final_corporate_action_df, corporate_action_df], ignore_index=True)
        # Remove all records of transaction_type 'dividend' where amount is 0
        # final_corporate_action_df = pd.merge(final_corporate_action_df, advisor_df, on="ISIN", how="left")

        final_corporate_action_df = final_corporate_action_df[
            ~((final_corporate_action_df['TRANSACTION_TYPE'].str.lower() == 'dividend') & 
              (final_corporate_action_df['AMOUNT'] <= 0.001))
        ].copy()
        final_corporate_action_df = final_corporate_action_df[
            ~((final_corporate_action_df['TRANSACTION_TYPE'].str.lower() == 'demerger in') & 
              (final_corporate_action_df['QUANTITY'] <= 0.0))
        ].copy()
        # final_corporate_action_df.to_csv("final_corporate_action_df.csv", index=False)
        final_corporate_action_df["TRANSACTION_TYPE"] = final_corporate_action_df["TRANSACTION_TYPE"].str.title()
        # data_updated, msg = await upload_bulk_file(final_corporate_action_df, "PROSPECT_FT_TRANSACTION")
        # data_updated, msg = True, "OK"
        if not final_corporate_action_df.empty:
            return True, "Corporate Action Generated Successfully", final_corporate_action_df
        else:
            return False, "No Corporate Action Found", pd.DataFrame()
    except Exception as e:
        app_logger.trace(f"EQUITY CORPORATE ACTION | | {traceback.format_exc()}")
        # print(e, "ERROR IN CALCULATION FOR EQUITY CORP ACTION")
        return False, "Error in calculation for equity corp action", pd.DataFrame()



async def debt_corp_cal_v2(client_id: int, closing_date: date, db: Session, user_id:int):
    try:
        ft_trans_data = db.query( 
            ProspectFtTransaction.ISIN, 
            ProspectFtTransaction.TRADE_DATE, 
            ProspectFtTransaction.TRANSACTION_TYPE, 
            ProspectFtTransaction.QUANTITY, 
            ProspectFtTransaction.RATE, 
            ProspectFtTransaction.AMOUNT, 
            ProspectFtTransaction.ACTUAL_AMOUNT).filter(
                ProspectFtTransaction.SOURCE_ID == 4, 
                ProspectFtTransaction.CLIENT_CODE == client_id, 
                ProspectFtTransaction.TRADE_DATE <= closing_date).all()
        ft_debt_trans_df = pd.DataFrame(ft_trans_data, columns=["ISIN", 'TRADE_DATE', 'TRANSACTION_TYPE', 'QUANTITY', 'RATE', 'AMOUNT', 'ACTUAL_AMOUNT'])
        if ft_debt_trans_df.empty:
            app_logger.info(f"DEBT CORPORATE ACTION | NO DATA FOUND | ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        ft_debt_trans_df['TRADE_DATE'] = pd.to_datetime(ft_debt_trans_df['TRADE_DATE'], errors="coerce")
        ft_debt_trans_df['TRANSACTION_TYPE'] = ft_debt_trans_df['TRANSACTION_TYPE'].str.strip()
        ft_debt_trans_df['TRANSACTION_TYPE'] = ft_debt_trans_df['TRANSACTION_TYPE'].str.title()
        # ft_debt_trans_df['TRAN_TYPE'] = ft_debt_trans_df['TRANSACTION_TYPE'].replace(['Apply In IPO', 'Market Purchase', 'Market Sale', 'Opening', 'Redemption', 'Withdrawal'], ['Purchase', 'Purchase', 'Sell', 'Purchase', 'Sell', 'Sell'])
        debt_corporate = db.query(DMDebtInstrument.ISIN, 
                                  DMDebtInstrument.FACE_VALUE, 
                                  DMDebtInstrument.INTEREST_PERIOD, 
                                  DMDebtInstrument.MATURITY_DATE, 
                                  DMDebtInstrument.INTEREST_PERC, 
                                  DMDebtInstrument.INTEREST_INTERVAL, 
                                  DMDebtInstrument.INTEREST_COMPOUNDING_INTERVAL,
                                  DMDebtInstrument.ISSUE_OPEN_DATE,
                                  DMDebtInstrument.ISSUE_CLOSE_DATE,
                                  HDRBondGsecMaster.BGM_INTERESTONAPPL).join(
                                      HDRBondGsecMaster, func.trim(DMDebtInstrument.ISIN) == func.trim(HDRBondGsecMaster.BGM_ISIN), isouter=True 
                                  ).filter(
                                      func.trim(DMDebtInstrument.ISIN).in_(ft_debt_trans_df['ISIN'].unique().tolist())).all()
        debt_corporate_df = pd.DataFrame(debt_corporate, columns=["ISIN", "FACE_VALUE", "INTEREST_PERIOD" ,"MATURITY_DATE" ,"INTEREST_PERC",
                                    "INTEREST_INTERVAL" ,"INTEREST_COMPOUNDING_INTERVAL", "ISSUE_OPENING_DATE", "ISSUE_CLOSING_DATE", "IPO_INTEREST_PERC"])
        # debt_corporate_df.to_csv("debt_corporate_df.csv", index=False)
        if debt_corporate_df.empty:
            app_logger.info(f"DEBT CORPORATE ACTION | NO DATA FOUND | CORPORATE ACTION ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        debt_corporate_df['IPO_INTEREST_PERC'] = debt_corporate_df['IPO_INTEREST_PERC'].fillna(debt_corporate_df['INTEREST_PERC'])
        debt_corporate_df.drop_duplicates(subset=['ISIN'], keep='first', inplace=True)
        debt_trans_df = pd.merge(ft_debt_trans_df, debt_corporate_df, on="ISIN", how="left")
        debt_trans_df["ISIN"] = debt_trans_df["ISIN"].astype(str)
        debt_trans_df["ISIN"] = debt_trans_df["ISIN"].str.strip()
        debt_trans_df['TRADE_DATE'] = pd.to_datetime(debt_trans_df['TRADE_DATE']).dt.date
        advisor_df = await get_advisor_scheme_code_debt(db, client_code=client_id)
        xirr_final_df = pd.DataFrame()
        debt_raw_data = pd.DataFrame()
        final_debt_df = pd.DataFrame()
        for Scrip_Code in debt_trans_df['ISIN'].unique().tolist():
            try:
                df = debt_trans_df[debt_trans_df['ISIN'] == Scrip_Code].sort_values(by=['TRADE_DATE', 'TRANSACTION_TYPE'], ascending=[True, False])
                purchase_df = df[df['TRANSACTION_TYPE'].isin(['Market Purchase', 'Opening', 'Purchase', 'Apply In Ipo'])].copy()
                purchase_df['ACTUAL_AMOUNT'] = purchase_df['AMOUNT']
                redemption_df = df[df['TRANSACTION_TYPE'].isin(['Redemption', 'Market Sale', 'Withdrawal', 'Sell'])]
                redemption_df['ACTUAL_AMOUNT'] = -redemption_df['AMOUNT']
                if not purchase_df.empty:
                    interest_percentage = float(purchase_df['INTEREST_PERC'].iloc[0])
                    interest_period = purchase_df['INTEREST_PERIOD'].iloc[0]
                    interval_period = purchase_df['INTEREST_COMPOUNDING_INTERVAL'].iloc[0]
                    if interval_period == 365:
                        interval_period = 12
                else:
                    interest_percentage = 0
                    interest_period = 0
                    interval_period = 0
                calculate_df = pd.DataFrame()
                calculate_df = pd.concat([calculate_df, purchase_df], ignore_index=True)
                interest_entries = []
                cumulative_quantity = 0
                purchase_records = pd.DataFrame(columns=['TRADE_DATE', 'QUANTITY', 'RATE'])
                transactions_df = df.sort_values(by='TRADE_DATE').reset_index(drop=True)

                for index, row in transactions_df.iterrows():
                    transaction_date = row['TRADE_DATE']
                    transaction_type = row['TRANSACTION_TYPE']
                    quantity = float(row['QUANTITY'])
                    face_value = float(row['FACE_VALUE'])
                    amount = float(row['AMOUNT'])
                    interest_period = float(row['INTEREST_PERIOD'])
                    interest_type = row['INTEREST_INTERVAL']  # Assuming INTEREST_TYPE is a column indicating the type of interest (e.g., 'Cumulative', 'Non-Cumulative')
                    issue_opening_date = row['ISSUE_OPENING_DATE']  # Add issue opening date
                    issue_closing_date = row['ISSUE_CLOSING_DATE']  # Add issue closing date
                    ipo_interest_percentage = float(row['IPO_INTEREST_PERC'])  # Add IPO interest percentage

                    if index + 1 < len(transactions_df):
                        next_transaction_date = transactions_df.loc[index + 1, 'TRADE_DATE']
                    else:
                        next_transaction_date = closing_date

                    if transaction_type in ['Market Purchase', 'Opening', 'Purchase']:
                        cumulative_quantity += quantity
                        purchase_records = pd.concat([purchase_records, pd.DataFrame([{'TRADE_DATE': transaction_date, 'QUANTITY': quantity, 'RATE': row['RATE']}])], ignore_index=True)
                    elif transaction_type in ['Redemption', 'Market Sale', 'Withdrawal', 'Sell']:
                        remaining_quantity = quantity
                        for i, purchase_row in purchase_records.iterrows():
                            if remaining_quantity <= 0:
                                break
                            if purchase_row['QUANTITY'] <= remaining_quantity:
                                remaining_quantity -= purchase_row['QUANTITY']
                                purchase_records = purchase_records.drop(i)
                            else:
                                purchase_records.at[i, 'QUANTITY'] -= remaining_quantity
                                remaining_quantity = 0
                        cumulative_quantity -= quantity
                    elif transaction_type == 'Apply In Ipo':
                        # Handle Apply in IPO transaction type
                        cumulative_quantity += quantity
                        start_date_new = transaction_date
                        # purchase_records = pd.concat([purchase_records, pd.DataFrame([{'TRADE_DATE': issue_closing_date, 'QUANTITY': quantity, 'RATE': row['RATE']}])], ignore_index=True)
                        # Apply different interest percentage during issue_opening_date to issue_closing_date
                        if issue_opening_date and issue_closing_date and ipo_interest_percentage:
                            date_range = pd.date_range(start=start_date_new, end=issue_closing_date, freq='D')
                            daily_interest = ((float(quantity * ipo_interest_percentage * face_value) / 100) / interest_period)
                            interest_entries.extend([
                                {
                                    "ISIN": Scrip_Code,
                                    "TRADE_DATE": date,
                                    "AMOUNT": daily_interest,
                                    "TRANSACTION_TYPE": "Interest",
                                    "TRAN_TYPE": "S",
                                    "ACTUAL_AMOUNT": -daily_interest
                                } for date in date_range
                            ])                            
                    if interest_type == 'Cumulative' and interval_period > 0 and cumulative_quantity > 0:
                        for purchase_index, purchase_row in purchase_records.iterrows():
                            if transaction_type == 'Apply In Ipo':
                                start_date_new = issue_closing_date + pd.DateOffset(days=1)
                            else:
                                start_date_new = purchase_row['TRADE_DATE']
                            new_amount1 = purchase_row['QUANTITY'] * purchase_row['RATE']
                            while pd.Timestamp(start_date_new) < pd.Timestamp(next_transaction_date):
                                new_purchase_date = start_date_new
                                date_range, calculated_amount = await geneate_date_range(new_purchase_date, interval_period, new_amount1, interest_percentage, interest_period)
                                if next_transaction_date in date_range:
                                    date_range = deepcopy(date_range[date_range < next_transaction_date])
                                interest_entries.extend([
                                    {
                                        "ISIN": Scrip_Code,
                                        "TRADE_DATE": date,
                                        "AMOUNT": calculated_amount,
                                        "TRANSACTION_TYPE": "Interest",
                                        "TRAN_TYPE": "S",
                                        "ACTUAL_AMOUNT": -calculated_amount
                                    } for date in date_range[:-1]
                                ])
                                start_date_new = date_range[-1]
                                # Update the new amount with the accrued interest
                                new_amount1 += (calculated_amount * (len(date_range) - 1))
                    if interest_type != 'Cumulative' and cumulative_quantity > 0:
                        if transaction_type == 'Apply In Ipo':
                            transaction_date = issue_closing_date + pd.DateOffset(days=1)
                        daily_interest = ((float(cumulative_quantity * interest_percentage * face_value) / 100) / interest_period)
                        date_range = pd.date_range(start=transaction_date, end=next_transaction_date - pd.DateOffset(days=1), freq='D')
                        interest_entries.extend([
                            {
                                "ISIN": Scrip_Code,
                                "TRADE_DATE": date,
                                "AMOUNT": daily_interest,
                                "TRANSACTION_TYPE": "Interest",
                                "TRAN_TYPE": "S",
                                "ACTUAL_AMOUNT": -daily_interest
                            } for date in date_range
                        ])
                    # Ensure the transaction itself is not duplicated
                    if transaction_type not in ['Market Purchase', 'Opening', 'Purchase', 'Redemption', 'Market Sale', 'Withdrawal', 'Sell', 'Apply In Ipo']:
                        interest_entries.append({
                            "ISIN": Scrip_Code,
                            "TRADE_DATE": transaction_date,
                            "TRANSACTION_TYPE": "Interest",
                            "TRAN_TYPE": "S",
                            "AMOUNT": amount,
                            "ACTUAL_AMOUNT": -amount if transaction_type in ['Redemption', 'Market Sale', 'Withdrawal', 'Sell'] else amount
                        })
                interest_df = pd.DataFrame(interest_entries, columns=["ISIN", "TRADE_DATE", "TRAN_TYPE", "TRANSACTION_TYPE", "AMOUNT", "ACTUAL_AMOUNT"])
                interest_df["RATE"] = interest_df["QUANTITY"] = 0
                interest_df['AMOUNT'] = interest_df['AMOUNT'].astype(float).apply(lambda x: round_half_up(x, 2))
                interest_df['ACTUAL_AMOUNT'] = interest_df['ACTUAL_AMOUNT'].astype(float).apply(lambda x: round_half_up(x, 2))
                final_debt_df = pd.concat([final_debt_df, interest_df], ignore_index=True)
                # final_debt_df = final_debt_df[final_debt_df["ACTUAL_AMOUNT"] != 0].copy()

            except Exception as e:
                app_logger.trace(f"DEBT CORPORATE ACTION | ERROR IN CALCULATION | {traceback.format_exc()}")
                # final_debt_df = pd.concat([final_debt_df, pd.DataFrame([{'ISIN': Scrip_Code, 'STATUS': 'FAILED', "QUANTITY"}])], ignore_index=True)
        final_debt_df = pd.merge(final_debt_df, advisor_df, on="ISIN", how="left")
        final_debt_df["CLIENT_CODE"] = client_id
        final_debt_df["SOURCE_ID"] = 4
        final_debt_df["IS_CORPORATE_ACTION"] = 1
        final_debt_df["CREATED_BY_ID"] = final_debt_df["UPDATED_BY_ID"] = user_id
        final_debt_df["CREATED_DATE"] = final_debt_df["UPDATED_DATE"] = datetime.now()
        final_debt_df["STATUS"] = "Success"
        # final_debt_df.to_csv("final_debt_df.csv", index=False)
        # data_updated, msg = await upload_bulk_file(final_debt_df, "PROSPECT_FT_TRANSACTION")
        if not final_debt_df.empty:
            return True, "Corporate Action Generated Successfully", final_debt_df
        else:
            return False, "Debt Corporate Action Failed", pd.DataFrame()
        # return {"message": "Debt corp calculation done..."}
    except Exception as e:
        return False, "Debt corp calculation failed...", pd.DataFrame()


