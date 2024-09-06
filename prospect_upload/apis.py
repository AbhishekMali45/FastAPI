from fastapi import FastAPI, APIRouter, HTTPException, Request, Response
from fastapi.params import Depends
from datetime import date
from fastapi.responses import FileResponse
from pyxirr import xirr
import tempfile
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime
from fastapi import Form
from pandas import Timestamp
# from utils.services import app_logger 
# from utils.config import get_settings
import numpy as np
import os, sys
import io
import math
from sqlalchemy import or_, and_,func
from authorization.services import check_if_permission
from db_module.connection import get_db
import asyncio
from models.models import (
    DmAccount,
    DmMutualFund,
    ProspectFamilyMaster,
    ProspectFtTransaction,
    ProspectFtHolding,
    ProspectCorporateActionStatus,
    FtMfClosingPrice,
    ProspectMFCorporateActionMaster,
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
    Insurance,
    TblFpInsurancePolicy,
    ProspectInsurancePolicyMaster,
    StageProspectFtMfTransaction,
    ProspectFtMfCorporateAction,
    ProspectInsuranceHolding,
    )
# settings = get_settings()
from schemas.schemas import CreateAccount,EmailValidation
from io import BytesIO
from fastapi import File, HTTPException, UploadFile
from prospect_client.add_prospect.services import get_value_type, validate_data
from fastapi.responses import StreamingResponse
import pandas as pd
from models.models import (SlideMaster)
from sqlalchemy import desc
from utils.services import app_logger
import traceback
from utils.config import get_settings
import boto3
from jose import jwt
from typing import Optional, Tuple
from botocore.exceptions import NoCredentialsError
from db_module.create import upload_bulk_file 
from db_module.read import fetch_pms_rolling_return_table, fetch_pms_underlying_table, fetch_pms_rolling_return_all,fetch_pms_underlying_all
from io import BytesIO
settings = get_settings()
from utils.basic_auth import JWTBearer, decodeJWT
from prospect_client.prospect_calculation.apis import calculation_for_equity_corp_action, debt_corp_cal_v2

prospect_upload_router = APIRouter(
    prefix="/prospect/upload_data", tags=["prospect_client"], responses={422: {"description": "Not Found"}}
)


@prospect_upload_router.post("/transactions/equity/", dependencies=[Depends(JWTBearer())])
async def upload_prospect_data_equity(header_request: Request, client_id: int = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        raise HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        raise HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        raise HTTPException(status_code=403, detail=msg)
    
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    
    # Read the file into memory
    file.file.seek(0)  # Reset file pointer to the beginning
    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")

    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        df.columns = df.columns.str.strip()
    except Exception as e:
        app_logger.error(f"PROSPECT Equity UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded File is Empty")

    df['FILE_NAME'] = file.filename
    df.columns = df.columns.str.strip()
    
    required_columns = ['Client Code', 'Advisor Name', 'Trade Date', 'ISIN', 'Trans Type', 'Quantity','Rate', 'Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        app_logger.error(f"PROSPECT EQUITY UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")

    df.rename(columns={
        'Client Code': 'CLIENT_CODE',
        'Advisor Name': 'ADVISOR_NAME',
        'Trade Date': 'TRADE_DATE',
        'ISIN': 'ISIN',
        'Trans Type':"TRANSACTION_TYPE",
        'Quantity': 'QUANTITY',
        'Rate': 'RATE',
        'Amount': 'AMOUNT',
    }, inplace=True)

    df['STATUS'] = 'Success'
    # Validating mandatory columns
    if df['CLIENT_CODE'].isnull().any():
        raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
    if not df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
        raise HTTPException(status_code=400, detail="Client Code should be an integer")
    
    if not df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
        raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
    
    if len(df['CLIENT_CODE'].unique()) > 1:
        raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

    if client_id not in df['CLIENT_CODE'].unique():
        raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")

    if df['ADVISOR_NAME'].isnull().any() or df['ADVISOR_NAME'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
    
    if df['ADVISOR_NAME'].str.len().max() > 100:
        raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")
    
    if df['TRADE_DATE'].isnull().any():
        raise HTTPException(status_code=400, detail="Trade Date should not be blank")
    
    try:
        df['TRADE_DATE'] = pd.to_datetime(df['TRADE_DATE'], errors='raise')
    except ValueError:
        raise HTTPException(status_code=400, detail="Trade Date contains invalid date values")
    
    if (df['TRADE_DATE'] > pd.Timestamp.now()).any():
        raise HTTPException(status_code=400, detail="Trade Date should not be greater than the current date")
    
    if df['ISIN'].isnull().any() or df['ISIN'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="ISIN should not be blank")
    
    if df['ISIN'].str.match(r'^INE[A-Z0-9]{9}$').any() == False:
        raise HTTPException(status_code=400, detail="ISIN should start with 'INE' followed by 9 alphanumeric characters")

    if df['RATE'].isnull().any() or df['RATE'].eq('').any() or (df['RATE'] == 0).any():
        raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
        
    if not df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Rate should be of type int or float")
    
    if df['AMOUNT'].isnull().any() or df['AMOUNT'].eq('').any() or (df["AMOUNT"] == 0).any():
        raise HTTPException(status_code=400, detail="Amount should not be blank or 0")
    
    if not df['AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Amount should be of type int or float")
    
    if df['QUANTITY'].isnull().any() or df['QUANTITY'].eq('').any() or (df['QUANTITY'] == 0).any():
        raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")
    
    if not df['QUANTITY'].apply(lambda x: isinstance(x, int)).all():
        raise HTTPException(status_code=400, detail="Quantity should be of type int")
    
    df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].str.strip()
    
    if df['TRANSACTION_TYPE'].isnull().any() or df['TRANSACTION_TYPE'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="Trans Type should not be blank")
    
    valid_transaction_types = ['corpus in', 'corpus out', 'ipo', 'purchase', 'sell']
    if not df['TRANSACTION_TYPE'].str.lower().isin(valid_transaction_types).all():
        raise HTTPException(status_code=400, detail="Invalid Trans Type found in the file")

    # Standardizing and preparing data
    df[['QUANTITY', 'RATE', 'AMOUNT']] = df[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
    df['ADVISOR_NAME'] = df['ADVISOR_NAME'].astype(str).replace(['', 'na', 'none', 'null', 'nan'], np.nan).fillna('Centrum')
    df['TRAN_TYPE'] = df['TRANSACTION_TYPE'].apply(lambda x: 'B' if x.strip().lower() in ['corpus in', 'ipo', 'purchase'] else 'S')
    df['ACTUAL_AMOUNT'] = df.apply(lambda row: row['AMOUNT'] if row['TRAN_TYPE'] == 'B' else -row['AMOUNT'], axis=1)

    # Get unique ISINs from the DataFrame
    df['ISIN'] = df['ISIN'].astype(str)
    df['ISIN'] = df['ISIN'].str.strip()
    
    unique_isins = df['ISIN'].str.strip().unique().tolist()
    # Fetching ISIN data from DMSecurityMaster and ProspectSecurityMaster
    security_isin = db.query(
        DMSecurityMaster.ISIN,
        DMSecurityMaster.SCRIP_KEY
        ).filter(
            # DMSecurityMaster.ASSET_CLASS == "Equity",
            func.trim(DMSecurityMaster.ISIN).in_(unique_isins),
            DMSecurityMaster.SOURCE_ID == 2
        ).all()
    # security_isin = db.query(DMSecurityMaster.ISIN, DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.ASSET_CLASS == "Equity").all()
    secmaster_df = pd.DataFrame(security_isin, columns=['ISIN', 'SCHEME_CODE']).drop_duplicates()
    sec_isin_df = pd.merge(df, secmaster_df, how='left', on='ISIN')
    secmaster_df = pd.DataFrame(security_isin, columns=['ISIN', 'SCHEME_CODE']).drop_duplicates()
    sec_isin_df = pd.merge(df, secmaster_df, how='left', on='ISIN')

    df1 = sec_isin_df.dropna(subset=['SCHEME_CODE']).copy()
    df1['STATUS'] = "Success"
    df1["SOURCE_TABLE"] = "DM_SECURITY_MASTER"

    df2 = sec_isin_df[sec_isin_df['SCHEME_CODE'].isnull()].copy()
    df2['REMARK'] = 'ISIN not found in Security Master Table'
    df2['STATUS'] = "Failure"

    prospect_isin = db.query(ProspectSecurityMaster.ISIN).filter(ProspectSecurityMaster.SOURCE_ID == 2).all()
    prospect_data = pd.DataFrame()  # Initialize prospect_data as an empty DataFrame
    if prospect_isin:
        prospect_isin_df = pd.DataFrame(prospect_isin, columns=['ISIN'])
        prospect_data = pd.merge(df2, prospect_isin_df, how='left', on='ISIN', indicator=True)
        prospect_data = prospect_data[prospect_data['_merge'] == 'both']
        prospect_data['STATUS'] = "Success"
        prospect_data['REMARK'] = ''
        prospect_data["SOURCE_TABLE"] = "PROSPECT_SECURITY_MASTER"

        prospect_data.drop('_merge', axis=1, inplace=True)
        df2 = df2[~df2['ISIN'].isin(prospect_data['ISIN'])]

    merged_df = pd.concat([df1, df2, prospect_data], ignore_index=True)
    
    start_date = df['TRADE_DATE'].min()
    end_date = df['TRADE_DATE'].max()
    existing_data_db = db.query(
        ProspectFtTransaction.CLIENT_CODE,                                 
        ProspectFtTransaction.ADVISOR_NAME, 
        ProspectFtTransaction.TRADE_DATE, 
        ProspectFtTransaction.ISIN, 
        ProspectFtTransaction.TRAN_TYPE, 
        ProspectFtTransaction.QUANTITY, 
        ProspectFtTransaction.RATE, 
        ProspectFtTransaction.AMOUNT, 
        ProspectFtTransaction.TRANSACTION_TYPE, 
        ProspectFtTransaction.SCHEME_CODE
            ).filter(
                ProspectFtTransaction.CLIENT_CODE == client_id, 
                ProspectFtTransaction.SOURCE_ID == 2,
                ProspectFtTransaction.TRADE_DATE >= start_date,
                ProspectFtTransaction.TRADE_DATE <= end_date
                ).all()
    
    if existing_data_db:
        existed_data = pd.DataFrame(existing_data_db, columns=['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'ISIN', 'TRAN_TYPE', 'QUANTITY', 'RATE', 'AMOUNT', 'TRANSACTION_TYPE', 'SCHEME_CODE'])
        existed_data[['CLIENT_CODE', 'SCHEME_CODE']] = existed_data[['CLIENT_CODE', 'SCHEME_CODE']].apply(pd.to_numeric, errors='coerce', downcast='integer')
        existed_data['TRADE_DATE'] = pd.to_datetime(existed_data['TRADE_DATE'], errors="coerce")
        existed_data[['QUANTITY', 'RATE', 'AMOUNT']] = existed_data[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
        existed_data[['ISIN', 'ADVISOR_NAME', 'TRAN_TYPE', 'TRANSACTION_TYPE']] = existed_data[['ISIN', 'ADVISOR_NAME', 'TRAN_TYPE', 'TRANSACTION_TYPE']].astype(str)
        comparison_columns = ['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'ISIN', 'TRAN_TYPE', 'QUANTITY', 'RATE', 'AMOUNT', 'TRANSACTION_TYPE', 'SCHEME_CODE']
        duplicates = pd.merge(merged_df, existed_data, on=comparison_columns, how='inner')
        merged_df.loc[merged_df.index.isin(duplicates.index), 'REMARK'] = 'Duplicate record found in the database'
        merged_df.loc[merged_df.index.isin(duplicates.index), 'STATUS'] = 'Duplicate'       


    zero_value_rows = (merged_df['QUANTITY'] == 0) & (merged_df['RATE'] == 0) & (merged_df['AMOUNT'] == 0)
    merged_df.loc[zero_value_rows, 'STATUS'] = "Failure"
    merged_df.loc[zero_value_rows, 'REMARK'] = 'All values are zero'
    merged_df['IS_DELETED'] = 0
    merged_df['CREATED_BY_ID'] = user_id
    merged_df['SOURCE_ID'] = 2
    merged_df['UPDATED_BY_ID'] = user_id
    merged_df['CREATED_DATE'] = datetime.now()
    merged_df['UPDATED_DATE'] = datetime.now()
    merged_df.fillna('', inplace=True)
    merged_df.fillna(np.nan, inplace = True)
    # merged_df.to_csv("equity_transaction.csv", index = False)
    data = merged_df.replace('', None).to_dict(orient='records')

    # Ensure the length of merged_df matches the original DataFrame
    # if len(merged_df) != len(df):
    #     raise HTTPException(status_code=500, detail="The processed data length does not match the original file length")
    # Bulk insert mappings into the database
    try:
        db.bulk_insert_mappings(ProspectFtTransaction, data)
        db.commit()
        await update_corporate_action_status(db, client_id, user_id)
        app_logger.info(f"PROSPECT EQUITY UPLOAD | File processed successfully | {len(data)}")
        return {"message": "File Uploaded Successfully", "data_length": len(data)}
    except Exception as e:
        app_logger.error(f"PROSPECT EQUITY UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
        raise HTTPException(status_code=400, detail="Error while saving the data")


@prospect_upload_router.post("/transactions/debt/", dependencies=[Depends(JWTBearer())])
async def upload_prospect_data_debt(header_request: Request, client_id: int = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        return HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        return HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)    
    
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    
    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")

    # Read the uploaded Excel file into a DataFrame
    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        df.columns = df.columns.str.strip()
    except Exception as e:
        app_logger.error(f"PROSPECT DEBT UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    if df.empty:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    df['FILE_NAME'] = file.filename
    df.columns = df.columns.str.strip()
    
    # Validating mandatory columns
    required_columns = ['Client Code', 'Advisor Name', 'Trade Date','ISIN', 'Trans Type', 'Quantity', 'Rate', 'Amount']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        app_logger.error(f"PROSPECT DEBT UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
        
    df.rename(columns={
        'Client Code': 'CLIENT_CODE',
        'Advisor Name': 'ADVISOR_NAME',
        'Trade Date': 'TRADE_DATE',
        'ISIN': 'ISIN',
        'Trans Type':"TRANSACTION_TYPE",
        'Quantity': 'QUANTITY',
        'Rate': 'RATE',
        'Amount': 'AMOUNT',
    }, inplace=True)

    if df['CLIENT_CODE'].isnull().any():
        raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
    if not df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
        raise HTTPException(status_code=400, detail="Client Code should be an integer")
    
    if not df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
        raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
    
    if len(df['CLIENT_CODE'].unique()) > 1:
        raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

    if client_id not in df['CLIENT_CODE'].unique():
        raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")
    
    if df['ADVISOR_NAME'].isnull().any() or df['ADVISOR_NAME'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
    
    if df['ADVISOR_NAME'].str.len().max() > 100:
        raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")
    
    if df['TRADE_DATE'].isnull().any():
        raise HTTPException(status_code=400, detail="Trade Date should not be blank")
    
    try:
        df['TRADE_DATE'] = pd.to_datetime(df['TRADE_DATE'], errors='raise')
    except ValueError:
        raise HTTPException(status_code=400, detail="Trade Date contains invalid date values")
    
    if (df['TRADE_DATE'] > pd.Timestamp.now()).any():
        raise HTTPException(status_code=400, detail="Trade Date should not be greater than the current date")
    
    if df['ISIN'].isnull().any() or df['ISIN'].eq('').any() or (df['ISIN'] == '0').any():
        raise HTTPException(status_code=400, detail="ISIN should not be blank or 0")

    if df['ISIN'].str.len().max() > 20:
        raise HTTPException(status_code=400, detail="ISIN should not be greater than 20 characters")
    
    if df['RATE'].isnull().any() or df['RATE'].eq('').any() or (df['RATE'] == 0).any():
        raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
        
    if not df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Rate should be of type int or float")
    
    if not df['AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Amount should be of type int or float")
    
    if df['AMOUNT'].isnull().any() or df['AMOUNT'].eq('').any() or (df['AMOUNT'] == 0).any():
        raise HTTPException(status_code=400, detail="Amount should not be blank or 0")
    
    if df['QUANTITY'].isnull().any() or df['QUANTITY'].eq('').any() or (df['QUANTITY'] == 0).any():
        raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")
    
    if not df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Quantity should be of type int or float")
    
    df['TRANSACTION_TYPE'] = df['TRANSACTION_TYPE'].str.strip()
    
    if df['TRANSACTION_TYPE'].isnull().any() or df['TRANSACTION_TYPE'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="Trans Type should not be blank")

    valid_transaction_types = ['apply in ipo', 'market purchase', 'market sale', 'opening', 'redemption', 'withdrawal']
    if not df['TRANSACTION_TYPE'].str.lower().isin(valid_transaction_types).all():
        raise HTTPException(status_code=400, detail="Invalid Trans Type found in the file")

    
    # Standardizing and preparing data
    df[['QUANTITY', 'RATE', 'AMOUNT']] = df[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
    df['TRADE_DATE'] = pd.to_datetime(df['TRADE_DATE'])
    df['ADVISOR_NAME'] = df['ADVISOR_NAME'].astype(str).replace(['', 'na', 'none', 'null', 'nan'], np.nan).fillna('Centrum')
    df['TRAN_TYPE'] = df['TRANSACTION_TYPE'].apply(lambda x: 'B' if x.strip().lower() in ['opening', 'apply in ipo', 'market purchase'] else 'S')
    df['ACTUAL_AMOUNT'] = df.apply(lambda row: row['AMOUNT'] if row['TRAN_TYPE'] == 'B' else -row['AMOUNT'], axis=1)

    # Get unique ISINs from the DataFrame
    df['ISIN'] = df['ISIN'].astype(str)
    df['ISIN'] = df['ISIN'].str.strip()
    unique_isins = df['ISIN'].unique().tolist()
    # Fetching ISIN data from DMSecurityMaster and ProspectSecurityMaster
    security_isin = db.query(
                            DMSecurityMaster.ISIN,
                            DMSecurityMaster.SCRIP_KEY
                        ).filter(
                            func.trim(DMSecurityMaster.ISIN).in_(unique_isins),
                            DMSecurityMaster.SOURCE_ID == 4
                            ).all()
    # security_isin = db.query(DMSecurityMaster.ISIN, DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.ASSET_CLASS == "Equity").all()
    secmaster_df = pd.DataFrame(security_isin, columns=['ISIN', 'SCHEME_CODE']).drop_duplicates()
    secmaster_df.drop_duplicates(subset=['ISIN'], inplace=True)
  
    secmaster_df['ISIN'] = secmaster_df['ISIN'].str.strip()
    sec_isin_df = pd.merge(df, secmaster_df, how='left', on='ISIN')

    df1 = sec_isin_df.dropna(subset=['SCHEME_CODE']).copy()
    df1['STATUS'] = "Success"
    df1["SOURCE_TABLE"] = "DM_SECURITY_MASTER"

    df2 = sec_isin_df[sec_isin_df['SCHEME_CODE'].isnull()].copy()
    df2['REMARK'] = 'ISIN not found in the Security Master Table'
    df2['STATUS'] = "Failure"

    prospect_isin = db.query(ProspectSecurityMaster.ISIN, ProspectSecurityMaster.SCHEME_CODE).filter(ProspectSecurityMaster.SOURCE_ID == 4).all()
    prospect_data = pd.DataFrame()  # Initialize prospect_data as an empty DataFrame
    if prospect_isin:
        prospect_isin_df = pd.DataFrame(prospect_isin, columns=['ISIN', 'SCHEME_CODE'])
        prospect_isin_df.drop_duplicates(subset=['ISIN'], inplace=True)
        prospect_data = pd.merge(df2, prospect_isin_df, how='left', on='ISIN', indicator=True)
        prospect_data = prospect_data[prospect_data['_merge'] == 'both']
        prospect_data['STATUS'] = "Success"
        prospect_data['REMARK'] = ''
        prospect_data["SOURCE_TABLE"] = "PROSPECT_SECURITY_MASTER"
        prospect_data.drop('_merge', axis=1, inplace=True)
        df2 = df2[~df2['ISIN'].isin(prospect_data['ISIN'])]

    merged_df = pd.concat([df1, df2, prospect_data], ignore_index=True)
    merged_df.rename(columns={'Client Code': 'CLIENT_CODE', 'Advisor Name': 'ADVISOR_NAME', 'Trade Date': 'TRADE_DATE', 'ISIN': 'ISIN', 'Trans Type': 'TRAN_TYPE', 'Quantity': 'QUANTITY', 'Rate': 'RATE', 'Amount': 'AMOUNT'}, inplace=True)

    existing_data_db = db.query(ProspectFtTransaction.CLIENT_CODE, ProspectFtTransaction.ADVISOR_NAME, ProspectFtTransaction.TRADE_DATE, ProspectFtTransaction.ISIN, ProspectFtTransaction.TRAN_TYPE, ProspectFtTransaction.QUANTITY, ProspectFtTransaction.RATE, ProspectFtTransaction.AMOUNT, ProspectFtTransaction.TRANSACTION_TYPE, ProspectFtTransaction.SCHEME_CODE).filter(ProspectFtTransaction.CLIENT_CODE == client_id, ProspectFtTransaction.SOURCE_ID == 4).all()
    if existing_data_db:
        existed_data = pd.DataFrame(existing_data_db, columns=['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'ISIN', 'TRAN_TYPE', 'QUANTITY', 'RATE', 'AMOUNT', 'TRANSACTION_TYPE', 'SCHEME_CODE'])
        existed_data[['CLIENT_CODE', 'SCHEME_CODE']] = existed_data[['CLIENT_CODE', 'SCHEME_CODE']].apply(pd.to_numeric, errors='coerce', downcast='integer')
        existed_data['TRADE_DATE'] = pd.to_datetime(existed_data['TRADE_DATE'], errors="coerce")
        existed_data[['QUANTITY', 'RATE', 'AMOUNT']] = existed_data[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
        existed_data[['ISIN', 'ADVISOR_NAME', 'TRAN_TYPE', 'TRANSACTION_TYPE']] = existed_data[['ISIN', 'ADVISOR_NAME', 'TRAN_TYPE', 'TRANSACTION_TYPE']].astype(str)
        comparison_columns = ['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'ISIN', 'TRAN_TYPE', 'QUANTITY', 'RATE', 'AMOUNT', 'TRANSACTION_TYPE', 'SCHEME_CODE']
        duplicates = pd.merge(merged_df, existed_data, on=comparison_columns, how='inner')
        merged_df.loc[merged_df.index.isin(duplicates.index), 'REMARK'] = 'Duplicate record found in the database'
        merged_df.loc[merged_df.index.isin(duplicates.index), 'STATUS'] = 'Duplicate'

    zero_value_rows = (merged_df['QUANTITY'] == 0) & (merged_df['RATE'] == 0) & (merged_df['AMOUNT'] == 0)
    merged_df.loc[zero_value_rows, 'STATUS'] = "Failure"
    merged_df.loc[zero_value_rows, 'REMARK'] = 'All values are zero'
    merged_df['IS_DELETED'] = 0
    merged_df['CREATED_BY_ID'] = user_id
    merged_df['SOURCE_ID'] = 4
    merged_df['UPDATED_BY_ID'] = user_id
    merged_df['CREATED_DATE'] = datetime.now()
    merged_df['UPDATED_DATE'] = datetime.now()
    merged_df.fillna('', inplace=True)
    merged_df.fillna(np.nan, inplace = True)
    # merged_df.to_csv("debt_transaction.csv", index = False)
    data = merged_df.replace('', None).to_dict(orient='records')

    # Ensure the length of merged_df matches the original DataFrame
    # if len(merged_df) != len(df):
    #     raise HTTPException(status_code=500, detail="The processed data length does not match the original file length")
    # Bulk insert mappings into the database
    try:
        db.bulk_insert_mappings(ProspectFtTransaction, data)
        db.commit()
        await update_corporate_action_status(db, client_id, user_id)
        app_logger.info(f"PROSPECT DEBT UPLOAD | File processed successfully | {len(data)}")
        return {"message": "File Uploaded Successfully", "data_length": len(data)}
    except Exception as e:
        app_logger.error(f"PROSPECT DEBT UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
        raise HTTPException(status_code=400, detail="Error while saving the data")
    

@prospect_upload_router.post("/transactions/other_products/", dependencies=[Depends(JWTBearer())])
async def upload_other_products_transactions(
    header_request: Request,
    client_id: int = Form(...), 
    file: UploadFile = File(...),  
    db = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        return HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        return HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)
    
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    
    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB.")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")

    file.file.seek(0)
    file_bytes = await file.read()  # Read the file into memory
    excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
    updated_record_len = 0
    if excel_df.empty:
        raise HTTPException(status_code=400, detail="Uploaded File is Empty")
    
    excel_df.columns = excel_df.columns.str.strip()
    required_columns = ['Client Code', 'Advisor Name', 'Trade Date', 'Scheme Code', 'Trans Type','Folio No.', 'Quantity','Rate', 'Amount']
    missing_columns = [col for col in required_columns if col not in excel_df.columns]
    if missing_columns:
        app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
    
    if not excel_df.empty:
        excel_df.rename(columns={'Client Code':'CLIENT_CODE', 
                    'Advisor Name':'ADVISOR_NAME',
                    'Trade Date':'TRADE_DATE',
                    'Scheme Code':'SCHEME_CODE',
                    'Trans Type':'TRANSACTION_TYPE',
                    'Folio No.':'FOLIO_NO',
                    'Quantity':'QUANTITY',
                    'Rate':'RATE',
                    'Amount':'AMOUNT'}, inplace=True)
        
        excel_df['STATUS'] = 'Success'
        excel_df["FILE_NAME"] = file.filename
        # excel_df['SCHEME_CODE'] = excel_df['SCHEME_CODE'].astype(int)
        if excel_df['CLIENT_CODE'].isnull().any():
            raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            raise HTTPException(status_code=400, detail="Client Code should be an integer")
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

        if client_id not in excel_df['CLIENT_CODE'].unique():
            raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")
        
        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
    
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")

        if excel_df['TRADE_DATE'].isnull().any():
            raise HTTPException(status_code=400, detail="Trade Date should not be blank")
    
        try:
            excel_df['TRADE_DATE'] = pd.to_datetime(excel_df['TRADE_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Trade Date contains invalid date values")
        
        if (excel_df['TRADE_DATE'] > pd.Timestamp.now()).any():
            raise HTTPException(status_code=400, detail="Trade Date should not be greater than the current date")
        
        excel_df['TRANSACTION_TYPE'] = excel_df['TRANSACTION_TYPE'].str.strip()
    
        if excel_df['TRANSACTION_TYPE'].isnull().any() or excel_df['TRANSACTION_TYPE'].str.strip().eq('').any():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | INVALID TRANSACTION TYPE | FOUND INVALID TRANSACTION TYPES")
            raise HTTPException(status_code=400, detail="Trans Type should not be blank")
        
        valid_transaction_types = ['purchase', 'redemption']
        if not excel_df['TRANSACTION_TYPE'].str.lower().isin(valid_transaction_types).all():
            raise HTTPException(status_code=400, detail="Invalid Trans Type found in the file")

        excel_df['TRAN_TYPE'] = excel_df['TRANSACTION_TYPE'].apply(lambda x: 'B' if x.strip().lower() in [ 'purchase'] else 'S')
        
        if excel_df['SCHEME_CODE'].isnull().any():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | MISSING SCHEME CODE | SCHEME CODE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Scheme Code should not be empty")
        
        if not excel_df['SCHEME_CODE'].apply(lambda x: isinstance(x, int)).all():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | ERROR IN SCHEME CODE | SCHEME CODE SHOULD BE INTEGER")
            raise HTTPException(status_code=400, detail="Scheme Code should be integer")
        
        if not excel_df['SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | INVALID SCHEME CODE | SCHEME CODE SHOULD NOT BE GREATER THAN 10 DIGIT NUMBER")
            raise HTTPException(status_code=400, detail="Scheme Code should not be greater than 10 digits")
        
        try:
            excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].astype(str)
            excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].str.strip()
        except Exception as e:
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | ERROR WHILE READING THE FILE | {e}")
            raise HTTPException(status_code=400, detail="Folio No. should be a string")

        if excel_df['FOLIO_NO'].isnull().any() or (excel_df['FOLIO_NO'].str.strip() == '').any():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | MISSING FOLIO NUMBER | FOLIO NUMBER COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Folio No. should not be blank")
        
        if not excel_df['FOLIO_NO'].apply(lambda x: isinstance(x, str) and len(x) <= 25).all():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | INVALID FOLIO NUMBER | FOLIO NUMBER SHOULD BE A STRING OF UP TO 20 CHARACTERS")
            raise HTTPException(status_code=400, detail="Folio No. should not be greater than 25 characters")

        if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
            raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
        
        if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Rate should be of type int or float")
        
        if not excel_df['AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Amount should be of type int or float")
        
        if excel_df['AMOUNT'].isnull().any() or excel_df['AMOUNT'].eq('').any() or (excel_df['AMOUNT'] == 0).any():
            raise HTTPException(status_code=400, detail="Amount should not be blank or 0")

        if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
            raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")
        
        if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Quantity should be of type int or float")
        
        if excel_df['RATE'].isnull().any():
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | INVALID RATE | RATE CANNOT BE NULL")
            raise HTTPException(status_code=400, detail="Rate cannot be null")

        scheme_codes_from_file = excel_df['SCHEME_CODE'].unique()
        scheme_codes_default_db = db.query(DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.SCRIP_KEY.in_(scheme_codes_from_file)).all()
        scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list
        scheme_codes_from_db = db.query(ProspectSecurityMaster.SCHEME_CODE).filter(ProspectSecurityMaster.SCHEME_CODE.in_(scheme_codes_from_file)).all()
        scheme_codes_from_db = pd.DataFrame(scheme_codes_from_db, columns=['SCHEME_CODE'])['SCHEME_CODE'].tolist()  # Flatten the list of tuples to list

        missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_from_db) - set(scheme_codes_default_db)
        if missing_scheme_codes:
            excel_df.loc[excel_df['SCHEME_CODE'].isin(missing_scheme_codes), 'STATUS'] = 'Failure'
            excel_df.loc[excel_df['SCHEME_CODE'].isin(missing_scheme_codes), 'REMARK'] = 'Scheme Code not found in Security Master Table'
        # setting source id as 5
        excel_df['SOURCE_ID'] = 5
        # if excel_df[['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'SCHEME_CODE', 'TRANSACTION_TYPE', 'FOLIO_NO', 'QUANTITY', 'RATE', 'AMOUNT']].isnull().values.any():
        #     raise HTTPException(status_code=400, detail="One or more required columns are null in the input file.")
        start_date = excel_df['TRADE_DATE'].min()
        end_date = excel_df['TRADE_DATE'].max()
        client_code = excel_df['CLIENT_CODE'].iloc[0]
        get_data_for_client = db.query(
            ProspectFtTransaction.CLIENT_CODE,
            ProspectFtTransaction.ADVISOR_NAME,
            ProspectFtTransaction.TRADE_DATE,
            ProspectFtTransaction.SCHEME_CODE,
            ProspectFtTransaction.TRANSACTION_TYPE,
            ProspectFtTransaction.FOLIO_NO,
            ProspectFtTransaction.QUANTITY,
            ProspectFtTransaction.RATE,
            ProspectFtTransaction.AMOUNT
            ).filter(ProspectFtTransaction.CLIENT_CODE == client_code,
                ProspectFtTransaction.TRADE_DATE >= start_date,
                ProspectFtTransaction.TRADE_DATE <= end_date
                ).all()
        existing_records = pd.DataFrame.from_records(get_data_for_client, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'TRADE_DATE', 'SCHEME_CODE', 'TRANSACTION_TYPE', 'FOLIO_NO', 'QUANTITY', 'RATE', 'AMOUNT'])
        excel_df[['QUANTITY', 'RATE', 'AMOUNT']] = excel_df[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
        excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].astype(str)
        excel_df['SCHEME_CODE'] = excel_df['SCHEME_CODE'].astype(int)
        if not existing_records.empty:
            existing_records['SCHEME_CODE'].fillna(0, inplace=True)
            # existing_records['SCHEME_CODE'] = existing_records['SCHEME_CODE'].astype(int)
            existing_records['CLIENT_CODE'] = existing_records['CLIENT_CODE'].astype(int)
            existing_records[['QUANTITY', 'RATE', 'AMOUNT']] = existing_records[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
            existing_records['FOLIO_NO'] = existing_records['FOLIO_NO'].astype(str)
            # Merge existing records with uploaded data to find duplicates
            merged_df = pd.merge(excel_df, existing_records, on=['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'SCHEME_CODE', 'TRANSACTION_TYPE', 
                        'FOLIO_NO', 'QUANTITY', 'RATE'], how='inner')
            if not merged_df.empty:
                # Mark duplicates in the uploaded data
                excel_df.loc[excel_df.index.isin(merged_df.index), 'STATUS'] = 'Duplicate'
                excel_df.loc[excel_df.index.isin(merged_df.index), 'REMARK'] = 'Duplicate record found in the database'
        excel_df["TRANSACTION_TYPE"] = excel_df["TRANSACTION_TYPE"].str.strip()
        excel_df["TRANSACTION_TYPE"] = excel_df["TRANSACTION_TYPE"].str.title()
        excel_df['TRAN_TYPE'] = excel_df['TRANSACTION_TYPE'].apply(lambda x:'B' if x == 'Purchase' else 'S')
        excel_df["ACTUAL_AMOUNT"] = np.where(excel_df['TRAN_TYPE'] == 'S', -excel_df["AMOUNT"], excel_df["AMOUNT"])
        excel_df["SOURCE_ID"] = 5
        excel_df["SOURCE_TABLE"] = "DM_MUTUALFUND"
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_TRANSACTION')
        if not response:
            raise HTTPException(status_code=400, detail="Error while uploading the file in database")
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            raise HTTPException(status_code=400, detail="Error while saving the data")
    
    app_logger.info("PROSPECT OTHER PRODUCTS UPLOAD | NO ERROR FOUND | UPLOADED SUCESSFULLY")
    return {"message":"File Uploaded Successfully"}



@prospect_upload_router.post("/transactions/insurance/ulip/", dependencies=[Depends(JWTBearer())])
async def upload_insurance_transaction_ulip(
    header_request: Request,
    client_id: int = Form(...), 
    file: UploadFile = File(...),  
    db = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        return HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        return HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)
    status_value = 200
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    
    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")
    
    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        excel_df.columns = excel_df.columns.str.strip()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    updated_record_len = 0
    input_data_length = len(excel_df)
    
    if excel_df.empty:
        raise HTTPException(status_code=400, detail="Uploaded File is Empty")
    
    if not excel_df.empty:
      
        # Validating mandatory columns
        required_columns = ['Client Code', 'Advisor Name', 'Payment Date','Policy Code','Policy Name','Trans Type', 'Quantity','Rate', 'Amount']
        missing_columns = [col for col in required_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT INSURANCE ULIP TRANSACTION UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
        
        excel_df.rename(columns={
                'Client Code':'CLIENT_CODE', 
                'Advisor Name':'ADVISOR_NAME',
                'Payment Date':"PAYMENT_DATE",
                'Policy Code':'POLICY_CODE',
                'Policy Name':'POLICY_NAME',
                'Trans Type':'TRANSACTION_TYPE',
                'Quantity':'QUANTITY',
                'Rate':'RATE',
                'Amount':'AMOUNT'}, inplace=True)
          
        excel_df['STATUS'] = 'Success'
        excel_df["FILE_NAME"] = file.filename
        if excel_df['CLIENT_CODE'].isnull().any():
            raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            raise HTTPException(status_code=400, detail="Client Code should be an integer")
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

        if client_id not in excel_df['CLIENT_CODE'].unique():
            raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")
        
        if excel_df['PAYMENT_DATE'].isnull().any():
            raise HTTPException(status_code=400, detail="Payment Date should not be blank")
        
        try:
            excel_df['PAYMENT_DATE'] = pd.to_datetime(excel_df['PAYMENT_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Payment Date contains invalid date values")
        
        if (excel_df['PAYMENT_DATE'] > pd.Timestamp.now()).any():
            raise HTTPException(status_code=400, detail="Payment Date should not be greater than the current date")
        
        if excel_df['POLICY_CODE'].isnull().any() or excel_df['POLICY_CODE'].eq('').any():
            raise HTTPException(status_code=400, detail="Policy Code should not be blank")
        
        try:
            excel_df['POLICY_CODE'] = excel_df['POLICY_CODE'].astype(str)
            excel_df['POLICY_CODE'] = excel_df['POLICY_CODE'].str.strip()
        except Exception as e:
            app_logger.error(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE READING THE FILE | {e}")
            raise HTTPException(status_code=400, detail="Policy Code should be a string")
            
        if not excel_df['POLICY_CODE'].apply(lambda x: isinstance(x, str) and len(x) <= 20).all():
            raise HTTPException(status_code=400, detail="Policy Code should not be greater than 20 characters")
        
        if excel_df['POLICY_NAME'].isnull().any() or excel_df['POLICY_NAME'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="Policy Name should not be blank")
        
        if not excel_df['POLICY_NAME'].apply(lambda x: isinstance(x, str) and len(x) <= 200).all():
            raise HTTPException(status_code=400, detail="Policy Name should not be greater than 200 characters")
        
        # Clean and validate the TRANSACTION_TYPE column
        excel_df['TRANSACTION_TYPE'] = excel_df['TRANSACTION_TYPE'].str.strip().str.title()
        valid_transaction_types = ['Purchase', 'Sell']
        if not excel_df['TRANSACTION_TYPE'].isin(valid_transaction_types).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP TRANSACTION UPLOAD | INVALID TRANSACTION TYPE | FOUND INVALID TRANSACTION TYPES")
            raise HTTPException(status_code=400, detail=f"Invalid Trans Type found in the file")

        excel_df['TRAN_TYPE'] = np.where(excel_df['TRANSACTION_TYPE'] == 'Purchase', 'B', 'S')
        
        if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
            raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
        
        if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP TRANSACTION UPLOAD | INVALID DATA TYPES | 'Rate' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Rate should be of type int or float")
        
        if excel_df['AMOUNT'].isnull().any() or excel_df['AMOUNT'].eq('').any() or (excel_df['AMOUNT'] == 0).any():
            raise HTTPException(status_code=400, detail="Amount should not be blank or 0")
        
        if not excel_df['AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Amount should be of type int or float")
        
        if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
            raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")
        
        if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Quantity should be of type int or float")
        
        # Check if the scheme codes in the uploaded file are present in the database
        policy_code_from_file = excel_df['POLICY_CODE'].unique()
        policy_code_default_db = db.query(TblFpInsurancePolicy.POLICY_CODE).filter(TblFpInsurancePolicy.POLICY_CODE.in_(policy_code_from_file)).all()
        policy_code_default_db = pd.DataFrame(policy_code_default_db, columns=['POLICY_CODE'])['POLICY_CODE'].tolist()  # Flatten the list of tuples to list
        policy_code_from_db = db.query(ProspectInsurancePolicyMaster.POLICY_CODE).filter(ProspectInsurancePolicyMaster.POLICY_CODE.in_(policy_code_from_file)).all()
        policy_code_from_db = pd.DataFrame(policy_code_from_db, columns=['POLICY_CODE'])['POLICY_CODE'].tolist()  # Flatten the list of tuples to list

        missing_policy_number = set(policy_code_from_file) - set(policy_code_from_db) - set(policy_code_default_db)
        if missing_policy_number:
            excel_df.loc[excel_df['POLICY_CODE'].isin(missing_policy_number), 'STATUS'] = 'Failure'
            excel_df.loc[excel_df['POLICY_CODE'].isin(missing_policy_number), 'REMARK'] = 'Policy Code not found in Policy Master Table'
        
        excel_df['SOURCE_ID'] = 7
        start_date = excel_df['PAYMENT_DATE'].min()
        end_date = excel_df['PAYMENT_DATE'].max()
        
        excel_df.rename(columns={'PAYMENT_DATE': 'TRADE_DATE'}, inplace=True)
        excel_df.drop(columns=['POLICY_NAME'], inplace=True)
        
        get_data_for_client = db.query(ProspectFtTransaction.CLIENT_CODE,
                                       ProspectFtTransaction.ADVISOR_NAME,
                                       ProspectFtTransaction.TRADE_DATE,
                                       ProspectFtTransaction.POLICY_CODE,
                                       ProspectFtTransaction.TRANSACTION_TYPE,
                                       ProspectFtTransaction.QUANTITY,
                                       ProspectFtTransaction.RATE,
                                       ProspectFtTransaction.AMOUNT
                                       ).filter(ProspectFtTransaction.CLIENT_CODE == client_id,
                    ProspectFtTransaction.TRADE_DATE >= start_date,
                    ProspectFtTransaction.TRADE_DATE <= end_date).all()
        existing_records = pd.DataFrame.from_records(get_data_for_client, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'TRADE_DATE', 'POLICY_CODE', 'TRANSACTION_TYPE', 'QUANTITY', 'RATE', 'AMOUNT'])
        excel_df[['QUANTITY', 'RATE', 'AMOUNT']] = excel_df[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
        if not existing_records.empty:
            existing_records['CLIENT_CODE'] = existing_records['CLIENT_CODE'].astype(int)
            existing_records[['QUANTITY', 'RATE', 'AMOUNT']] = existing_records[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
            # Merge existing records with uploaded data to find duplicates
            merged_df = pd.merge(excel_df, existing_records, on=['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'POLICY_CODE', 'TRANSACTION_TYPE', 
                        'QUANTITY', 'RATE'], how='inner')
            if not merged_df.empty:
                # Mark duplicates in the uploaded data
                excel_df.loc[excel_df.index.isin(merged_df.index), 'STATUS'] = 'Duplicate'
                excel_df.loc[excel_df.index.isin(merged_df.index), 'REMARK'] = 'Duplicate record found in the database'
        excel_df["TRANSACTION_TYPE"] = excel_df["TRANSACTION_TYPE"].str.strip()
        excel_df["TRANSACTION_TYPE"] = excel_df["TRANSACTION_TYPE"].str.title()
        excel_df['TRAN_TYPE'] = excel_df['TRANSACTION_TYPE'].apply(lambda x:'B' if x == 'Purchase' else 'S')
        excel_df["ACTUAL_AMOUNT"] = np.where(excel_df['TRAN_TYPE'] == 'S', -excel_df["AMOUNT"], excel_df["AMOUNT"])
        excel_df["SOURCE_ID"] = 7
        excel_df["SOURCE_TABLE"] = "TBLFP_INSURANCE_POLICY"
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_TRANSACTION')
        if not response:
            raise HTTPException(status_code=400, detail="Error while uploading the file in database")
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT INSURANCE ULIP UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            raise HTTPException(status_code=400, detail="Error while saving the data")
        

    app_logger.info("PROSPECT INSURANCE ULIP TRANSACTION UPLOAD | NO ERROR FOUND | UPLOADED SUCESSFULLY")
    return {"message":"File Uploaded Successfully"}


async def get_mf_scrip_code(db: Session):
    
    query = db.query(DmMutualFund.SCHEME_CODE).distinct()
    result = query.all()
    data = pd.DataFrame(result, columns=['SCHEME_CODE'])
    data = data["SCHEME_CODE"].tolist()
    if data:
        return data
    else:
        return []
    

@prospect_upload_router.post("/transactions/mutual_funds/", dependencies=[Depends(JWTBearer())])
async def upload_mf_transaction_api(
    header_request: Request,
    client_id: int = Form(...),
    mf_type: Optional[str] = None,
    file: UploadFile = File(...),  
    db = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        return HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        return HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    
    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")

    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        excel_df.columns = excel_df.columns.str.strip()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    if not excel_df.empty:
        excel_df["FILE_NAME"] = file.filename
        if not mf_type:
            excel_df, message = await upload_mf_transaction(db, excel_df, user_id, client_id)
        elif mf_type.lower() == "sip":
            excel_df, message = await upload_mf_transaction_sip(db, client_id, excel_df, user_id)
        elif mf_type.lower() == "stp":
            excel_df, message = await upload_mf_transaction_stp(db, client_id, excel_df, user_id)
        elif mf_type.lower() == "switch":
            excel_df, message = await upload_mf_transaction_switch(db, client_id, excel_df, user_id)   
        elif mf_type.lower() == "swp":
            excel_df, message = await upload_mf_transaction_swp(db, client_id, excel_df, user_id)
        else:
            app_logger.error(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE PROCESSING THE FILE | WRONG MF TYPE PASSED")
            raise HTTPException(status_code=400, detail="Error while reading the file")
    if excel_df.empty:
        app_logger.trace(f"PROSPECT MUTUAL FUNDS UPLOAD | UPLOAD FAILED | {message}")
        raise HTTPException(status_code=400, detail=f"{message}")
    else:
        app_logger.info("PROSPECT MUTUAL FUNDS UPLOAD | NO ERROR FOUND | UPLOADED SUCESSFULLY")
        return {"message":"File Uploaded Successfully"}


async def upload_mf_transaction(db: Session, excel_df: pd.DataFrame,
                user_id: str, client_id: int):
    try:
        scrip_codes = await get_mf_scrip_code(db)
        excel_df.columns = excel_df.columns.str.strip()
        mandatory_columns = ['Client Code', 'Advisor Name', 'Trade Date','Scheme Name', 'Scheme Code','RTA Code', 'Folio No.', 
                             'Trans Type', 'Quantity', 'Rate', 'Amount', 'Reinvest']
        missing_columns = [col for col in mandatory_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT MF TRANSACTION UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            return pd.DataFrame(), f"Missing columns: {', '.join(missing_columns)}"
        
        excel_df.rename(columns={'Client Code':'CLIENT_CODE', 
                    'Advisor Name':'ADVISOR_NAME',
                    'Trade Date':'TRADE_DATE',
                    'Scheme Code':'SCHEME_CODE',
                    'Scheme Name':'SCHEME_NAME',
                    'RTA Code':'RTA_CODE',
                    'Trans Type':'TRANSACTION_TYPE',
                    'Folio No.':'FOLIO_NO',
                    'Quantity':'QUANTITY',
                    'Rate':'RATE',
                    'Amount':'AMOUNT',
                    "Reinvest":"RE_INVEST"}, inplace=True)
        excel_df['STATUS'] = 'Success'
        excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].astype(str)
        if excel_df['CLIENT_CODE'].isnull().any():
            return  pd.DataFrame(), "Client Code should not be blank"
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            return  pd.DataFrame(), "Client Code should be an integer"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            return  pd.DataFrame(), "Client Code should not be greater than 20 digits"
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            return  pd.DataFrame(), "Multiple Client Code found in the uploaded file"

        if client_id not in excel_df['CLIENT_CODE'].unique():
            return pd.DataFrame(), "Client Code does not match with the selected Client"

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            return pd.DataFrame(), "Advisor Name should not be blank"
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            return pd.DataFrame(),"Advisor Name should not be greater than 100 characters"
        
        if excel_df['TRADE_DATE'].isnull().any():
            return pd.DataFrame(),"Trade Date should not be blank"
        
        try:
            excel_df['TRADE_DATE'] = pd.to_datetime(excel_df['TRADE_DATE'], errors='raise')
        except ValueError:
            return pd.DataFrame(),"Trade Date contains invalid date values"
        
        if (excel_df['TRADE_DATE'] > pd.Timestamp.now()).any():
            return pd.DataFrame(),"Trade Date should not be greater than the current date"
        
        
        excel_df['TRANSACTION_TYPE'] = excel_df['TRANSACTION_TYPE'].str.strip().str.title()
    
        if excel_df['TRANSACTION_TYPE'].isnull().any() or excel_df['TRANSACTION_TYPE'].str.strip().eq('').any():
            return pd.DataFrame(),"Trans Type should not be blank"
        
         
        valid_transaction_types = ['Corpus In', 'Corpus Out', 'Nfo', 'Purchase', 'Redemption', 'Segregated Units', 'Transmission In', 'Transmission Out']
        if not excel_df['TRANSACTION_TYPE'].str.title().isin(valid_transaction_types).all():
            return pd.DataFrame(), "Invalid Trans Type found in the file"

        transaction_type_mapping = {
            'Corpus In': 'B',
            'Corpus Out': 'S',
            'Nfo': 'B',
            'Purchase': 'B',
            'Redemption': 'S',
            'Segregated Units': 'B',
            'Transmission In': 'B',
            'Transmission Out': 'S'
        }

        # Apply the mapping to the TRAN_TYPE column
        excel_df['TRAN_TYPE'] = excel_df['TRANSACTION_TYPE'].map(transaction_type_mapping)
        # Step 2: Check for duplicates
        if excel_df['SCHEME_CODE'].isnull().any() or excel_df['SCHEME_CODE'].eq('').any():
            app_logger.error(f"PROSPECT PMS UPLOAD | MISSING SCHEME CODE | SCHEME_CODE COLUMN IS EMPTY")
            return pd.DataFrame(),"Scheme Code should not be blank"
        
        # excel_df["SCHEME_CODE"] = excel_df["SCHEME_CODE"].astype(int)
        if not excel_df['SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT PMS UPLOAD | ERROR IN SCHEME CODE | SCHEME CODE SHOULD BE NUMERIC AND UP TO 10 DIGITS")
            return pd.DataFrame(),"Scheme Code should be numeric and should not be greater than 10 digits"
        
        if excel_df['SCHEME_NAME'].isnull().any() or excel_df['SCHEME_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Scheme Name should not be blank"
        
        if not excel_df['SCHEME_NAME'].apply(lambda x: isinstance(x, str) and len(x) <= 200).all():
            return pd.DataFrame(),"Scheme Name should not be greater than 200 characters"

        if excel_df['FOLIO_NO'].isnull().any() or excel_df['FOLIO_NO'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING FOLIO NUMBER | FOLIO NUMBER COLUMN IS EMPTY")
            return pd.DataFrame(), "Folio No. should not be blank"
        
        excel_df["FOLIO_NO"] = excel_df["FOLIO_NO"].astype(str)
        if not excel_df['FOLIO_NO'].apply(lambda x: isinstance(x, str) and len(x) <= 25).all():
            return pd.DataFrame(),"Folio No. should not be greater than 25 characters"
        
        if excel_df['RTA_CODE'].isnull().any() or excel_df['RTA_CODE'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING RTA CODE| RTA CODE COLUMN IS EMPTY")
            return pd.DataFrame(), "RTA Code should not be blank"
        
        if not excel_df['RTA_CODE'].apply(lambda x: isinstance(x, str) and len(x) <= 200).all():
            return pd.DataFrame(), "RTA Code should not be greater than 20 characters"
            
        if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
            return pd.DataFrame(), "Rate should not be blank or 0"
        
        if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP TRANSACTION UPLOAD | INVALID DATA TYPES | 'Rate' contains non-numeric values")
            return pd.DataFrame(),"Rate should be of type int or float"
        
        if excel_df['AMOUNT'].isnull().any() or excel_df['AMOUNT'].eq('').any() or (excel_df['AMOUNT'] == 0).any():
            return pd.DataFrame(),"Amount should not be blank or 0"
        
        if not excel_df['AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            return pd.DataFrame(), "Amount should be of type int or float"
        
        if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
            return pd.DataFrame(), "Quantity should not be blank or 0"
        
        if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
            return pd.DataFrame(), "Quantity should be of type int or float"
        
        if excel_df['RE_INVEST'].isnull().any() or excel_df['RE_INVEST'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING RE_INVEST | RE_INVEST COLUMN IS EMPTY")
            return pd.DataFrame(), "Reinvest should not be blank"
        
        excel_df["RE_INVEST"] = excel_df["RE_INVEST"].str.strip().str.upper()
        if not excel_df['RE_INVEST'].apply(lambda x: x in ['P', 'R']).all():
            app_logger.error(f"PROSPECT MF TRANSACTION UPLOAD | INVALID RE_INVEST VALUES | RE_INVEST must be a single character, either 'P' or 'R'")
            return pd.DataFrame(), "Reinvest should be a single character, either 'P' or 'R'."

        
        scheme_codes_from_file = excel_df['SCHEME_CODE'].unique()
        scheme_codes_default_db = db.query(
            DMSecurityMaster.SCRIP_KEY
            ).filter(
                func.trim(DMSecurityMaster.SCRIP_KEY).in_(scheme_codes_from_file),
                DMSecurityMaster.SOURCE_ID == 3
                ).all()
        scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list

        missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_default_db)
        if missing_scheme_codes:
            app_logger.error(f"PROSPECT MF TRANSACTION UPLOAD | MISSING SCHEME CODES | SCHEME CODES NOT FOUND IN DATABASE")
            return pd.DataFrame(), "Some Scheme Codes not found in Security Master Table"

        excel_df["SOURCE_ID"] = 3
        excel_df.drop(columns=["SCHEME_NAME"], inplace=True)
        # if excel_df[['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'SCHEME_CODE', 'TRANSACTION_TYPE', 'FOLIO_NO', 'QUANTITY', 'RATE', 'AMOUNT']].isnull().values.any():
        #     raise HTTPException(status_code=400, detail="One or more required columns are null in the input file.")
        start_date = excel_df['TRADE_DATE'].min()
        end_date = excel_df['TRADE_DATE'].max()
        get_data_for_client = db.query(ProspectFtTransaction.CLIENT_CODE,
            ProspectFtTransaction.ADVISOR_NAME,
            ProspectFtTransaction.TRADE_DATE,
            ProspectFtTransaction.SCHEME_CODE,
            ProspectFtTransaction.RTA_CODE,
            ProspectFtTransaction.TRANSACTION_TYPE,
            ProspectFtTransaction.FOLIO_NO,
            ProspectFtTransaction.QUANTITY,
            ProspectFtTransaction.RATE,
            ProspectFtTransaction.AMOUNT
            ).filter(ProspectFtTransaction.CLIENT_CODE == client_id,
                ProspectFtTransaction.TRADE_DATE >= start_date,
                ProspectFtTransaction.TRADE_DATE <= end_date).all()
        existing_records = pd.DataFrame.from_records(get_data_for_client, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'TRADE_DATE', 'SCHEME_CODE', 'RTA_CODE', 'TRANSACTION_TYPE', 'FOLIO_NO', 'QUANTITY', 'RATE', 'AMOUNT'])
        excel_df[['QUANTITY', 'RATE', 'AMOUNT']] = excel_df[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
        excel_df['RTA_CODE'] = excel_df['RTA_CODE'].astype(str)
        excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].astype(str)
        if not existing_records.empty:
            existing_records['CLIENT_CODE'] = existing_records['CLIENT_CODE'].astype(int)
            existing_records[['QUANTITY', 'RATE', 'AMOUNT']] = existing_records[['QUANTITY', 'RATE', 'AMOUNT']].astype(float).round(8)
            existing_records['RTA_CODE'] = existing_records['RTA_CODE'].astype(str)
            existing_records['FOLIO_NO'] = existing_records['FOLIO_NO'].astype(str)
            # Merge existing records with uploaded data to find duplicates
            merged_df = pd.merge(excel_df, existing_records, on=['CLIENT_CODE', 'ADVISOR_NAME', 'TRADE_DATE', 'SCHEME_CODE', 
                        "RTA_CODE", "TRANSACTION_TYPE", 
                        "FOLIO_NO", "QUANTITY", "RATE"], how='inner')
            if not merged_df.empty:
                # Mark duplicates in the uploaded data
                excel_df.loc[excel_df.index.isin(merged_df.index), 'STATUS'] = 'Duplicate'
        excel_df['TRANSACTION_TYPE'] = excel_df['TRANSACTION_TYPE'].str.lower().str.strip()
        excel_df['TRANSACTION_TYPE'] = excel_df['TRANSACTION_TYPE'].str.title().str.strip()
        excel_df["ACTUAL_AMOUNT"] = np.where(excel_df['TRAN_TYPE'] == 'S', -excel_df["AMOUNT"], excel_df["AMOUNT"])
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["SOURCE_TABLE"] = "DM_MUTUALFUND"
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None, 'nan': None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_TRANSACTION')
        if not response:
            app_logger.error(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            return pd.DataFrame(), "Error while saving the data"
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT MUTUAL FUND UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            return pd.DataFrame(), "Error while saving the data"
        
        return excel_df, "File Uploaded Successfully"
    except Exception as e:
        app_logger.trace(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE UPLOADING THE DATA | {traceback.format_exc()}")
        return pd.DataFrame(), "Error while saving the data"


def generate_transaction_dates_stp(row):
    frequency_map = {
        'daily': 'D',
        'weekly': 'W',
        'monthly': 'M',
        'quarterly': '3M',
        'yearly': '12M'
    }
    freq = frequency_map.get(row['FREQUENCY'].lower(), 'D')  # Default to daily if not found
    dates = pd.date_range(start=row['STP_START_DATE'], periods=row['NO_OF_INSTALLMENT'], freq=freq)
    dates = dates.to_series().apply(lambda d: d if d.weekday() < 5 else (d + pd.DateOffset(days=(7 - d.weekday()))))  # 5 and 6 correspond to Saturday and Sunday
    today = pd.Timestamp.now().normalize()
    dates = dates[dates <= today]
    dates = dates[:row['NO_OF_INSTALLMENT']]
    return dates.to_list()


def generate_swp_dates(row):
    frequency_map = {
        'daily': 'D',
        'weekly': 'W',
        'monthly': 'M',
        'quarterly': '3M',
        'yearly': '12M'
    }
    freq = frequency_map.get(row['FREQUENCY'].lower(), 'D')  # Default to daily if not found
    start_date = pd.to_datetime(row['SWP_START_DATE'])
    end_date = start_date + pd.DateOffset(months=12)  # Assuming a maximum of one year duration for simplicity

    # Generate date range from start to end date at the specified frequency
    dates = pd.date_range(start=start_date, periods=row['NO_OF_INSTALLMENT'], freq=freq)
    dates = dates.to_series().apply(lambda d: d.replace(day=start_date.day) if d.day != start_date.day else d)
    # Postpone dates that fall on weekends to the next working day
    dates = dates.apply(lambda d: d if d.weekday() < 5 else (d + pd.DateOffset(days=(7 - d.weekday()))))  # 5 and 6 correspond to Saturday and Sunday
    # Filter out weekends if necessary (optional)
    # dates = dates.to_series().apply(lambda d: d if d.weekday() < 5 else (d + pd.DateOffset(days=(7 - d.weekday()))))
    today = pd.Timestamp.now().normalize()
    dates = dates[dates <= today]
    dates = dates[:row['NO_OF_INSTALLMENT']]
    return dates.tolist()


def generate_sip_dates(row):
    start_date = pd.to_datetime(row['SIP_START_DATE'])
    end_date = datetime.now()  # Current date
    frequency = row['FREQUENCY'].lower()
    frequency = frequency.replace(' ', '')
    num_installments = int(row['NO_OF_INSTALLMENT'])
    
    if 'daily' in frequency:
        freq = 'D'
    elif 'weekly' in frequency:
        freq = 'W'
    elif 'fortnightly' in frequency:
        freq = '2W'
    elif 'monthly' in frequency:
        freq = 'M'
    elif 'quarterly' in frequency:
        freq = '3M'
    elif 'yearly' in frequency:
        freq = '12M'
    else:
        return pd.Series()  # Return empty if frequency is not recognized

    # Generate date range from start to current date at the specified frequency
    dates = pd.date_range(start=start_date, periods=num_installments, freq=freq)
    dates = dates.to_series().apply(lambda d: d.replace(day=start_date.day) if d.day != start_date.day else d)

    # Filter out weekends
    dates = dates.apply(lambda d: d if d.weekday() < 5 else (d + pd.DateOffset(days=(7 - d.weekday()))))  # 5 and 6 correspond to Saturday and Sunday
    # print(dates)
    # Filter out dates that are in the future
    today = pd.Timestamp.now().normalize()
    dates = dates[dates <= today]

    # Ensure we don't exceed the number of installments
    dates = dates[:num_installments]
    return dates.to_list()


async def upload_mf_transaction_sip(db: Session, client_id: int, excel_df: pd.DataFrame, user_id: str):
    if "FILE_NAME" in excel_df.columns.to_list():
        excel_df.drop(columns=["FILE_NAME"], inplace=True)
    try:

        excel_df.columns = excel_df.columns.str.strip()
        mandatory_columns = ['Client Code', 'Advisor Name', 'Scheme Code', 'Scheme Name', 'Folio No.', 
                             'SIP Start Date', 'SIP Amount', 'No. of Installment', 'Frequency','Reinvest']
        missing_columns = [col for col in mandatory_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT EQUITY UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            return pd.DataFrame(),f"Missing columns: {', '.join(missing_columns)}"

        excel_df.rename(columns={'Client Code':'CLIENT_CODE', 
                    'Advisor Name':'ADVISOR_NAME',
                'Scheme Code':'SCHEME_CODE',
                'Scheme Name':'SCHEME_NAME',
                'Folio No.':'FOLIO_NO',
                'SIP Start Date':'SIP_START_DATE',
                'SIP Amount':'SIP_AMOUNT',
                'No. of Installment':'NO_OF_INSTALLMENT',
                'Frequency':'FREQUENCY',
                "Reinvest":"RE_INVEST"}, inplace=True)
        
        excel_df.columns = excel_df.columns.str.strip()
        input_data_length = len(excel_df)
        if excel_df.empty:
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | INVALID CLIENT CODE | NO RECORDS FOUND IN UPLOADED FILE")
            return pd.DataFrame(), "No records found in uploaded file"
        
        excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].astype(str)
        if excel_df['CLIENT_CODE'].isnull().any():
            return pd.DataFrame(),"Client Code should not be blank"
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            return pd.DataFrame(), "Client Code should be an integer"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            return pd.DataFrame(), "Client Code should not be greater than 20 digits"
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            return pd.DataFrame(), "Multiple Client Code found in the uploaded file"

        if client_id not in excel_df['CLIENT_CODE'].unique():
            return pd.DataFrame(),"Client Code does not match with the selected Client"

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Advisor Name should not be blank"
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            return pd.DataFrame(),"Advisor Name should not be greater than 100 characters"

        if excel_df['SCHEME_CODE'].isnull().any() or excel_df['SCHEME_CODE'].eq('').any():
            app_logger.error(f"PROSPECT MF SIP TRANSACTION UPLOAD | MISSING SCHEME CODE | SCHEME_CODE COLUMN IS EMPTY")
            return pd.DataFrame(),"Scheme Code should not be blank"
        
        if not excel_df['SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT MF SIP TRANSACTION UPLOAD | ERROR IN SCHEME CODE | SCHEME CODE SHOULD BE NUMERIC AND UP TO 10 DIGITS")
            return pd.DataFrame(),"Scheme code should be numeric and should not be greater than 10 digits"
        
        if excel_df['SCHEME_NAME'].isnull().any() or excel_df['SCHEME_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Scheme Name should not be blank"
        
        if not excel_df['SCHEME_NAME'].apply(lambda x: isinstance(x, str) and len(x) <= 200).all():
            return pd.DataFrame(),"Scheme Name should not be greater than 200 characters"

        if excel_df['FOLIO_NO'].isnull().any() or excel_df['FOLIO_NO'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING FOLIO NUMBER | FOLIO NUMBER COLUMN IS EMPTY")
            return pd.DataFrame(), "Folio No. should not be blank"
        
        if not excel_df['FOLIO_NO'].apply(lambda x: isinstance(x, str) and len(x) <= 25).all():
            return pd.DataFrame(),"Folio No. should not be greater than 25 characters"
        
        if excel_df['SIP_START_DATE'].isnull().any():
            return pd.DataFrame(),"SIP Start Date should not be blank"
        
        if excel_df['SIP_AMOUNT'].isnull().any() or excel_df['SIP_AMOUNT'].eq('').any() or (excel_df['SIP_AMOUNT'] == 0).any():
            return pd.DataFrame(), "SIP Amount should not be blank"

        try:
            excel_df['SIP_START_DATE'] = pd.to_datetime(excel_df['SIP_START_DATE'], errors='raise')
        except ValueError:
            return pd.DataFrame(),"SIP Start Date contains invalid date values"
        
        if (excel_df['SIP_START_DATE'] > pd.Timestamp.now()).any():
            return pd.DataFrame(),"SIP Start Date should not be greater than the current date"

        if excel_df['SIP_AMOUNT'].isnull().any() or excel_df['SIP_AMOUNT'].eq('').any():
            return pd.DataFrame(),"SIP Amount should not be blank"
        


        if not excel_df['SIP_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            return pd.DataFrame(),"SIP Amount should be of type int or float"
        
        if excel_df['NO_OF_INSTALLMENT'].isnull().any() or excel_df['NO_OF_INSTALLMENT'].eq('').any():
            return pd.DataFrame(),"No Of Installment should not be blank"
        
        if not excel_df['NO_OF_INSTALLMENT'].apply(lambda x: isinstance(x, (int))).all().all():
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | INVALID DATA TYPES | 'NO_OF_INSTALLMENT' contains non-numeric values")
            return pd.DataFrame(),"No Of Installment should be of type int"

        if excel_df['FREQUENCY'].isnull().any() or excel_df['FREQUENCY'].eq('').any():
            return pd.DataFrame(),"Frequency should not be blank"
        
        excel_df['FREQUENCY'] = excel_df['FREQUENCY'].str.strip().str.title()
        valid_frequencies = ['Daily', 'Weekly','Monthly','Quarterly','Fortnightly','Yearly']
        if not excel_df['FREQUENCY'].isin(valid_frequencies).all():
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | INVALID FREQUENCY TYPE | FOUND INVALID FREQUENCY TYPES")
            return pd.DataFrame(),"Invalid Frequency types found in the file"
        
        if excel_df['RE_INVEST'].isnull().any() or excel_df['RE_INVEST'].eq('').any():
            return pd.DataFrame(),"Reinvest should not be blank"

        excel_df["RE_INVEST"] = excel_df["RE_INVEST"].str.strip().str.upper()
        if not excel_df['RE_INVEST'].apply(lambda x: x in ['P', 'R']).all():
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | INVALID RE_INVEST VALUES | RE_INVEST must be a single character, either 'P' or 'R'")
            return pd.DataFrame(),"Reinvest should be a single character, either 'P' or 'R'."
        
        excel_df['SIP_START_DATE'] = pd.to_datetime(excel_df['SIP_START_DATE']).dt.date
     
        excel_df['SIP_AMOUNT'] = excel_df['SIP_AMOUNT'].astype(float).round(8)
        
        scheme_codes_from_file = excel_df['SCHEME_CODE'].unique()
        scheme_codes_default_db = db.query(DMSecurityMaster.SCRIP_KEY).filter(func.trim(DMSecurityMaster.SCRIP_KEY).in_(scheme_codes_from_file), DMSecurityMaster.SOURCE_ID == 3).all()
        scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list  # Flatten the list of tuples to list
        
        missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_default_db)
        if missing_scheme_codes:
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | MISSING SCHEME CODES | SCHEME CODES NOT FOUND IN DATABASE")
            return pd.DataFrame(), f"Some Scheme Codes not found in Security Master Table"
        
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_MF_SIP_TRANSACTION_MASTER')
        if not response:
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            return pd.DataFrame(), "Error while saving the data"
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT MUTUAL FUND SIP UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            return pd.DataFrame(), "Error while saving the data"
        
        return excel_df, "File Uploaded Successfully"
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | ERROR WHILE UPLOADING THE FILE | {e}")
        return pd.DataFrame(), "Error while uploading the file"

async def upload_mf_transaction_stp(db: Session, client_id:int, excel_df: pd.DataFrame, user_id: str):
    excel_df.columns = excel_df.columns.str.strip()
    if "FILE_NAME" in excel_df.columns.to_list():
        excel_df.drop(columns=["FILE_NAME"], inplace=True)
    try:
        excel_df.columns = excel_df.columns.str.strip()
        mandatory_columns = ['Client Code', 'Advisor Name', 'From Scheme Code', 'From Scheme Name', 'From Folio No.','To Folio No.',
                            'To Scheme Code','To Scheme Name','STP Start Date', 'STP Amount', 'No. of Installment', 'Frequency','Reinvest']
        missing_columns = [col for col in mandatory_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT EQUITY UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            return pd.DataFrame(),f"Missing columns: {', '.join(missing_columns)}"
        excel_df.rename(columns={
                'Client Code':'CLIENT_CODE',
                'Advisor Name':'ADVISOR_NAME',
                'From Scheme Code':'FROM_SCHEME_CODE',
                'From Scheme Name':'FROM_SCHEME_NAME',
                'From Folio No.':'FROM_FOLIO_NO',
                'To Scheme Code':'TO_SCHEME_CODE',
                'To Scheme Name':'TO_SCHEME_NAME',
                'To Folio No.':'TO_FOLIO_NO',
                'STP Start Date':'STP_START_DATE',
                'STP Amount':'STP_AMOUNT',
                'Frequency':'FREQUENCY',
                "No. of Installment":'NO_OF_INSTALLMENT',
                "Reinvest":"RE_INVEST"}, inplace=True)
    
        excel_df['FROM_FOLIO_NO'] = excel_df['FROM_FOLIO_NO'].astype(str)
        excel_df['TO_FOLIO_NO'] = excel_df['TO_FOLIO_NO'].astype(str)
        if excel_df['CLIENT_CODE'].isnull().any():
            return pd.DataFrame(),"Client Code should not be blank"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            return pd.DataFrame(),"Client Code should be an integer"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            return pd.DataFrame(),"Client Code should not be greater than 20 digits"
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            return pd.DataFrame(),"Multiple Client Code found in the uploaded file"

        if client_id not in excel_df['CLIENT_CODE'].unique():
            return pd.DataFrame(),"Client Code does not match with the selected Client"

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Advisor Name should not be blank"
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            return pd.DataFrame(),"Advisor Name should not be greater than 100 characters"
        
        excel_df['STP_START_DATE'] = pd.to_datetime(excel_df['STP_START_DATE'])
        
        if excel_df['FROM_SCHEME_CODE'].isnull().any() or excel_df['FROM_SCHEME_CODE'].eq('').any():
            return pd.DataFrame(),"From Scheme Code should not be blank"
        
        if not excel_df['FROM_SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO SCHEME CODE | TO SCHEME CODE SHOULD BE INTEGER AND UP TO 10 DIGITS")
            return pd.DataFrame(),"From Scheme Code should be an integer and should not be greater than 10 digits"
        
        if excel_df['TO_SCHEME_CODE'].isnull().any() or excel_df['TO_SCHEME_CODE'].eq('').any():
            return pd.DataFrame(),"To Scheme Code should not be blank."
        
        if not excel_df['TO_SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO SCHEME CODE | TO SCHEME CODE SHOULD BE INTEGER AND UP TO 10 DIGITS")
            return pd.DataFrame(),"To Scheme Code should be an integer and should not be greater than 10 digits"

        if not excel_df["TO_SCHEME_CODE"].apply(lambda x: isinstance(x, int)).any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | ERROR IN TO SCHEME CODE | TO SCHEME CODE SHOULD BE INTEGER")
            return pd.DataFrame(), "To Scheme Code should be integer"
        
        if excel_df["FROM_SCHEME_NAME"].isnull().any() or excel_df['FROM_SCHEME_NAME'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING FROM SCHEME NAME | FROM SCHEME NAME COLUMN IS EMPTY")
            return pd.DataFrame(), "From Scheme Name should not be blank"
        
        if excel_df['FROM_SCHEME_NAME'].str.len().max() > 200:
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID FROM SCHEME NAME LENGTH | FROM SCHEME NAME SHOULD NOT BE GREATER THAN 200 CHARACTERS")
            return pd.DataFrame(),"From Scheme Name should not be greater than 200 characters"
        
        if excel_df["TO_SCHEME_NAME"].isnull().any() or excel_df['TO_SCHEME_NAME'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING TO SCHEME NAME | TO SCHEME NAME COLUMN IS EMPTY")
            return pd.DataFrame(),"To Scheme Name should not be blank"
        
        if excel_df['TO_SCHEME_NAME'].str.len().max() > 200:
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO SCHEME NAME LENGTH | TO SCHEME NAME SHOULD NOT BE GREATER THAN 200 CHARACTERS")
            return pd.DataFrame(),"To Scheme Name should not be greater than 200 characters"
        
        if excel_df['FROM_FOLIO_NO'].isnull().any() or excel_df['FROM_FOLIO_NO'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING FROM FOLIO NUMBER | FROM FOLIO NUMBER COLUMN IS EMPTY")
            return pd.DataFrame(),"From Folio No. should not be blank"
        
        if excel_df['FROM_FOLIO_NO'].str.len().max() > 25:
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID FROM FOLIO NO LENGTH | FROM FOLIO NO SHOULD NOT BE GREATER THAN 25 CHARACTERS")
            return pd.DataFrame(),"From Folio No. should not be greater than 25 characters"
        
        if excel_df['TO_FOLIO_NO'].isnull().any() or excel_df['TO_FOLIO_NO'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING TO FOLIO NUMBER | TO FOLIO NUMBER COLUMN IS EMPTY")
            return pd.DataFrame(),"To Folio No. should not be blank"
        
        if excel_df['TO_FOLIO_NO'].str.len().max() > 25:
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO FOLIO NO LENGTH | TO FOLIO NO SHOULD NOT BE GREATER THAN 25 CHARACTERS")
            return pd.DataFrame(),"To Folio No. should not be greater than 25 characters"
        
        if excel_df['STP_START_DATE'].isnull().any():
            return pd.DataFrame(),"STP Start Date should not be blank"
        
        try:
            excel_df['STP_START_DATE'] = pd.to_datetime(excel_df['STP_START_DATE'], errors='raise')
        except ValueError:
            return pd.DataFrame(),"STP Start Date contains invalid date values"
        
        if (excel_df['STP_START_DATE'] > pd.Timestamp.now()).any():
            return pd.DataFrame(),"STP Start Date should not be greater than the current date"

        if excel_df['STP_AMOUNT'].isnull().any() or excel_df['STP_AMOUNT'].eq('').any() or (excel_df['STP_AMOUNT'] == 0).any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING STP AMOUNT | STP AMOUNT COLUMN IS EMPTY")
            return pd.DataFrame(),"STP Amount should not be blank"
        
        if not excel_df['STP_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID DATA TYPES | 'STP Amount' contains non-numeric values")
            return pd.DataFrame()," 'STP Amount' should be of type int or float"
        
        if excel_df['NO_OF_INSTALLMENT'].isnull().any() or excel_df['NO_OF_INSTALLMENT'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING STP AMOUNT | STP AMOUNT COLUMN IS EMPTY")
            return pd.DataFrame(),"No Of Installment should not be blank"
        
        if not excel_df['NO_OF_INSTALLMENT'].apply(lambda x: isinstance(x, (int))).all().all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID DATA TYPES | 'NO_OF_INSTALLMENT' contains non-numeric values")
            return pd.DataFrame(),"No Of Installment should be of type int"
        
        if excel_df['FREQUENCY'].isnull().any() or excel_df['FREQUENCY'].eq('').any():
            return pd.DataFrame(),"Frequency should not be blank"
        
        excel_df['FREQUENCY'] = excel_df['FREQUENCY'].str.strip().str.title()
        valid_frequencies = ['Daily', 'Weekly','Monthly','Quarterly','Fortnightly','Yearly']
        if not excel_df['FREQUENCY'].isin(valid_frequencies).all():
            app_logger.error(f"PROSPECT MUTUAL FUNDS SIP UPLOAD | INVALID FREQUENCY TYPE | FOUND INVALID FREQUENCY TYPES")
            return pd.DataFrame(),f"Invalid Frequency types found in the file"
        
        if excel_df['RE_INVEST'].isnull().any() or excel_df['RE_INVEST'].eq('').any():
            return pd.DataFrame(),"Reinvest should not be blank"
        
        excel_df["RE_INVEST"] = excel_df["RE_INVEST"].str.strip().str.upper()
        if not excel_df['RE_INVEST'].apply(lambda x: x in ['P', 'R']).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID RE_INVEST VALUES | Reinvest must be a single character, either 'P' or 'R'")
            return pd.DataFrame(),"Reinvest should be a single character, either 'P' or 'R'."
        
        scheme_codes_from_file = excel_df['TO_SCHEME_CODE'].drop_duplicates().to_list()
        scheme_codes_from_file_2 = excel_df['FROM_SCHEME_CODE'].drop_duplicates().to_list()
        scheme_codes_from_file = scheme_codes_from_file + scheme_codes_from_file_2
        scheme_codes_default_db = db.query(DMSecurityMaster.SCRIP_KEY).filter(func.trim(DMSecurityMaster.SCRIP_KEY).in_(scheme_codes_from_file), DMSecurityMaster.SOURCE_ID == 3).all()
        scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list  # Flatten the list of tuples to list

        missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_default_db)
        if missing_scheme_codes:
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING SCHEME CODES | SCHEME CODES NOT FOUND IN DATABASE")
            return pd.DataFrame(),"Some Scheme Codes not found in Security Master Table"

        excel_df['STP_START_DATE'] = pd.to_datetime(excel_df['STP_START_DATE'])  # Ensure datetime format
        excel_df['STP_AMOUNT'] = excel_df['STP_AMOUNT'].astype(float).round(8)
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_MF_STP_TRANSACTION_MASTER')
        if not response:
            app_logger.error(f"PROSPECT MF STP UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            return pd.DataFrame(),"Error while saving the data"
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT MF STP UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            return pd.DataFrame(),"Error while saving the data"
        
        return excel_df, "File Uploaded Successfully"
    except Exception as e:
        app_logger.error(f"PROSPECT MF STP UPLOAD | ERROR WHILE UPLOADING THE FILE | {e}")
        return pd.DataFrame(), "Error while uploading the file"


async def upload_mf_transaction_switch(db: Session, client_id:int, excel_df: pd.DataFrame, user_id: str):
    excel_df.columns = excel_df.columns.str.strip()
    if "FILE_NAME" in excel_df.columns.to_list():
        excel_df.drop(columns=["FILE_NAME"], inplace=True)
    try:
        excel_df.columns = excel_df.columns.str.strip()
        mandatory_columns = ['Client Code', 'Advisor Name', 'From Scheme Code', 'From Scheme Name',
                            'To Scheme Code','To Scheme Name','Date of Switch', 'Amount', 'Reinvest']
        missing_columns = [col for col in mandatory_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT EQUITY UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            return pd.DataFrame(),f"Missing columns: {', '.join(missing_columns)}"
        excel_df.rename(columns={'Client Code':'CLIENT_CODE', 
                'Advisor Name':'ADVISOR_NAME',
                'From Scheme Code':'FROM_SCHEME_CODE',
                'From Scheme Name':'FROM_SCHEME_NAME',
                'To Scheme Code':'TO_SCHEME_CODE',
                'To Scheme Name':'TO_SCHEME_NAME',
                'Date of Switch':'DATE_OF_SWITCH',
                'Amount':'AMOUNT',
                "Reinvest":"RE_INVEST"}, inplace=True)
        
        if excel_df['CLIENT_CODE'].isnull().any():
            return pd.DataFrame(),"Client Code should not be blank"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            return pd.DataFrame(),"Client Code should be an integer"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            return pd.DataFrame(),"Client Code should not be greater than 20 digits"
    
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            return pd.DataFrame(),"Multiple Client Code found in the uploaded file"

        if client_id not in excel_df['CLIENT_CODE'].unique():
            return pd.DataFrame(),"Client Code does not match with the selected Client"

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Advisor Name should not be blank"
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            return pd.DataFrame(),"Advisor Name should not be greater than 100 characters"

        if excel_df['FROM_SCHEME_CODE'].isnull().any() or excel_df['FROM_SCHEME_CODE'].eq('').any():
            return pd.DataFrame(),"From Scheme Code should not be blank"
        
        if not excel_df['FROM_SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO SCHEME CODE | TO SCHEME CODE SHOULD BE INTEGER AND UP TO 10 DIGITS")
            return pd.DataFrame(),"From Scheme Code should be an integer and should not be greater than 10 digits."
        
        if excel_df['TO_SCHEME_CODE'].isnull().any() or excel_df['TO_SCHEME_CODE'].eq('').any():
            return pd.DataFrame(),"To Scheme Code should not be blank"
        
        if not excel_df['TO_SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO SCHEME CODE | TO SCHEME CODE SHOULD BE INTEGER AND UP TO 10 DIGITS")
            return pd.DataFrame(),"To Scheme Code should be an integer and should not be greater than 10 digits"

        if not excel_df["TO_SCHEME_CODE"].apply(lambda x: isinstance(x, int)).any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | ERROR IN TO SCHEME CODE | TO SCHEME CODE SHOULD BE INTEGER")
            return pd.DataFrame(), "To Scheme Code should be integer"
        
        if excel_df["FROM_SCHEME_NAME"].isnull().any() or excel_df['FROM_SCHEME_NAME'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING FROM SCHEME NAME | FROM SCHEME NAME COLUMN IS EMPTY")
            return pd.DataFrame(), "From Scheme Name should not be blank"
        
        if excel_df['FROM_SCHEME_NAME'].str.len().max() > 200:
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID FROM SCHEME NAME LENGTH | FROM SCHEME NAME SHOULD NOT BE GREATER THAN 200 CHARACTERS")
            return pd.DataFrame(),"From Scheme Name should not be greater than 200 characters"
        
        if excel_df["TO_SCHEME_NAME"].isnull().any() or excel_df['TO_SCHEME_NAME'].eq('').any():
            app_logger.error(f"PROSPECT MF STP UPLOAD | MISSING TO SCHEME NAME | TO SCHEME NAME COLUMN IS EMPTY")
            return pd.DataFrame(),"To Scheme Name should not be blank"
        
        if excel_df['TO_SCHEME_NAME'].str.len().max() > 200:
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID TO SCHEME NAME LENGTH | TO SCHEME NAME SHOULD NOT BE GREATER THAN 200 CHARACTERS")
            return pd.DataFrame(),"To Scheme Name should not be greater than 200 characters"
        
        if excel_df['AMOUNT'].isnull().any() or excel_df['AMOUNT'].eq('').any() or (excel_df['AMOUNT'] == 0).any():
            return pd.DataFrame(),"Amount should not be blank"
        
        if not excel_df['AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            return pd.DataFrame(),"Amount should be of type int or float"
        
        if excel_df['DATE_OF_SWITCH'].isnull().any():
            return pd.DataFrame(),"Date of Switch should not be blank"
        
        try:
            excel_df['DATE_OF_SWITCH'] = pd.to_datetime(excel_df['DATE_OF_SWITCH'], errors='raise')
        except ValueError:
            return pd.DataFrame(),"Date of Switch contains invalid date values"
        
        if (excel_df['DATE_OF_SWITCH'] > pd.Timestamp.now()).any():
            return pd.DataFrame(),"Date of Switch should not be greater than the current date"
        
        if excel_df['RE_INVEST'].isnull().any() or excel_df['RE_INVEST'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING RE_INVEST | RE_INVEST COLUMN IS EMPTY")
            return pd.DataFrame(),"Reinvest should not be blank"
        
        excel_df["RE_INVEST"] = excel_df["RE_INVEST"].str.strip().str.upper()
        if not excel_df['RE_INVEST'].apply(lambda x: x in ['P', 'R']).all():
            app_logger.error(f"PROSPECT MF STP UPLOAD | INVALID RE_INVEST VALUES | RE_INVEST must be a single character, either 'P' or 'R'")
            return pd.DataFrame(),"Reinvest should be a single character, either 'P' or 'R'."

        scheme_codes_from_file = excel_df['TO_SCHEME_CODE'].drop_duplicates().to_list()
        scheme_codes_from_file_2 = excel_df['FROM_SCHEME_CODE'].drop_duplicates().to_list()
        scheme_codes_from_file = scheme_codes_from_file + scheme_codes_from_file_2
        scheme_codes_default_db = db.query(DMSecurityMaster.SCRIP_KEY).filter(func.trim(DMSecurityMaster.SCRIP_KEY).in_(scheme_codes_from_file), DMSecurityMaster.SOURCE_ID == 3).all()
        scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list  # Flatten the list of tuples to list

        missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_default_db)
        if missing_scheme_codes:
            app_logger.error(f"PROSPECT MF SWITCH UPLOAD | MISSING SCHEME CODES | SCHEME CODES NOT FOUND IN DATABASE")
            return pd.DataFrame(),"Some Scheme Codes not found in Security Master Table"

        excel_df['AMOUNT'] = excel_df['AMOUNT'].astype(float).round(8)
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_MF_SWITCH_TRANSACTION_MASTER')
        if not response:
            app_logger.error(f"PROSPECT MUTUAL FUNDS SWITCH UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            return pd.DataFrame(),"Error while saving the data"
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT MF SWITCH UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            return pd.DataFrame(),"Error while saving the data"
        
        return excel_df, "File Uploaded Successfully"
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SWITCH UPLOAD | ERROR WHILE UPLOADING THE FILE | {e}")
        return pd.DataFrame(), "Error while uploading the file"


async def upload_mf_transaction_swp(db: Session, client_id:int, excel_df: pd.DataFrame, user_id: str):
    excel_df.columns = excel_df.columns.str.strip()
    if "FILE_NAME" in excel_df.columns.to_list():
        excel_df.drop(columns=["FILE_NAME"], inplace=True)
    try:
        excel_df.columns = excel_df.columns.str.strip()

        mandatory_columns = ['Client Code', 'Advisor Name', 'Scheme Name', 'Scheme Code','Folio No.', 
                             'SWP Start Date', 'SWP Amount', 'Frequency', 'No. of Installment', 'Reinvest']
        missing_columns = [col for col in mandatory_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT MF TRANSACTION UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            return pd.DataFrame(),f"Missing columns: {', '.join(missing_columns)}"
        excel_df.rename(columns={
            'Client Code':'CLIENT_CODE', 
            'Advisor Name':'ADVISOR_NAME',
            'Scheme Name':'SCHEME_NAME',
            'Scheme Code':'SCHEME_CODE',
            'Folio No.':'FOLIO_NO',
            'SWP Start Date':'SWP_START_DATE',
            'SWP Amount':'SWP_AMOUNT',
            'Frequency':'FREQUENCY',
            'No. of Installment':'NO_OF_INSTALLMENT',
            "Reinvest":"RE_INVEST"}, inplace=True)

        excel_df['FOLIO_NO'] = excel_df['FOLIO_NO'].astype(str)
        if excel_df['CLIENT_CODE'].isnull().any():
            return pd.DataFrame(),"Client Code should not be blank"
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            return pd.DataFrame(),"Client Code should be an integer"
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            return pd.DataFrame(),"Client Code should not be greater than 20 digits"
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            return pd.DataFrame(),"Multiple Client Code found in the uploaded file"

        if client_id not in excel_df['CLIENT_CODE'].unique():
            return pd.DataFrame(),"Client Code does not match with the selected Client"

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Advisor Name should not be blank"
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            return pd.DataFrame(),"Advisor Name should not be greater than 100 characters"

        if excel_df['SCHEME_CODE'].isnull().any() or excel_df['SCHEME_CODE'].eq('').any():
            app_logger.error(f"PROSPECT PMS UPLOAD | MISSING SCHEME CODE | SCHEME_CODE COLUMN IS EMPTY")
            return pd.DataFrame(),"Scheme Code should not be blank"
        
        if not excel_df['SCHEME_CODE'].apply(lambda x: isinstance(x, int) and len(str(x)) <= 10).all():
            app_logger.error(f"PROSPECT PMS UPLOAD | ERROR IN SCHEME CODE | SCHEME CODE SHOULD BE NUMERIC AND UP TO 10 DIGITS")
            return pd.DataFrame(),"Scheme code should be numeric and should not be greater than 10 digits"
        
        if excel_df['SCHEME_NAME'].isnull().any() or excel_df['SCHEME_NAME'].str.strip().eq('').any():
            return pd.DataFrame(),"Scheme Name should not be blank"
        
        if not excel_df['SCHEME_NAME'].apply(lambda x: isinstance(x, str) and len(x) <= 200).all():
            return pd.DataFrame(),"Scheme Name should not be greater than 200 characters"

        if excel_df['FOLIO_NO'].isnull().any() or excel_df['FOLIO_NO'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING FOLIO NUMBER | FOLIO NUMBER COLUMN IS EMPTY")
            return pd.DataFrame(), "Folio No. should not be blank"
        
        if not excel_df['FOLIO_NO'].apply(lambda x: isinstance(x, str) and len(x) <= 25).all():
            return pd.DataFrame(),"Folio No. should not be greater than 25 characters"
        
        if excel_df['SWP_START_DATE'].isnull().any():
            return pd.DataFrame(),"SWP Start Date should not be blank"
        
        try:
            excel_df['SWP_START_DATE'] = pd.to_datetime(excel_df['SWP_START_DATE'], errors='raise')
        except ValueError:
            return pd.DataFrame(),"SWP Start Date contains invalid date values"
        
        if (excel_df['SWP_START_DATE'] > pd.Timestamp.now()).any():
            return pd.DataFrame(),"SWP Start Date should not be greater than the current date"
        
        if excel_df['SWP_AMOUNT'].isnull().any() or excel_df['SWP_AMOUNT'].eq('').any() or (excel_df['SWP_AMOUNT'] == 0).any():
            return pd.DataFrame(),"SWP Amount should not be blank"
        
        if not excel_df['SWP_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            return pd.DataFrame(),"SWP Amount should be of type int or float"

        if excel_df['NO_OF_INSTALLMENT'].isnull().any() or excel_df['NO_OF_INSTALLMENT'].eq('').any():
            return pd.DataFrame(),"No Of Installment should not be blank"
        
        if not excel_df['NO_OF_INSTALLMENT'].apply(lambda x: isinstance(x, (int))).all().all():
            app_logger.error(f"PROSPECT MUTUAL FUNDS SWP UPLOAD | INVALID DATA TYPES | 'NO_OF_INSTALLMENT' contains non-numeric values")
            return pd.DataFrame(),"No Of Installment should be of type int"
        
        if excel_df['FREQUENCY'].isnull().any() or excel_df['FREQUENCY'].eq('').any():
            return pd.DataFrame(),"Frequency should not be blank"
        
        excel_df['FREQUENCY'] = excel_df['FREQUENCY'].str.strip().str.title()
        valid_frequencies = ['Daily', 'Weekly','Monthly','Quarterly','Fortnightly','Yearly']
        if not excel_df['FREQUENCY'].isin(valid_frequencies).all():
            app_logger.error(f"PROSPECT MUTUAL FUNDS SWP UPLOAD | INVALID FREQUENCY TYPE | FOUND INVALID FREQUENCY TYPES")
            return pd.DataFrame(),f"Invalid Frequency types found in the file"
        
        if excel_df['RE_INVEST'].isnull().any() or excel_df['RE_INVEST'].eq('').any():
            app_logger.error(f"PROSPECT MF TRANSACTIONS UPLOAD | MISSING RE_INVEST | RE_INVEST COLUMN IS EMPTY")
            return pd.DataFrame(),"Reinvest should not be blank"
        
        excel_df["RE_INVEST"] = excel_df["RE_INVEST"].str.strip().str.upper()
        if not excel_df['RE_INVEST'].apply(lambda x: x in ['P', 'R']).all():
            app_logger.error(f"PROSPECT MF SWP UPLOAD | INVALID RE_INVEST VALUES | RE_INVEST must be a single character, either 'P' or 'R'")
            return pd.DataFrame(),"Reinvest should be a single character, either 'P' or 'R'."

        scheme_codes_from_file = excel_df['SCHEME_CODE'].unique()
        scheme_codes_default_db = db.query(DMSecurityMaster.SCRIP_KEY).filter(func.trim(DMSecurityMaster.SCRIP_KEY).in_(scheme_codes_from_file), DMSecurityMaster.SOURCE_ID == 3).all()
        scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list  # Flatten the list of tuples to list
        missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_default_db)
        if missing_scheme_codes:
            app_logger.error(f"PROSPECT MF SWP UPLOAD | MISSING SCHEME CODES | SCHEME CODES NOT FOUND IN DATABASE")
            return pd.DataFrame(), f"Some Scheme Codes not found in Security Master Table"

        
        excel_df['SWP_AMOUNT'] = excel_df['SWP_AMOUNT'].astype(float).round(8)
        excel_df["CREATED_BY_ID"] = excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_FT_MF_SWP_TRANSACTION_MASTER')
        if not response:
            app_logger.error(f"PROSPECT MUTUAL FUNDS SWP UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            return pd.DataFrame(), "Error while saving the data"
        try:
            await update_corporate_action_status(db, client_id, user_id)
        except:
            app_logger.error(f"PROSPECT MF SWP UPLOAD | ERROR WHILE SAVING THE DATA | {e}")
            return pd.DataFrame(),"Error while saving the data"
        
        return excel_df, "File Uploaded Successfully"
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SWP UPLOAD | ERROR WHILE UPLOADING THE FILE | {e}")
        return pd.DataFrame(), "Error while uploading the file"


async def get_mf_transaction_data(db: Session, client_code):
    try:
        getsip_data = db.query(ProspectFtTransaction.CLIENT_CODE,
            ProspectFtTransaction.ADVISOR_NAME,
            ProspectFtTransaction.SCHEME_CODE,
            ProspectFtTransaction.FOLIO_NO,
            ProspectFtTransaction.TRADE_DATE,
            ProspectFtTransaction.TRANSACTION_TYPE,
            ProspectFtTransaction.AMOUNT,
            ProspectFtTransaction.TRAN_TYPE,
            ProspectFtTransaction.ACTUAL_AMOUNT,
            ProspectFtTransaction.STATUS,
            ProspectFtTransaction.RATE,
            ProspectFtTransaction.QUANTITY,
            ProspectFtTransaction.RE_INVEST
            ).filter(ProspectFtTransaction.CLIENT_CODE == client_code, ProspectFtTransaction.SOURCE_ID==3).all()
        if getsip_data:
            transaction_df = pd.DataFrame.from_records(getsip_data, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'SCHEME_CODE', 'FOLIO_NO', 'TRADE_DATE', 'TRANSACTION_TYPE', 'AMOUNT', 'TRAN_TYPE', 'ACTUAL_AMOUNT', 'STATUS',
                'RATE', 'QUANTITY', 'RE_INVEST'])
            return transaction_df
        return pd.DataFrame()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS DATA | ERROR WHILE FETCHING THE DATA | {e}")
        return pd.DataFrame()


async def get_mf_sip_data(db: Session, client_code, user_id: str):
    try:
        getsip_data = db.query(ProspectFtMfSipTransactionMaster.CLIENT_CODE,
            ProspectFtMfSipTransactionMaster.ADVISOR_NAME,
            ProspectFtMfSipTransactionMaster.SCHEME_CODE,
            ProspectFtMfSipTransactionMaster.SCHEME_NAME,
            ProspectFtMfSipTransactionMaster.FOLIO_NO,
            ProspectFtMfSipTransactionMaster.SIP_START_DATE,
            ProspectFtMfSipTransactionMaster.SIP_AMOUNT,
            ProspectFtMfSipTransactionMaster.NO_OF_INSTALLMENT,
            ProspectFtMfSipTransactionMaster.FREQUENCY,
            ProspectFtMfSipTransactionMaster.RE_INVEST
            ).filter(ProspectFtMfSipTransactionMaster.CLIENT_CODE == client_code).all()
        
        if getsip_data:
            
            sip_df = pd.DataFrame.from_records(getsip_data, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'SCHEME_CODE', 'SCHEME_NAME', 'FOLIO_NO', 'SIP_START_DATE', 'SIP_AMOUNT', 'NO_OF_INSTALLMENT', 'FREQUENCY', 'RE_INVEST'])
            
            # Find the earliest start date and all distinct scheme codes
            earliest_start_date = sip_df['SIP_START_DATE'].min()
            distinct_scheme_codes = sip_df['SCHEME_CODE'].drop_duplicates().to_list()
            # Fetch NAV prices for the distinct scheme codes from the earliest start date to the current date
            nav_data = db.query(
                FtMfClosingPrice.SCHEME_CODE,
                FtMfClosingPrice.NAV_DATE,
                FtMfClosingPrice.NAV_RS
            ).filter(
                FtMfClosingPrice.SCHEME_CODE.in_(distinct_scheme_codes),
                FtMfClosingPrice.NAV_DATE >= earliest_start_date,
                FtMfClosingPrice.NAV_DATE <= datetime.now().date()
            ).all()

            # Convert fetched data into a DataFrame
            nav_df = pd.DataFrame(nav_data, columns=['SCHEME_CODE', 'TRADE_DATE', 'NAV_PRICE'])
            nav_df["TRADE_DATE"] = pd.to_datetime(nav_df["TRADE_DATE"])
            # Merge the NAV data with the SIP DataFrame on scheme code and trade date
            
            
            sip_df["FREQUENCY"] = sip_df["FREQUENCY"].str.lower()
            sip_df['SIP_DATES'] = sip_df.apply(generate_sip_dates, axis=1)
            sip_df = sip_df.explode('SIP_DATES')
            sip_df['TRADE_DATE'] = sip_df['SIP_DATES']
            sip_df['TRADE_DATE'] = pd.to_datetime(sip_df['TRADE_DATE'])
            sip_df['TRANSACTION_TYPE'] = 'Purchase'
            sip_df['AMOUNT'] = sip_df['SIP_AMOUNT']
            sip_df['TRAN_TYPE'] = 'B'
            sip_df['ACTUAL_AMOUNT'] = sip_df['SIP_AMOUNT']
            sip_df['STATUS'] = 'Success'
            sip_df['CREATED_BY_ID'] = sip_df['UPDATED_BY_ID'] = user_id 
            sip_df['CREATED_DATE'] = sip_df['UPDATED_DATE'] = datetime.now()
            sip_df = pd.merge(sip_df, nav_df, how='outer', on=['SCHEME_CODE', 'TRADE_DATE'])
            sip_df['NAV_PRICE'] = sip_df['NAV_PRICE'].bfill()
            sip_df.dropna(subset=['TRANSACTION_TYPE'], inplace=True)
            sip_df['UNITS'] = sip_df['SIP_AMOUNT'] / sip_df['NAV_PRICE']
            sip_df.rename(columns={"UNITS":"QUANTITY", "NAV_PRICE":"RATE"}, inplace=True)
            sip_df.replace({np.nan: None, pd.NaT: None}, inplace=True)
            sip_df.drop(columns=['SIP_START_DATE', 'SIP_AMOUNT', 'SIP_DATES', 'NO_OF_INSTALLMENT', 'FREQUENCY'], inplace=True)
            return sip_df
        return pd.DataFrame()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SIP DATA | ERROR WHILE FETCHING THE DATA | {e}")
        return pd.DataFrame()


async def get_mf_switch_data(db: Session, client_code, user_id: str):
    try:
        getswitch_data = db.query(ProspectFtMfSwitchTransactionMaster.CLIENT_CODE,
            ProspectFtMfSwitchTransactionMaster.ADVISOR_NAME,
            ProspectFtMfSwitchTransactionMaster.FROM_SCHEME_CODE,
            ProspectFtMfSwitchTransactionMaster.FROM_SCHEME_NAME,
            ProspectFtMfSwitchTransactionMaster.FROM_FOLIO_NO,
            ProspectFtMfSwitchTransactionMaster.TO_SCHEME_CODE,
            ProspectFtMfSwitchTransactionMaster.TO_SCHEME_NAME,
            ProspectFtMfSwitchTransactionMaster.TO_FOLIO_NO,
            ProspectFtMfSwitchTransactionMaster.DATE_OF_SWITCH,
            ProspectFtMfSwitchTransactionMaster.AMOUNT,
            ProspectFtMfSwitchTransactionMaster.RE_INVEST
            ).filter(ProspectFtMfSwitchTransactionMaster.CLIENT_CODE == client_code).all()
        if getswitch_data:
            switch_df = pd.DataFrame.from_records(getswitch_data, columns=['CLIENT_CODE', 'ADVISOR_NAME','FROM_SCHEME_CODE', 
                        'FROM_SCHEME_NAME', 'FROM_FOLIO_NO', 'TO_SCHEME_CODE', 'TO_SCHEME_NAME', 'TO_FOLIO_NO', 'DATE_OF_SWITCH', 'AMOUNT',"RE_INVEST"])
            earliest_start_date = switch_df['DATE_OF_SWITCH'].min()
            distinct_scheme_codes = switch_df['FROM_SCHEME_CODE'].drop_duplicates().to_list()
            distinct_scheme_codes += switch_df['TO_SCHEME_CODE'].drop_duplicates().to_list()
            # Fetch NAV prices for the distinct scheme codes from the earliest start date to the current date
            nav_data = db.query(
                FtMfClosingPrice.SCHEME_CODE,
                FtMfClosingPrice.NAV_DATE,
                FtMfClosingPrice.NAV_RS
            ).filter(
                FtMfClosingPrice.SCHEME_CODE.in_(distinct_scheme_codes),
                FtMfClosingPrice.NAV_DATE >= earliest_start_date,
                FtMfClosingPrice.NAV_DATE <= datetime.now().date()
            ).all()

            # Convert fetched data into a DataFrame
            nav_df = pd.DataFrame(nav_data, columns=['SCHEME_CODE', 'TRADE_DATE', 'NAV_PRICE'])

            sell_df = switch_df.copy()
            sell_df['TRADE_DATE'] = pd.to_datetime(sell_df['DATE_OF_SWITCH']).dt.date
            sell_df['TRANSACTION_TYPE'] = 'SELL'
            sell_df['SCHEME_CODE'] = sell_df['FROM_SCHEME_CODE']
            sell_df['SCHEME_NAME'] = sell_df['FROM_SCHEME_NAME']
            sell_df['FOLIO_NO'] = sell_df['FROM_FOLIO_NO']
            sell_df['TRAN_TYPE'] = 'S'
            sell_df['ACTUAL_AMOUNT'] = -sell_df['AMOUNT']
            sell_df['STATUS'] = 'Success'
            
            # Create purchase entry for "to" scheme
            buy_df = switch_df.copy()
            buy_df['TRADE_DATE'] = pd.to_datetime(buy_df['DATE_OF_SWITCH']).dt.date
            buy_df['TRANSACTION_TYPE'] = 'PURCHASE'
            buy_df['SCHEME_CODE'] = buy_df['TO_SCHEME_CODE']
            buy_df['SCHEME_NAME'] = buy_df['TO_SCHEME_NAME']
            buy_df['FOLIO_NO'] = buy_df['TO_FOLIO_NO']
            buy_df['TRAN_TYPE'] = 'B'
            buy_df['ACTUAL_AMOUNT'] = buy_df['AMOUNT']
            buy_df['STATUS'] = 'Success'
            
            # Common fields for both dataframes
            final_switch_df = pd.concat([sell_df, buy_df], ignore_index=True)
            final_switch_df.drop(columns=['FROM_SCHEME_CODE', 'FROM_SCHEME_NAME', 'TO_SCHEME_CODE', 'TO_SCHEME_NAME', 'DATE_OF_SWITCH',
                                    'FROM_FOLIO_NO', 'TO_FOLIO_NO'], inplace=True)
            final_switch_df['CREATED_BY_ID'] = final_switch_df['UPDATED_BY_ID'] = user_id 
            final_switch_df['CREATED_DATE'] = final_switch_df['UPDATED_DATE'] = datetime.now()
            final_switch_df['TRADE_DATE'] = pd.to_datetime(final_switch_df['TRADE_DATE'])
            nav_df['TRADE_DATE'] = pd.to_datetime(nav_df['TRADE_DATE'])
            final_switch_df = pd.merge(final_switch_df, nav_df, how='outer', on=['SCHEME_CODE', 'TRADE_DATE'])
            final_switch_df['NAV_PRICE'] = final_switch_df.groupby('SCHEME_CODE')['NAV_PRICE'].bfill()
            final_switch_df.dropna(subset=['TRANSACTION_TYPE'], inplace=True)
            final_switch_df['UNITS'] = final_switch_df['AMOUNT'] / final_switch_df['NAV_PRICE']
            final_switch_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
            final_switch_df.rename(columns={"UNITS":"QUANTITY", "NAV_PRICE":"RATE"}, inplace=True)
            return final_switch_df
        return pd.DataFrame()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SWITCH DATA | ERROR WHILE FETCHING THE DATA | {e}")
        return pd.DataFrame()


async def get_mf_stp_data(db: Session, client_code, user_id: str):
    try:
        getstp_data = db.query(ProspectFtMfStpTransactionMaster.CLIENT_CODE,
            ProspectFtMfStpTransactionMaster.ADVISOR_NAME,
            ProspectFtMfStpTransactionMaster.FROM_SCHEME_CODE,
            ProspectFtMfStpTransactionMaster.FROM_SCHEME_NAME,
            ProspectFtMfStpTransactionMaster.FROM_FOLIO_NO,
            ProspectFtMfStpTransactionMaster.TO_SCHEME_CODE,
            ProspectFtMfStpTransactionMaster.TO_SCHEME_NAME,
            ProspectFtMfStpTransactionMaster.TO_FOLIO_NO,
            ProspectFtMfStpTransactionMaster.STP_START_DATE,
            ProspectFtMfStpTransactionMaster.STP_AMOUNT,
            ProspectFtMfStpTransactionMaster.FREQUENCY,
            ProspectFtMfStpTransactionMaster.NO_OF_INSTALLMENT,
            ProspectFtMfStpTransactionMaster.RE_INVEST,
            ).filter(ProspectFtMfStpTransactionMaster.CLIENT_CODE == client_code).all()
        if getstp_data:
            stp_df = pd.DataFrame.from_records(getstp_data, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'FROM_SCHEME_CODE', 'FROM_SCHEME_NAME', 'FROM_FOLIO_NO', 'TO_SCHEME_CODE', 'TO_SCHEME_NAME', 'TO_FOLIO_NO', 'STP_START_DATE', 
                'AMOUNT', 'FREQUENCY', 'NO_OF_INSTALLMENT', 'RE_INVEST'])
            stp_df["FREQUENCY"] = stp_df["FREQUENCY"].str.lower()
            earliest_start_date = stp_df['STP_START_DATE'].min()
            distinct_scheme_codes = stp_df['FROM_SCHEME_CODE'].drop_duplicates().to_list()

            # Fetch NAV prices for the distinct scheme codes from the earliest start date to the current date
            nav_data = db.query(
                FtMfClosingPrice.SCHEME_CODE,
                FtMfClosingPrice.NAV_DATE,
                FtMfClosingPrice.NAV_RS
            ).filter(
                FtMfClosingPrice.SCHEME_CODE.in_(distinct_scheme_codes),
                FtMfClosingPrice.NAV_DATE >= earliest_start_date,
                FtMfClosingPrice.NAV_DATE <= datetime.now().date()
            ).all()

            # Convert fetched data into a DataFrame
            nav_df = pd.DataFrame(nav_data, columns=['SCHEME_CODE', 'TRADE_DATE', 'NAV_PRICE'])

            # Generate transaction dates based on frequency and number of installments
            stp_df['TRANSACTION_DATES'] = stp_df.apply(generate_transaction_dates_stp, axis=1)
            stp_df = stp_df.explode('TRANSACTION_DATES')
            
            # Create sell entries for "from" scheme
            sell_df = stp_df.copy()
            sell_df['TRADE_DATE'] = pd.to_datetime(sell_df['TRANSACTION_DATES'])
            sell_df['TRANSACTION_TYPE'] = 'SELL'
            sell_df['SCHEME_CODE'] = sell_df['FROM_SCHEME_CODE']
            sell_df['SCHEME_NAME'] = sell_df['FROM_SCHEME_NAME']
            sell_df['FOLIO_NO'] = sell_df['FROM_FOLIO_NO']
            sell_df['TRAN_TYPE'] = 'S'
            sell_df['ACTUAL_AMOUNT'] = -sell_df['AMOUNT']
            sell_df['STATUS'] = 'Success'
            
            # Create purchase entries for "to" scheme
            buy_df = stp_df.copy()
            buy_df['TRADE_DATE'] = pd.to_datetime(buy_df['TRANSACTION_DATES'])
            buy_df['TRANSACTION_TYPE'] = 'PURCHASE'
            buy_df['SCHEME_CODE'] = buy_df['TO_SCHEME_CODE']
            buy_df['SCHEME_NAME'] = buy_df['TO_SCHEME_NAME']
            buy_df['FOLIO_NO'] = buy_df['TO_FOLIO_NO']
            buy_df['TRAN_TYPE'] = 'B'
            buy_df['ACTUAL_AMOUNT'] = buy_df['AMOUNT']
            buy_df['STATUS'] = 'Success'
            
            # Combine sell and buy dataframes
            final_stp_df = pd.concat([sell_df, buy_df], ignore_index=True)
            final_stp_df.drop(columns=['FROM_SCHEME_CODE', 'FROM_SCHEME_NAME', 'TO_SCHEME_CODE', 'TO_SCHEME_NAME', 'STP_START_DATE', 'TRANSACTION_DATES', 
                                       'FROM_FOLIO_NO', 'TO_FOLIO_NO', 'FREQUENCY', 'NO_OF_INSTALLMENT'], inplace=True)
            final_stp_df['CREATED_BY_ID'] = final_stp_df['UPDATED_BY_ID'] = user_id 
            final_stp_df['CREATED_DATE'] = final_stp_df['UPDATED_DATE'] = datetime.now()
            final_stp_df = pd.merge(final_stp_df, nav_df, how='outer', on=['SCHEME_CODE', 'TRADE_DATE'])
            final_stp_df['NAV_PRICE'] = final_stp_df['NAV_PRICE'].bfill()
            final_stp_df.dropna(subset=['TRANSACTION_TYPE'], inplace=True)
            final_stp_df['UNITS'] = final_stp_df['AMOUNT'] / final_stp_df['NAV_PRICE']  
            final_stp_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
            final_stp_df.rename(columns={"UNITS":"QUANTITY", "NAV_PRICE":"RATE"}, inplace=True)
            return final_stp_df
        return pd.DataFrame()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS STP DATA | ERROR WHILE FETCHING THE DATA | {e}")
        return pd.DataFrame()


async def get_mf_swp_data(db: Session, client_code, user_id: str):
    try:
        getswp_data = db.query(ProspectFtMfSwpTransactionMaster.CLIENT_CODE,
            ProspectFtMfSwpTransactionMaster.ADVISOR_NAME,
            ProspectFtMfSwpTransactionMaster.SCHEME_CODE,
            ProspectFtMfSwpTransactionMaster.SCHEME_NAME,
            ProspectFtMfSwpTransactionMaster.FOLIO_NO,
            ProspectFtMfSwpTransactionMaster.SWP_START_DATE,
            ProspectFtMfSwpTransactionMaster.SWP_AMOUNT,
            ProspectFtMfSwpTransactionMaster.NO_OF_INSTALLMENT,
            ProspectFtMfSwpTransactionMaster.FREQUENCY,
            ProspectFtMfSwpTransactionMaster.RE_INVEST
            ).filter(ProspectFtMfSwpTransactionMaster.CLIENT_CODE == client_code).all()
        if getswp_data:
            swp_df = pd.DataFrame.from_records(getswp_data, columns=['CLIENT_CODE', 'ADVISOR_NAME',
                'SCHEME_CODE', 'SCHEME_NAME', 'FOLIO_NO', 'SWP_START_DATE', 'SWP_AMOUNT', 'NO_OF_INSTALLMENT', 'FREQUENCY', 'RE_INVEST'])
            swp_df["FREQUENCY"] = swp_df["FREQUENCY"].str.lower()
            earliest_start_date = swp_df['SWP_START_DATE'].min()
            distinct_scheme_codes = swp_df['SCHEME_CODE'].drop_duplicates().to_list()

            # Fetch NAV prices for the distinct scheme codes from the earliest start date to the current date
            nav_data = db.query(
                FtMfClosingPrice.SCHEME_CODE,
                FtMfClosingPrice.NAV_DATE,
                FtMfClosingPrice.NAV_RS
            ).filter(
                FtMfClosingPrice.SCHEME_CODE.in_(distinct_scheme_codes),
                FtMfClosingPrice.NAV_DATE >= earliest_start_date,
                FtMfClosingPrice.NAV_DATE <= datetime.now().date()
            ).all()
            nav_df = pd.DataFrame(nav_data, columns=['SCHEME_CODE', 'TRADE_DATE', 'NAV_PRICE'])
            swp_df['SWP_DATES'] = swp_df.apply(generate_swp_dates, axis=1)  # Assuming generate_swp_dates is similar to generate_sip_dates
            swp_df = swp_df.explode('SWP_DATES')
            swp_df['TRADE_DATE'] = pd.to_datetime(swp_df['SWP_DATES'])
            swp_df['TRANSACTION_TYPE'] = 'WITHDRAWAL'
            swp_df['AMOUNT'] = swp_df['SWP_AMOUNT']
            swp_df['TRAN_TYPE'] = 'S'
            swp_df['ACTUAL_AMOUNT'] = -swp_df['SWP_AMOUNT']  # Negative because it's a withdrawal
            swp_df['STATUS'] = 'Success'
            swp_df['CREATED_BY_ID'] = swp_df['UPDATED_BY_ID'] = user_id 
            swp_df['CREATED_DATE'] = swp_df['UPDATED_DATE'] = datetime.now()
            swp_df = pd.merge(swp_df, nav_df, how='outer', on=['SCHEME_CODE', 'TRADE_DATE'])
            swp_df['NAV_PRICE'] = swp_df['NAV_PRICE'].bfill()
            swp_df.dropna(subset=['TRANSACTION_TYPE'], inplace=True)
            swp_df['UNITS'] = swp_df['AMOUNT'] / swp_df['NAV_PRICE']
            swp_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
            swp_df.drop(columns=['SWP_START_DATE', 'SWP_AMOUNT', 'SWP_DATES', 'NO_OF_INSTALLMENT', 'FREQUENCY'], inplace=True)
            swp_df.rename(columns={"UNITS":"QUANTITY", "NAV_PRICE":"RATE"}, inplace=True)
            return swp_df
        return pd.DataFrame()
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SWP DATA | ERROR WHILE FETCHING THE DATA | {e}")
        return pd.DataFrame()


async def get_advisor_data(db:Session, client_code):
    try:
        query = db.query(ProspectFtTransaction.ADVISOR_NAME,
                         ProspectFtTransaction.ISIN,
                         DMSecurityMaster.SCRIP_KEY
                         ).join(
                             DMSecurityMaster, 
                             ProspectFtTransaction.SCHEME_CODE == DMSecurityMaster.SCRIP_KEY,
                             isouter = True)
        query = query.filter(ProspectFtTransaction.CLIENT_CODE == client_code)
        query = query.all()
        query_df = pd.DataFrame(query, columns=["ADVISOR_NAME", "ISIN", "SCHEME_CODE"])
        query_df.drop_duplicates(subset=["ISIN"],  inplace=True)
        return query_df
    except Exception as e:
        app_logger.trace(f"ADVISOR AND SCHEME CODE | | {traceback.format_exc()}")
        return pd.DataFrame()


async def get_advisor_data_based_on_scheme_code(db:Session, client_code):
    try:
        query = db.query(ProspectFtTransaction.ADVISOR_NAME,
                         DMSecurityMaster.SCRIP_KEY
                         ).join(
                             DMSecurityMaster, 
                             ProspectFtTransaction.SCHEME_CODE == DMSecurityMaster.SCRIP_KEY,
                             isouter = True)
        query = query.filter(ProspectFtTransaction.CLIENT_CODE == client_code)
        query = query.all()
        query_df = pd.DataFrame(query, columns=["ADVISOR_NAME", "SCHEME_CODE"])
        query_df.drop_duplicates(subset=["SCHEME_CODE"],  inplace=True)
        return query_df
    except Exception as e:
        app_logger.trace(f"ADVISOR AND SCHEME CODE | | {traceback.format_exc()}")
        return pd.DataFrame()


async def generate_mf_systematic_records(client_code, closing_date, db, user_id):
    try:
        transaction_df, sip_df, final_switch_df, final_stp_df, swp_df = await asyncio.gather(
            get_mf_transaction_data(db, client_code),
            get_mf_sip_data(db, client_code, user_id),
            get_mf_switch_data(db, client_code, user_id),
            get_mf_stp_data(db, client_code, user_id),
            get_mf_swp_data(db, client_code, user_id)
        )
        # transaction_df = await get_mf_transaction_data(db, client_code)
        # sip_df = await get_mf_sip_data(db, client_code, user_id)
        # final_switch_df = await get_mf_switch_data(db, client_code, user_id) 
        # final_stp_df = await get_mf_stp_data(db, client_code, user_id) 
        # swp_df = await get_mf_swp_data(db, client_code, user_id)
        transaction_df["flag"] = 1
        sip_df["flag"] = 1
        final_stp_df["flag"] = 1
        final_switch_df["flag"] = 1
        swp_df["flag"] = 1
        # Concatenate all transaction dataframes
        all_transactions_df = pd.concat([transaction_df, sip_df, final_stp_df, final_switch_df, swp_df], ignore_index=True)
        just_transactions_df =  pd.concat([sip_df, final_stp_df, final_switch_df, swp_df], ignore_index=True)
        if all_transactions_df.empty:
            app_logger.info(f"MUTUAL FUNDS CORPORATE ACTION | NO DATA FOUND | ")
            return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        all_transactions_df['SCHEME_CODE'] = all_transactions_df['SCHEME_CODE'].astype(int)
        scheme_code_list = all_transactions_df['SCHEME_CODE'].drop_duplicates().to_list()
        reinvestemet_sec_df = all_transactions_df[all_transactions_df["RE_INVEST"] == "R"].copy()
        scheme_code_list_reinvest = reinvestemet_sec_df['SCHEME_CODE'].drop_duplicates().to_list()
        all_transactions_df['TRADE_DATE'] = pd.to_datetime(all_transactions_df['TRADE_DATE'])
        start_date = all_transactions_df['TRADE_DATE'].min()
        end_date = closing_date
        # Fetch corporate actions data
        get_advisor_data_df = await get_advisor_data_based_on_scheme_code(db=db, client_code=client_code)
        corporate_actions_data_1 = db.query(
            ProspectMFCorporateActionMaster.SCHEME_CODE,
            ProspectMFCorporateActionMaster.ISIN,
            ProspectMFCorporateActionMaster.DIVD_PERC,
            ProspectMFCorporateActionMaster.CORPORATE_RATE,
            ProspectMFCorporateActionMaster.RECORD_DATE,
            ProspectMFCorporateActionMaster.F_RATIO,
            ProspectMFCorporateActionMaster.T_RATIO,
            ProspectMFCorporateActionMaster.ACTION_TYPE
        ).filter(ProspectMFCorporateActionMaster.SCHEME_CODE.in_(scheme_code_list),
                 ProspectMFCorporateActionMaster.RECORD_DATE >= start_date,
                 ProspectMFCorporateActionMaster.RECORD_DATE <= end_date,
                 ProspectMFCorporateActionMaster.ACTION_TYPE != 'DIVIDEND',
                ).all()

        corporate_actions_df_1 = pd.DataFrame(corporate_actions_data_1, columns=['SCHEME_CODE', 'ISIN', 
            'DIVD_PERC', 'CORPORATE_RATE', 'TRADE_DATE', 'F_RATIO', 'T_RATIO', 'TRANSACTION_TYPE'])

        corporate_actions_data_2 = db.query(
            ProspectMFCorporateActionMaster.SCHEME_CODE,
            ProspectMFCorporateActionMaster.ISIN,
            ProspectMFCorporateActionMaster.DIVD_PERC,
            ProspectMFCorporateActionMaster.CORPORATE_RATE,
            ProspectMFCorporateActionMaster.RECORD_DATE,
            ProspectMFCorporateActionMaster.F_RATIO,
            ProspectMFCorporateActionMaster.T_RATIO,
            ProspectMFCorporateActionMaster.ACTION_TYPE,
            FtMfClosingPrice.NAV_RS,
            FtMfClosingPrice.NAV_DATE
        ).join(
            FtMfClosingPrice,
                and_(
                    ProspectMFCorporateActionMaster.SCHEME_CODE == FtMfClosingPrice.SCHEME_CODE,
                    ProspectMFCorporateActionMaster.RECORD_DATE == FtMfClosingPrice.NAV_DATE,
                ),
                isouter=True
        ).filter(ProspectMFCorporateActionMaster.SCHEME_CODE.in_(scheme_code_list_reinvest),
                 ProspectMFCorporateActionMaster.RECORD_DATE >= start_date,
                 ProspectMFCorporateActionMaster.RECORD_DATE <= end_date,
                 ProspectMFCorporateActionMaster.ACTION_TYPE == 'DIVIDEND',
                ).all()
        
        # # Convert corporate actions data into a DataFrame
        corporate_actions_df_2 = pd.DataFrame(corporate_actions_data_2, columns=['SCHEME_CODE', 'ISIN', 
            'DIVD_PERC', 'CORPORATE_RATE', 'TRADE_DATE', 'F_RATIO', 'T_RATIO', 'TRANSACTION_TYPE', "NAV","NAV_DATE"])
        if 'DIVD_PERC' in corporate_actions_df_2.columns and not corporate_actions_df_2['DIVD_PERC'].isnull().all():
            corporate_actions_df_2 = corporate_actions_df_2[corporate_actions_df_2['DIVD_PERC'].abs() > 0.000001].copy()
            # corporate_actions_df_2["DIVD_PERC"] = corporate_actions_df_2["DIVD_PERC"].astype(float).round(5)
        corporate_actions_df_2["RE_INVEST"] = "R"
        corporate_actions_df = pd.concat([corporate_actions_df_1, corporate_actions_df_2], ignore_index=True)
        if corporate_actions_df.empty:
            app_logger.info(f"MUTUAL FUNDS CORPORATE ACTION | NO DATA FOUND | ")
            if not just_transactions_df.empty:
                just_transactions_df["SOURCE_ID"] = 3
                just_transactions_df = just_transactions_df[["SCHEME_CODE", "ADVISOR_NAME", "AMOUNT","ACTUAL_AMOUNT","RATE","QUANTITY","TRAN_TYPE",
                            "TRANSACTION_TYPE","TRADE_DATE","SOURCE_ID","RE_INVEST"]].copy()
                just_transactions_df["CLIENT_CODE"] = client_code
                just_transactions_df["IS_CORPORATE_ACTION"] = 1
                just_transactions_df["STATUS"] = "Success"
                just_transactions_df["CREATED_BY_ID"] = just_transactions_df["UPDATED_BY_ID"] = user_id
                just_transactions_df["CREATED_DATE"] = just_transactions_df["UPDATED_DATE"] = datetime.now()
                return True, "No Corporate Action Found for Transaction", just_transactions_df
            else:
                return True, "No Corporate Action Found for Transaction", pd.DataFrame()
        corporate_actions_df["flag"] = 2
        # # Merge corporate actions with all transactions
        corporate_actions_df["NAV"] = corporate_actions_df["NAV"].astype(float).round(8)
        corporate_actions_df["NAV"].bfill(inplace=True)
        all_transactions_df['TRADE_DATE'] = pd.to_datetime(all_transactions_df['TRADE_DATE'])
        corporate_actions_df['TRADE_DATE'] = pd.to_datetime(corporate_actions_df['TRADE_DATE'])
        all_transactions_df["QUANTITY"] = all_transactions_df["QUANTITY"].astype(float).round(5)
        all_transactions_df = pd.concat([all_transactions_df, corporate_actions_df], ignore_index=True)
        # all_transactions_df.to_csv('all_transactions_df_before_processing.csv', index=False)
        all_transactions_df.sort_values(by=['TRADE_DATE','flag',"TRAN_TYPE"], ascending=[True, False, True], inplace=True)
        final_data_df = pd.DataFrame()
        xirr_df = pd.DataFrame()
        scheme_codes_data = all_transactions_df['SCHEME_CODE'].dropna().drop_duplicates().to_list()
        scheme_expiry_data = db.query(DmMutualFund.STATUS, DmMutualFund.MATURITY_DATE).filter(
            DmMutualFund.SCHEME_CODE.in_(scheme_codes_data)
        )
        scheme_expiry_df = pd.DataFrame(scheme_expiry_data, columns=["status", "maturity_date"])
        scheme_expiry_df["maturity_date"] = pd.to_datetime(scheme_expiry_df["maturity_date"]).dt.date
        scheme_expiry_df.loc[scheme_expiry_df['maturity_date'] == date(1900, 1, 1), 'status'] = 'active'
        final_corporate_action_df = pd.DataFrame()
        # all_transactions_df.to_csv("all_transactions_df.csv", index=False)
        for scheme in all_transactions_df['SCHEME_CODE'].unique():
            purchase_rows_df = pd.DataFrame()
            temp_transactions_df = all_transactions_df[all_transactions_df['SCHEME_CODE'] == scheme].copy()
            temp_transactions_df['ADVISOR_NAME'] = temp_transactions_df['ADVISOR_NAME'].ffill().bfill()
            temp_transactions_df['RE_INVEST'] = temp_transactions_df['RE_INVEST'].ffill().bfill()
            # temp_transactions_df.to_csv(f"temp_transactions_df_{scheme}.csv", index=False)
            cumulative_quantity = 0
            for index, row in temp_transactions_df.iterrows():
                cumulative_quantity = round(cumulative_quantity, 8)
                if row['TRANSACTION_TYPE'] in ['Purchase', 'Corpus In', 'Nfo', 'Segregated Units', 'Transmission In']:
                    cumulative_quantity += row['QUANTITY']
                    temp_transactions_df.at[index, 'ACTUAL_QUANTITY'] = temp_transactions_df.at[index, 'QUANTITY']
                    purchase_rows_dict = {
                        "TRADE_DATE": row["TRADE_DATE"],
                        "QUANTITY": float(row['QUANTITY']),
                        "RATE": float(row['RATE']),
                        "TRANSACTION_TYPE": "PURCHASE"
                    }
                    purchase_row = pd.DataFrame([purchase_rows_dict])
                    purchase_rows_df = pd.concat([purchase_rows_df, purchase_row], ignore_index=True)
                elif row['TRANSACTION_TYPE'].upper() == 'DIVIDEND':
                    if cumulative_quantity > 0.0:
                        if temp_transactions_df.at[index, 'RE_INVEST'] == 'R':
                            temp_purchase_df = purchase_rows_df[
                                    ((purchase_rows_df['TRADE_DATE'] < row['TRADE_DATE']) |
                                    ((purchase_rows_df['TRADE_DATE'] == row['TRADE_DATE']) & 
                                    (purchase_rows_df['TRANSACTION_TYPE'] == 'SPLIT IN')))
                                ].copy()
                            temp_cumulative_quantity = temp_purchase_df['QUANTITY'].sum()
                            rate = float(row['DIVD_PERC'])  # Ensure rate is a float
                            nav_value = float(row['NAV']) 
                            temp_cumulative_quantity = float(temp_cumulative_quantity)  # Ensure temp_cumulative_quantity is a float
                            amount = round(rate * temp_cumulative_quantity, 3)
                            quantity = round(amount/nav_value, 3)
                            new_row = {
                                "SCHEME_CODE": scheme,
                                "AMOUNT": amount,
                                "ACTUAL_AMOUNT": -amount,
                                "RATE": nav_value,
                                "QUANTITY": quantity,
                                "TRAN_TYPE":"S",
                                "TRANSACTION_TYPE":"Div Reinvest",
                                "TRADE_DATE":row["TRADE_DATE"],
                                "RE_INVEST":row["RE_INVEST"]
                            }
                            new_row_df = pd.DataFrame([new_row])
                            final_corporate_action_df = pd.concat([final_corporate_action_df, new_row_df], ignore_index=True)                    
                            purchase_rows_dict = {
                                "TRADE_DATE": row["TRADE_DATE"],
                                "QUANTITY": quantity,
                                "RATE": nav_value,
                                "TRANSACTION_TYPE": "DIVIDEND"
                            }
                            purchase_row = pd.DataFrame([purchase_rows_dict])
                            purchase_rows_df = pd.concat([purchase_rows_df, purchase_row], ignore_index=True)
                            cumulative_quantity += quantity
                    else:
                        all_transactions_df.drop(index, inplace=True)
                
                elif row['TRANSACTION_TYPE'].upper() == 'BONUS':
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
                            calculated_value = round(calculated_value, 8)
                            # Extract the decimal part
                            # decimal_part = calculated_value - math.floor(calculated_value)
                            # if decimal_part > 0.9:
                                # rounded_value = math.floor(calculated_value) + 1
                            # else:
                                # rounded_value = math.floor(calculated_value)
                            new_row = {
                                "SCHEME_CODE": scheme,
                                "RATE": 0,
                                "AMOUNT": 0,
                                "TRAN_TYPE":"B",
                                "ACTUAL_AMOUNT": 0,
                                "QUANTITY": calculated_value,
                                "TRANSACTION_TYPE":"Bonus",
                                "TRADE_DATE":row["TRADE_DATE"],
                                "RE_INVEST":row["RE_INVEST"]
                            }
                            new_row_df = pd.DataFrame([new_row])
                            final_corporate_action_df = pd.concat([final_corporate_action_df, new_row_df], ignore_index=True)
                            cumulative_quantity += calculated_value
                            purchase_rows_dict = {
                                "TRADE_DATE": row["TRADE_DATE"],
                                "QUANTITY": calculated_value,
                                "RATE": 0,
                                "TRANSACTION_TYPE": "BONUS"
                            }
                            purchase_row = pd.DataFrame([purchase_rows_dict])
                            purchase_rows_df = pd.concat([purchase_rows_df, purchase_row], ignore_index=True)
                        else:
                            all_transactions_df.drop(index, inplace=True)
                    else:
                        all_transactions_df.drop(index, inplace=True)
                elif row['TRANSACTION_TYPE'] in ["Corpus Out", "Transmission Out", "Redemption"]:
                    if cumulative_quantity > 0.0 and cumulative_quantity >= float(row['QUANTITY']):
                        sell_quantity = float(row['QUANTITY'])
                        if sell_quantity == cumulative_quantity:
                            cumulative_quantity = 0
                            purchase_rows_df = pd.DataFrame()
                        else:
                            cumulative_quantity -= sell_quantity
                            remaining_sell_quantity = sell_quantity
                            for purchase_index, purchase_row in purchase_rows_df.iterrows():
                                if remaining_sell_quantity < 0:
                                    break
                                purchase_quantity = purchase_row['QUANTITY']
                                if remaining_sell_quantity >= purchase_quantity:
                                    remaining_sell_quantity -= purchase_quantity
                                    purchase_rows_df.drop(purchase_index, inplace=True)
                                else:
                                    purchase_rows_df.at[purchase_index, 'QUANTITY'] -= remaining_sell_quantity
                                    remaining_sell_quantity = 0
                    else:
                        all_transactions_df.at[index, 'STATUS'] = 'Failed'
                        break

                elif row['TRANSACTION_TYPE'].upper() == "SPLIT":
                    if cumulative_quantity > 0.0:
                        split_type_in = "Split In"
                        split_type_out = "Split Out"
                        f_ratio = float(row['F_RATIO'])  # Ensure F_RATIO is a float
                        t_ratio = float(row['T_RATIO'])  # Ensure T_RATIO is a float
                        temp_purchase_df = pd.DataFrame()
                        if f_ratio != 0.0 and t_ratio != 0.0:
                            for i, purchase_row in purchase_rows_df.iterrows():
                                curr_quantity = float(purchase_row['QUANTITY'])
                                curr_rate = float(purchase_row['RATE'])
                                new_row = {
                                    "SCHEME_CODE": scheme,
                                    "RATE": curr_rate,
                                    'TRANSACTION_TYPE' : split_type_out,
                                    "QUANTITY" : purchase_row['QUANTITY'],
                                    "ACTUAL_QUANTITY": purchase_row['QUANTITY'],
                                    "AMOUNT": curr_quantity * curr_rate,
                                    "ACTUAL_AMOUNT": -1 * curr_quantity * curr_rate,
                                    "TRAN_TYPE":"S",
                                    "TRADE_DATE":row["TRADE_DATE"],
                                    "RE_INVEST":row["RE_INVEST"]
                                    }
                                new_row_df = pd.DataFrame([new_row])
                                final_corporate_action_df = pd.concat([final_corporate_action_df, new_row_df], ignore_index=True)
                                new_quantity = float(curr_quantity) * f_ratio/t_ratio 
                                new_quantity = round(new_quantity, 8)
                                # decimal_part = new_quantity - math.floor(new_quantity)
                                # if decimal_part > 0.9:
                                #     rounded_value = math.floor(new_quantity) + 1
                                # else:
                                #     rounded_value = math.floor(new_quantity)
                                
                                new_rate = curr_rate * t_ratio/f_ratio
                                new_row_2 = {
                                    "SCHEME_CODE": scheme,
                                    'TRANSACTION_TYPE' : split_type_in,
                                    "QUANTITY" : new_quantity,
                                    'RATE' : new_rate,
                                    "AMOUNT": new_rate * new_quantity,
                                    "ACTUAL_AMOUNT":  new_rate * new_quantity,
                                    "TRAN_TYPE":"B",
                                    "TRADE_DATE":row["TRADE_DATE"],
                                    "RE_INVEST":row["RE_INVEST"]
                                    }
                                new_row_df_2 = pd.DataFrame([new_row_2])
                                final_corporate_action_df = pd.concat([final_corporate_action_df, new_row_df_2], ignore_index=True)
                                temp_purchase_df = pd.concat([temp_purchase_df, new_row_df_2], ignore_index=True)
                            cumulative_quantity = cumulative_quantity * f_ratio / t_ratio
                            if not temp_purchase_df.empty:
                                purchase_rows_df = temp_purchase_df[["TRADE_DATE","QUANTITY","RATE", "TRANSACTION_TYPE"]].copy()
                            else:
                                app_logger.info(f"SPLIT | MISSING DATA | scheme_code:{scheme}")
                        else:
                            all_transactions_df.drop(index, inplace=True)
                    else:
                        all_transactions_df.drop(index, inplace=True)
            temp_transactions_df2 = final_corporate_action_df.copy()
            # temp_transactions_df2 = temp_transactions_df2[["CLIENT_CODE", "ADVISOR_NAME", "SCHEME_CODE", "FOLIO_NO", "TRADE_DATE", "TRANSACTION_TYPE", "AMOUNT", "TRAN_TYPE", "ACTUAL_AMOUNT", "STATUS", "RATE", 
            #                                 "QUANTITY", "RE_INVEST"]].copy()
            # get_advisor_data_df.rename(columns={"ADVISOR_NAME": "ADVISOR_NAME_1"}, inplace=True)
            # temp_transactions_df2 = pd.merge(temp_transactions_df2, get_advisor_data_df, on="SCHEME_CODE", how="left")
            # temp_transactions_df2["ADVISOR_NAME"].fillna(temp_transactions_df2["ADVISOR_NAME_1"], inplace=True)
            # temp_transactions_df2.drop(columns=["ADVISOR_NAME_1"], inplace=True)

        final_data_df = pd.concat([final_data_df, temp_transactions_df2], ignore_index=True)
        final_data_df["SOURCE_ID"] = 3
        final_data_df["CLIENT_CODE"] = client_code
        final_data_df["IS_CORPORATE_ACTION"] = 1
        final_data_df["STATUS"] = "Success"
        final_data_df["CREATED_BY_ID"] = final_data_df["UPDATED_BY_ID"] = user_id
        final_data_df["CREATED_DATE"] = final_data_df["UPDATED_DATE"] = datetime.now()
        if not final_data_df.empty:
            final_data_df = pd.merge(final_data_df, get_advisor_data_df, on="SCHEME_CODE", how="left")
        if not just_transactions_df.empty:
            just_transactions_df["SOURCE_ID"] = 3
            just_transactions_df = just_transactions_df[["SCHEME_CODE", "ADVISOR_NAME", "AMOUNT","ACTUAL_AMOUNT","RATE","QUANTITY","TRAN_TYPE",
                        "TRANSACTION_TYPE","TRADE_DATE","SOURCE_ID","RE_INVEST"]].copy()
            just_transactions_df["CLIENT_CODE"] = client_code
            just_transactions_df["IS_CORPORATE_ACTION"] = 1
            just_transactions_df["STATUS"] = "Success"
            just_transactions_df["CREATED_BY_ID"] = just_transactions_df["UPDATED_BY_ID"] = user_id
            just_transactions_df["CREATED_DATE"] = just_transactions_df["UPDATED_DATE"] = datetime.now()
            final_data_df = pd.concat([final_data_df, just_transactions_df], ignore_index=True)
        if final_data_df.empty:
            return False, "No data found", pd.DataFrame()
        if "ACTUAL_QUANTITY" in final_data_df.columns:
            final_data_df.drop(columns=["ACTUAL_QUANTITY"], inplace=True)
        if not final_data_df.empty:
            return True, "Corporate Action Generated Successfully", final_data_df
        else:
            return False, "Error in generating corporate action", pd.DataFrame()

    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS SYSTEMATIC RECORDS | ERROR WHILE FETCHING THE DATA | {e}")
        app_logger.trace(f"PROSPECT MUTUAL FUNDS SYSTEMATIC RECORDS | ERROR WHILE FETCHING THE DATA | {traceback.format_exc()}")
        return False, "Error while fetching the data", pd.DataFrame()


@prospect_upload_router.get("/transactions/generate_corporate_actions/", dependencies=[Depends(JWTBearer())])
async def generate_corporate_actions(header_request: Request, client_id: int, db: Session = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        raise HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        raise HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "create_ppt")
    if not has_permission:
        raise HTTPException(status_code=403, detail=msg)
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")

    closing_date = datetime.now().date()
    final_df = pd.DataFrame()
    try:
        # Check if there are existing corporate actions for the client
        existing_corporate_actions = db.query(ProspectFtTransaction).filter(
            ProspectFtTransaction.CLIENT_CODE == client_id,
            ProspectFtTransaction.IS_CORPORATE_ACTION == 1
        ).all()

        if existing_corporate_actions:
            # Delete existing corporate actions for the client
            db.query(ProspectFtTransaction).filter(
                ProspectFtTransaction.CLIENT_CODE == client_id,
                ProspectFtTransaction.IS_CORPORATE_ACTION == 1
            ).delete(synchronize_session=False)
            db.commit()

    except Exception as e:
        app_logger.error(f"Error while deleting existing corporate actions: {e}")
        app_logger.trace(f"Error while deleting existing corporate actions: {traceback.format_exc()}")
        raise HTTPException(status_code=400, detail="Error while deleting existing corporate actions")
    corporate_action_status = db.query(ProspectCorporateActionStatus).filter(
        ProspectCorporateActionStatus.CLIENT_CODE == client_id
    ).first()
    if corporate_action_status:
            # Update the corporate action run date
        corporate_action_status.STATUS = "Progress"
    else:
        # Create new entry in corporate action status master
        new_corporate_action_status = ProspectCorporateActionStatus(
            CLIENT_CODE=client_id,
            TRANSACTION_UPLOAD_DATE=datetime.now(),
            CORPORATE_ACTION_EXE_DATE=datetime.now(),
            STATUS="Progress",
            CREATED_BY=user_id,
            CREATED_DATE=datetime.now()
        )
        db.add(new_corporate_action_status)
    db.commit()
    equity_status, equity_msg, equity_df = await calculation_for_equity_corp_action(client_id=client_id, closing_date=closing_date, db=db,
                                                    user_id=user_id)
    if not equity_status:
        if corporate_action_status:
            # Update the corporate action run date
            corporate_action_status.CORPORATE_ACTION_EXE_DATE = datetime.now()
            corporate_action_status.STATUS = "Failed"
        else:
            # Create new entry in corporate action status master
            new_corporate_action_status = ProspectCorporateActionStatus(
                CLIENT_CODE=client_id,
                TRANSACTION_UPLOAD_DATE=datetime.now(),
                CORPORATE_ACTION_EXE_DATE=datetime.now(),
                STATUS="Failed",
                CREATED_BY=user_id,
                CREATED_DATE=datetime.now()
            )
            db.add(new_corporate_action_status)
        db.commit()
        raise HTTPException(status_code=400, detail=f"Equity Corporate Action Error: {equity_msg}")
    final_df = pd.concat([final_df, equity_df], ignore_index=True)
    # Call debt corporate action function
    debt_status, debt_msg, debt_df = await debt_corp_cal_v2(client_id=client_id, closing_date=closing_date, db=db,
                                            user_id=user_id)
    if not debt_status:
        if corporate_action_status:
            # Update the corporate action run date
            corporate_action_status.CORPORATE_ACTION_EXE_DATE = datetime.now()
            corporate_action_status.STATUS = "Failed"
        else:
            # Create new entry in corporate action status master
            new_corporate_action_status = ProspectCorporateActionStatus(
                CLIENT_CODE=client_id,
                TRANSACTION_UPLOAD_DATE=datetime.now(),
                CORPORATE_ACTION_EXE_DATE=datetime.now(),
                STATUS="Failed",
                CREATED_BY=user_id,
                CREATED_DATE=datetime.now()
            )
            db.add(new_corporate_action_status)
        db.commit()
        raise HTTPException(status_code=400, detail=f"Debt Corporate Action Error: {debt_msg}")
    final_df = pd.concat([final_df, debt_df], ignore_index=True)

    # Call mutual fund corporate action function
    mf_status, mf_msg, mf_df = await generate_mf_systematic_records(client_code=client_id, closing_date=closing_date, db=db, user_id=user_id)
    if not mf_status:
        if corporate_action_status:
            # Update the corporate action run date
            corporate_action_status.CORPORATE_ACTION_EXE_DATE = datetime.now()
            corporate_action_status.STATUS = "Failed"
        else:
            # Create new entry in corporate action status master
            new_corporate_action_status = ProspectCorporateActionStatus(
                CLIENT_CODE=client_id,
                TRANSACTION_UPLOAD_DATE=datetime.now(),
                CORPORATE_ACTION_EXE_DATE=datetime.now(),
                STATUS="Failed",
                CREATED_BY=user_id,
                CREATED_DATE=datetime.now()
            )
            db.add(new_corporate_action_status)
        db.commit()
        raise HTTPException(status_code=400, detail=f"Mutual Fund Corporate Action Error: {mf_msg}")
    final_df = pd.concat([final_df, mf_df], ignore_index=True)
    if final_df.empty:
        if corporate_action_status:
            # Update the corporate action run date
            corporate_action_status.CORPORATE_ACTION_EXE_DATE = datetime.now()
            corporate_action_status.STATUS = "Success"
        else:
            # Create new entry in corporate action status master
            new_corporate_action_status = ProspectCorporateActionStatus(
                CLIENT_CODE=client_id,
                TRANSACTION_UPLOAD_DATE=datetime.now(),
                CORPORATE_ACTION_EXE_DATE=datetime.now(),
                STATUS="Success",
                CREATED_BY=user_id,
                CREATED_DATE=datetime.now()
            )
            db.add(new_corporate_action_status)
        db.commit()
        app_logger.info(f"PROSPECT MUTUAL FUNDS SYSTEMATIC RECORDS | CORPORATE ACTION GENERATED SUCCESSFULLY | ")
        return {"status": "success", "message": "No Corporate Action Found for Transactions"}
    # final_df.to_csv("final_df2.csv", index=False)
    if "ACTUAL_QUANTITY" in final_df.columns:
        final_df.drop(columns=["ACTUAL_QUANTITY"], inplace=True)
    status, msg = await upload_bulk_file(final_df, "PROSPECT_FT_TRANSACTION")
    # status, msg = True, "Success"
    # Check if entry in corporate action status master is present
    if status:
        corporate_action_status = db.query(ProspectCorporateActionStatus).filter(
            ProspectCorporateActionStatus.CLIENT_CODE == client_id
        ).first()

        if corporate_action_status:
            # Update the corporate action run date
            corporate_action_status.CORPORATE_ACTION_EXE_DATE = datetime.now()
            corporate_action_status.STATUS = "Success"
        else:
            # Create new entry in corporate action status master
            new_corporate_action_status = ProspectCorporateActionStatus(
                CLIENT_CODE=client_id,
                TRANSACTION_UPLOAD_DATE=datetime.now(),
                CORPORATE_ACTION_EXE_DATE=datetime.now(),
                STATUS="Success",
                CREATED_BY=user_id,
                CREATED_DATE=datetime.now()
            )
            db.add(new_corporate_action_status)
        db.commit()
        app_logger.info(f"PROSPECT MUTUAL FUNDS SYSTEMATIC RECORDS | CORPORATE ACTION GENERATED SUCCESSFULLY | ")
        return {"status": "Success", "message": "Corporate Action generated successfully"}
    else:
        app_logger.trace(f"PROSPECT MUTUAL FUNDS SYSTEMATIC RECORDS | ERROR WHILE UPLOADING THE DATA | {msg}")
        if corporate_action_status:
            # Update the corporate action run date
            corporate_action_status.CORPORATE_ACTION_EXE_DATE = datetime.now()
            corporate_action_status.STATUS = "Failed"
        else:
            # Create new entry in corporate action status master
            new_corporate_action_status = ProspectCorporateActionStatus(
                CLIENT_CODE=client_id,
                TRANSACTION_UPLOAD_DATE=datetime.now(),
                CORPORATE_ACTION_EXE_DATE=datetime.now(),
                STATUS="Failed",
                CREATED_BY=user_id,
                CREATED_DATE=datetime.now()
            )
            db.add(new_corporate_action_status)
        db.commit()
        raise HTTPException(status_code=400, detail="Error while uploading the data")


async def update_corporate_action_status(db: Session,client_id: int,user_id: int):
    # Check if entry in corporate action status master is present
    corporate_action_status = db.query(ProspectCorporateActionStatus).filter(
        ProspectCorporateActionStatus.CLIENT_CODE == client_id
    ).first()

    if corporate_action_status:
        # Update the corporate action run date
        corporate_action_status.TRANSACTION_UPLOAD_DATE = datetime.now()
    else:
        # Create new entry in corporate action status master
        new_corporate_action_status = ProspectCorporateActionStatus(
            CLIENT_CODE=client_id,
            TRANSACTION_UPLOAD_DATE=datetime.now(),
            CREATED_BY=user_id,
            CREATED_DATE=datetime.now()
        )
        db.add(new_corporate_action_status)
    db.commit()


@prospect_upload_router.post("/holdings/equity/", dependencies=[Depends(JWTBearer())])
async def upload_equity_holdings_api(header_request: Request, client_id: int = Form(...), file: UploadFile = File(...), db: Session = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        raise HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        raise HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        raise HTTPException(status_code=403, detail=msg)
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")

    # Read the file into memory
    file.file.seek(0)  # Reset file pointer to the beginning
    file_size = await file.read()
    file_name = file.filename
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")

    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        excel_df.columns = excel_df.columns.str.strip()
    except Exception as e:
        app_logger.error(f"PROSPECT EQUITY HOLDINGS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    if not excel_df.empty:
        excel_df['FILE_NAME'] = file.filename
        # Rename columns to capital case
        excel_df.columns = excel_df.columns.str.strip()
        required_columns = ['Client Code', 'Advisor Name', 'ISIN', 'Quantity', 'Rate', 'Investment Amount', 'Current Value', 'As On Date']
        missing_columns = [col for col in required_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT EQUITY HOLDINGS UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
        excel_df.rename(columns={
            'Client Code': 'CLIENT_CODE',
            'Advisor Name': 'ADVISOR_NAME',
            'ISIN': 'ISIN',
            'Quantity': 'QUANTITY',
            'Rate': 'RATE',
            'Investment Amount': 'INVESTMENT_AMOUNT',
            'Current Value': 'CURRENT_VALUE',
            'As On Date': 'AS_ON_DATE'
        }, inplace=True)

        excel_df['STATUS'] = 'Success'
        # Validating mandatory columns
        if excel_df['CLIENT_CODE'].isnull().any():
            raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            raise HTTPException(status_code=400, detail="Client Code should be an integer")
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

        if client_id not in excel_df['CLIENT_CODE'].unique():
            raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")
        
        if excel_df['AS_ON_DATE'].isnull().any():
            app_logger.error(f"PROSPECT EQUITY HOLDINGS UPLOAD | MISSING AS ON DATE | AS ON DATE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="As On Date should not be blank")
        
        try:
            excel_df['AS_ON_DATE'] = pd.to_datetime(excel_df['AS_ON_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="As On Date contains invalid date values")
        
        if (excel_df['AS_ON_DATE'] > pd.Timestamp.now()).any():
            app_logger.error(f"PROSPECT EQUITY HOLDINGS UPLOAD | INVALID AS ON DATE | AS ON DATE SHOULD NOT BE GREATER THAN CURRENT DATE")
            raise HTTPException(status_code=400, detail="As On Date should not be greater than the current date")
    
        if excel_df['ISIN'].isnull().any() or excel_df['ISIN'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="ISIN should not be blank")
    
        if excel_df['ISIN'].str.match(r'^INE[A-Z0-9]{9}$').any() == False:
            raise HTTPException(status_code=400, detail="ISIN should start with 'INE' followed by 9 alphanumeric characters")
        
        if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
            raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")
    
        if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, int)).all():
            raise HTTPException(status_code=400, detail="Quantity should be of type int")
    
        if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
            raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
        
        if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Rate should be of type int or float")
        
        if excel_df['INVESTMENT_AMOUNT'].isnull().any() or excel_df['INVESTMENT_AMOUNT'].eq('').any():
            raise HTTPException(status_code=400, detail="Investment Amount should not be blank.")
        
        if not excel_df['INVESTMENT_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Investment Amount should be of type int or float")
        
        if excel_df['CURRENT_VALUE'].isnull().any() or excel_df['CURRENT_VALUE'].eq('').any() or (excel_df['CURRENT_VALUE'] == 0).any():
            raise HTTPException(status_code=400, detail="Current Value should not be blank or 0")
        
        if not excel_df['CURRENT_VALUE'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Current Value should be of type int or float")
        
        excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
        excel_df['ISIN'] = excel_df['ISIN'].astype(str)
        excel_df['ISIN'] = excel_df['ISIN'].str.strip()
        
        unique_isins = excel_df['ISIN'].str.strip().unique().tolist()
        # Fetching ISIN data from DMSecurityMaster and ProspectSecurityMaster
        security_isin = db.query(
            DMSecurityMaster.ISIN,
            DMSecurityMaster.SCRIP_KEY
            ).filter(
                # DMSecurityMaster.ASSET_CLASS == "Equity",
                func.trim(DMSecurityMaster.ISIN).in_(unique_isins),
                DMSecurityMaster.SOURCE_ID == 2
            ).all()
        # security_isin = db.query(DMSecurityMaster.ISIN, DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.ASSET_CLASS == "Equity").all()
        secmaster_excel_df = pd.DataFrame(security_isin, columns=['ISIN', 'SCHEME_CODE']).drop_duplicates()
        sec_isin_excel_df = pd.merge(excel_df, secmaster_excel_df, how='left', on='ISIN')
        secmaster_excel_df = pd.DataFrame(security_isin, columns=['ISIN', 'SCHEME_CODE']).drop_duplicates()
        sec_isin_excel_df = pd.merge(excel_df, secmaster_excel_df, how='left', on='ISIN')

        excel_df1 = sec_isin_excel_df.dropna(subset=['SCHEME_CODE']).copy()
        excel_df1['STATUS'] = "Success"
        excel_df1["SOURCE_TABLE"] = "DM_SECURITY_MASTER"

        excel_df2 = sec_isin_excel_df[sec_isin_excel_df['SCHEME_CODE'].isnull()].copy()
        excel_df2['REMARK'] = 'ISIN not found in Security Master Table'
        excel_df2['STATUS'] = "Failure"

        prospect_isin = db.query(ProspectSecurityMaster.ISIN).filter(ProspectSecurityMaster.SOURCE_ID == 2).all()
        prospect_data = pd.DataFrame()  # Initialize prospect_data as an empty DataFrame
        if prospect_isin:
            prospect_isin_excel_df = pd.DataFrame(prospect_isin, columns=['ISIN'])
            prospect_data = pd.merge(excel_df2, prospect_isin_excel_df, how='left', on='ISIN', indicator=True)
            prospect_data = prospect_data[prospect_data['_merge'] == 'both']
            prospect_data['STATUS'] = "Success"
            prospect_data['REMARK'] = ''
            prospect_data["SOURCE_TABLE"] = "PROSPECT_SECURITY_MASTER"

            prospect_data.drop('_merge', axis=1, inplace=True)
            excel_df2 = excel_df2[~excel_df2['ISIN'].isin(prospect_data['ISIN'])]

        merged_excel_df = pd.concat([excel_df1, excel_df2, prospect_data], ignore_index=True)
        
        as_on_date = excel_df['AS_ON_DATE'].min()
        existing_data_db = db.query(
            ProspectFtHolding.CLIENT_CODE,                                 
            ProspectFtHolding.ADVISOR_NAME, 
            ProspectFtHolding.AS_ON_DATE, 
            ProspectFtHolding.ISIN,  
            ProspectFtHolding.QUANTITY, 
            ProspectFtHolding.RATE, 
            ProspectFtHolding.INVESTMENT_AMOUNT, 
            ProspectFtHolding.CURRENT_VALUE
                ).filter(
                    ProspectFtHolding.CLIENT_CODE == client_id, 
                    ProspectFtHolding.SOURCE_ID == 2,
                    ProspectFtHolding.AS_ON_DATE == as_on_date,
                    ).all()
        
        if existing_data_db:
            existed_data = pd.DataFrame(existing_data_db, columns=['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'ISIN', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE'])
            existed_data[['CLIENT_CODE']] = existed_data[['CLIENT_CODE']].apply(pd.to_numeric, errors='coerce', downcast='integer')
            existed_data['AS_ON_DATE'] = pd.to_datetime(existed_data['AS_ON_DATE'], errors="coerce")
            existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
            existed_data[['ISIN', 'ADVISOR_NAME']] = existed_data[['ISIN', 'ADVISOR_NAME']].astype(str)
            comparison_columns = ['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'ISIN', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']
            duplicates = pd.merge(merged_excel_df, existed_data, on=comparison_columns, how='inner')
            merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'REMARK'] = 'Duplicate record found in the database'
            merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'STATUS'] = 'Duplicate'       


        zero_value_rows = (merged_excel_df['QUANTITY'] == 0) & (merged_excel_df['RATE'] == 0) & (merged_excel_df['INVESTMENT_AMOUNT'] == 0)
        merged_excel_df.loc[zero_value_rows, 'STATUS'] = "Failure"
        merged_excel_df.loc[zero_value_rows, 'REMARK'] = 'All values are zero'
        merged_excel_df['FILE_NAME'] = file_name
        merged_excel_df['CREATED_BY_ID'] = user_id
        merged_excel_df['SOURCE_ID'] = 2
        # merged_excel_df['SOURCE_TABLE'] = "DM_SECURITY_MASTER"
        merged_excel_df['UPDATED_BY_ID'] = user_id
        merged_excel_df['CREATED_DATE'] = datetime.now()
        merged_excel_df['UPDATED_DATE'] = datetime.now()
        # merged_excel_df["REMARK"]
        # merged_excel_df.to_csv("equity_transaction.csv", index = False)
        data = merged_excel_df.replace('', None).to_dict(orient='records')

        # Save to database
        # data = excel_df.to_dict(orient='records')
        
        response, message = await upload_bulk_file(merged_excel_df, 'PROSPECT_FT_HOLDING')
        if not response:
            app_logger.error(f"PROSPECT EQUITY HOLDINGS UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            raise HTTPException(status_code=400, detail="Error while saving the data")            
        app_logger.info(f"PROSPECT EQUITY HOLDINGS UPLOAD | UPLOADED SUCCESSFULLY | CLIENT {client_id}")
        return {"message": "File Uploaded Successfully"}
    else:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")


@prospect_upload_router.post("/holdings/debt/", dependencies=[Depends(JWTBearer())])
async def upload_debt_holdings(header_request: Request, client_id: int = Form(...), file: UploadFile = File(...), db = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        raise HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        raise HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")

    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")
    file_name = None
    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        excel_df.columns = excel_df.columns.str.strip()
        file_name = file.filename
    except Exception as e:
        app_logger.error(f"PROSPECT DEBT HOLDINGS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    if not excel_df.empty:
        excel_df['FILE_NAME'] = file.filename
        excel_df.columns = excel_df.columns.str.strip()
        # Validating mandatory columns
        required_columns = ['Client Code', 'Advisor Name', 'ISIN', 'Quantity', 'Rate', 'Investment Amount', 'Current Value', 'As On Date']
        missing_columns = [col for col in required_columns if col not in excel_df.columns]
        if missing_columns:
            app_logger.error(f"PROSPECT DEBT HOLDINGS UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
            raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
        
        excel_df.rename(columns={
            'Client Code': 'CLIENT_CODE',
            'Advisor Name': 'ADVISOR_NAME',
            'ISIN': 'ISIN',
            'Quantity': 'QUANTITY',
            'Rate': 'RATE',
            'Investment Amount': 'INVESTMENT_AMOUNT',
            'Current Value': 'CURRENT_VALUE',
            'As On Date': 'AS_ON_DATE'
        }, inplace=True)

        excel_df['STATUS'] = 'Success'
        # Validating mandatory columns
        if excel_df['CLIENT_CODE'].isnull().any():
            raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            raise HTTPException(status_code=400, detail="Client Code should be an integer")
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

        if client_id not in excel_df['CLIENT_CODE'].unique():
            raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")

        if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
        
        if excel_df['ADVISOR_NAME'].str.len().max() > 100:
            raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")
        
        if excel_df['AS_ON_DATE'].isnull().any():
            app_logger.error(f"PROSPECT DEBT HOLDINGS UPLOAD | MISSING AS ON DATE | AS ON DATE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="As On Date should not be blank")
        
        try:
            excel_df['AS_ON_DATE'] = pd.to_datetime(excel_df['AS_ON_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="As On Date contains invalid date values")
        
        if (excel_df['AS_ON_DATE'] > pd.Timestamp.now()).any():
            app_logger.error(f"PROSPECT DEBT HOLDINGS UPLOAD | INVALID AS ON DATE | AS ON DATE SHOULD NOT BE GREATER THAN CURRENT DATE")
            raise HTTPException(status_code=400, detail="As On Date should not be greater than the current date")
    
        if excel_df['ISIN'].isnull().any() or excel_df['ISIN'].str.strip().eq('').any():
            raise HTTPException(status_code=400, detail="ISIN should not be blank")
        
        if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
            raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")
    
        if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Quantity should be of type int or float")
    
        if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
            raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
        
        if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Rate should be of type int or float")
        
        if excel_df['INVESTMENT_AMOUNT'].isnull().any() or excel_df['INVESTMENT_AMOUNT'].eq('').any():
            raise HTTPException(status_code=400, detail="Investment Amount should not be blank.")
        
        if not excel_df['INVESTMENT_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Investment Amount should be of type int or float")
        
        if excel_df['CURRENT_VALUE'].isnull().any() or excel_df['CURRENT_VALUE'].eq('').any() or excel_df['CURRENT_VALUE'].eq(0).any():
            raise HTTPException(status_code=400, detail="Current Value should not be blank or zero.")
        
        if not excel_df['CURRENT_VALUE'].apply(lambda x: isinstance(x, (int, float))).all():
            raise HTTPException(status_code=400, detail="Current Value should be of type int or float")
        
        # Save to database
        excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
        excel_df['ISIN'] = excel_df['ISIN'].astype(str)
        excel_df['ISIN'] = excel_df['ISIN'].str.strip()
        
        unique_isins = excel_df['ISIN'].str.strip().unique().tolist()
        # Fetching ISIN data from DMSecurityMaster and ProspectSecurityMaster
        security_isin = db.query(
            DMSecurityMaster.ISIN,
            DMSecurityMaster.SCRIP_KEY
            ).filter(
                # DMSecurityMaster.ASSET_CLASS == "Equity",
                func.trim(DMSecurityMaster.ISIN).in_(unique_isins),
                DMSecurityMaster.SOURCE_ID == 4
            ).all()
        # security_isin.drop_duplicates(subset=['ISIN'], inplace=True)
        # security_isin = db.query(DMSecurityMaster.ISIN, DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.ASSET_CLASS == "Equity").all()
        secmaster_excel_df = pd.DataFrame(security_isin, columns=['ISIN', 'SCHEME_CODE'])
        secmaster_excel_df.drop_duplicates(subset=['ISIN'], inplace=True)
        secmaster_excel_df['ISIN'] = secmaster_excel_df['ISIN'].str.strip()
        sec_isin_excel_df = pd.merge(excel_df, secmaster_excel_df, how='left', on='ISIN')

        excel_df1 = sec_isin_excel_df.dropna(subset=['SCHEME_CODE']).copy()
        excel_df1['STATUS'] = "Success"
        excel_df1["SOURCE_TABLE"] = "DM_SECURITY_MASTER"

        excel_df2 = sec_isin_excel_df[sec_isin_excel_df['SCHEME_CODE'].isnull()].copy()
        excel_df2['REMARK'] = 'ISIN not found in Security Master Table'
        excel_df2['STATUS'] = "Failure"

        prospect_isin = db.query(ProspectSecurityMaster.ISIN).filter(ProspectSecurityMaster.SOURCE_ID == 4).all()
        prospect_data = pd.DataFrame()  # Initialize prospect_data as an empty DataFrame
        if prospect_isin:
            prospect_isin_excel_df = pd.DataFrame(prospect_isin, columns=['ISIN'])
            prospect_data = pd.merge(excel_df2, prospect_isin_excel_df, how='left', on='ISIN', indicator=True)
            prospect_data = prospect_data[prospect_data['_merge'] == 'both']
            prospect_data['STATUS'] = "Success"
            prospect_data['REMARK'] = ''
            prospect_data["SOURCE_TABLE"] = "PROSPECT_SECURITY_MASTER"

            prospect_data.drop('_merge', axis=1, inplace=True)
            excel_df2 = excel_df2[~excel_df2['ISIN'].isin(prospect_data['ISIN'])]

        merged_excel_df = pd.concat([excel_df1, excel_df2, prospect_data], ignore_index=True)
        
        as_on_date = excel_df['AS_ON_DATE'].min()
        existing_data_db = db.query(
            ProspectFtHolding.CLIENT_CODE,                                 
            ProspectFtHolding.ADVISOR_NAME, 
            ProspectFtHolding.AS_ON_DATE, 
            ProspectFtHolding.ISIN,  
            ProspectFtHolding.QUANTITY, 
            ProspectFtHolding.RATE, 
            ProspectFtHolding.INVESTMENT_AMOUNT, 
            ProspectFtHolding.CURRENT_VALUE
                ).filter(
                    ProspectFtHolding.CLIENT_CODE == client_id, 
                    ProspectFtHolding.SOURCE_ID == 4,
                    ProspectFtHolding.AS_ON_DATE == as_on_date,
                    ).all()
        if existing_data_db:
            existed_data = pd.DataFrame(existing_data_db, columns=['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'ISIN', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE'])
            existed_data[['CLIENT_CODE']] = existed_data[['CLIENT_CODE']].apply(pd.to_numeric, errors='coerce', downcast='integer')
            existed_data['AS_ON_DATE'] = pd.to_datetime(existed_data['AS_ON_DATE'], errors="coerce")
            existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
            existed_data[['ISIN', 'ADVISOR_NAME']] = existed_data[['ISIN', 'ADVISOR_NAME']].astype(str)
            comparison_columns = ['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'ISIN', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']
            duplicates = pd.merge(merged_excel_df, existed_data, on=comparison_columns, how='inner')
            merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'REMARK'] = 'Duplicate record found in the database'
            merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'STATUS'] = 'Duplicate'       


        zero_value_rows = (merged_excel_df['QUANTITY'] == 0) & (merged_excel_df['RATE'] == 0) & (merged_excel_df['INVESTMENT_AMOUNT'] == 0)
        merged_excel_df.loc[zero_value_rows, 'STATUS'] = "Failure"
        merged_excel_df.loc[zero_value_rows, 'REMARK'] = 'All values are zero'
        merged_excel_df['FILE_NAME'] = file_name
        merged_excel_df['CREATED_BY_ID'] = user_id
        merged_excel_df['SOURCE_ID'] = 4
        merged_excel_df['UPDATED_BY_ID'] = user_id
        merged_excel_df['CREATED_DATE'] = datetime.now()
        merged_excel_df['UPDATED_DATE'] = datetime.now()
        # merged_excel_df['REMARK'] = merged_excel_df['REMARK'].astype(str) 
        # merged_excel_df['REMARK'].replace({"nan":None}, inplace=True)
        
        response, message = await upload_bulk_file(merged_excel_df, 'PROSPECT_FT_HOLDING')
        if not response:
            app_logger.error(f"PROSPECT DEBT HOLDINGS UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
            raise HTTPException(status_code=400, detail="Error while saving the data")            
        app_logger.info(f"Debt holdings | uploaded successfully | client {client_id}")
        return {"message": "File Uploaded Successfully"}
    else:
        app_logger.error(f"PROSPECT DEBT HOLDINGS UPLOAD |  | UPLOADED FILE IS EMPTY")
        raise HTTPException(status_code=400, detail="Uploaded file is empty")


@prospect_upload_router.post("/holdings/mutual_funds/", dependencies=[Depends(JWTBearer())])
async def upload_mutual_funds_holdings(
    header_request: Request, 
    client_id: int = Form(...), 
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        raise HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        raise HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        raise HTTPException(status_code=403, detail=msg)
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")

    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")
    file_name = None
    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        excel_df.columns = excel_df.columns.str.strip()
        file_name = file.filename
    except Exception as e:
        app_logger.error(f"PROSPECT MUTUAL FUNDS TRANSACTIONS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")

    # Rename columns to match database schema
    excel_df.columns = excel_df.columns.str.strip()
    if excel_df.empty:
        app_logger.error(f"PROSPECT MUTUAL FUNDS TRANSACTIONS UPLOAD | UPLOADED FILE IS EMPTY")
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    required_columns = ['Client Code', 'Advisor Name', 'Scheme Code', 'Reinvest', 'Folio No.', 'Quantity', 'Rate', 'Investment Amount', 'Current Value', 'As On Date']
    missing_columns = [col for col in required_columns if col not in excel_df.columns]
    if missing_columns:
        app_logger.error(f"PROSPECT MUTUAL FUNDS TRANSACTIONS UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
    
    excel_df.rename(columns={
        'Client Code': 'CLIENT_CODE',
        'Advisor Name': 'ADVISOR_NAME',
        'Scheme Code': 'SCHEME_CODE',
        'Reinvest': 'DIV_REINVEST',
        'Folio No.': 'FOLIO_NO',
        'Quantity': 'QUANTITY',
        'Rate': 'RATE',
        'Investment Amount': 'INVESTMENT_AMOUNT',
        'Current Value': 'CURRENT_VALUE',
        'As On Date': 'AS_ON_DATE'
    }, inplace=True)

    excel_df['STATUS'] = 'Success'
    if excel_df['CLIENT_CODE'].isnull().any():
        raise HTTPException(status_code=400, detail="Client Code should not be blank")

    if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
        raise HTTPException(status_code=400, detail="Client Code should be an integer")
    
    if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
        raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
    
    if len(excel_df['CLIENT_CODE'].unique()) > 1:
        raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

    if client_id not in excel_df['CLIENT_CODE'].unique():
        raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")

    if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="Advisor Name should not be blank")
    
    if excel_df['ADVISOR_NAME'].str.len().max() > 100:
        raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")
        
    if excel_df['FOLIO_NO'].isnull().any() or excel_df['FOLIO_NO'].eq('').any():
        app_logger.error(f"PROSPECT MF HOLDINGS UPLOAD | MISSING FOLIO NUMBER | FOLIO NUMBER COLUMN IS EMPTY")
        raise HTTPException(status_code=400, detail="Folio No. should not be blank")
    
    excel_df["FOLIO_NO"] = excel_df["FOLIO_NO"].astype(str)
    if not excel_df['FOLIO_NO'].apply(lambda x: isinstance(x, str) and len(x) <= 25).all():
        raise HTTPException(status_code=400, detail="Folio No. should not be greater than 25 characters")
        
    # Validate RE_INVEST column
    excel_df["DIV_REINVEST"] = excel_df["DIV_REINVEST"].str.strip().str.upper()
    if not excel_df['DIV_REINVEST'].apply(lambda x: x in ['P', 'R']).all():
        app_logger.error(f"PROSPECT MUTUAL FUNDS TRANSACTIONS UPLOAD | INVALID DIV_REINVEST VALUES | DIV_REINVEST must be a single character, either 'P' or 'R'")
        raise HTTPException(status_code=400, detail="DIV_REINVEST must be a single character, either 'P' or 'R'")

    if excel_df['AS_ON_DATE'].isnull().any():
        app_logger.error(f"PROSPECT MUTUAL FUNDS HOLDINGS UPLOAD | MISSING AS ON DATE | AS ON DATE COLUMN IS EMPTY")
        raise HTTPException(status_code=400, detail="As On Date should not be blank")
    
    try:
        excel_df['AS_ON_DATE'] = pd.to_datetime(excel_df['AS_ON_DATE'], errors='raise')
    except ValueError:
        raise HTTPException(status_code=400, detail="As On Date contains invalid date values")
    
    if (excel_df['AS_ON_DATE'] > pd.Timestamp.now()).any():
        app_logger.error(f"PROSPECT MUTUAL FUNDS HOLDINGS UPLOAD | INVALID AS ON DATE | AS ON DATE SHOULD NOT BE GREATER THAN CURRENT DATE")
        raise HTTPException(status_code=400, detail="As On Date should not be greater than the current date")
    
    # Validate SCHEME_CODE against dm_mutual funds
    scheme_codes_from_file = excel_df['SCHEME_CODE'].unique()
    scheme_codes_default_db = db.query(DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.SCRIP_KEY.in_(scheme_codes_from_file)).all()
    scheme_codes_default_db = pd.DataFrame(scheme_codes_default_db, columns=['SCRIP_KEY'])['SCRIP_KEY'].tolist()  # Flatten the list of tuples to list

    missing_scheme_codes = set(scheme_codes_from_file) - set(scheme_codes_default_db)
    if missing_scheme_codes:
        app_logger.error(f"PROSPECT MUTUAL FUNDS TRANSACTIONS UPLOAD | MISSING SCHEME CODES | SCHEME CODES NOT FOUND IN DATABASE")
        raise HTTPException(status_code=400, detail="Some Scheme Codes not found in Security Master Table")

    if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
        raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")

    if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Quantity should be of type int or float")

    if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
        raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
    
    if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Rate should be of type int or float")
    
    if excel_df['INVESTMENT_AMOUNT'].isnull().any() or excel_df['INVESTMENT_AMOUNT'].eq('').any():
        raise HTTPException(status_code=400, detail="Investment Amount should not be blank.")
    
    if not excel_df['INVESTMENT_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Investment Amount should be of type int or float")
    
    if excel_df['CURRENT_VALUE'].isnull().any() or excel_df['CURRENT_VALUE'].eq('').any() or excel_df['CURRENT_VALUE'].eq(0).any():
        raise HTTPException(status_code=400, detail="Current Value should not be blank or zero.")
    
    if not excel_df['CURRENT_VALUE'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Current Value should be of type int or float")
    
    excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)

    merged_excel_df = excel_df.copy()
    merged_excel_df['SCHEME_CODE'] = merged_excel_df['SCHEME_CODE'].astype(int)
    as_on_date = excel_df['AS_ON_DATE'].min()
    existing_data_db = db.query(
        ProspectFtHolding.CLIENT_CODE,                                 
        ProspectFtHolding.ADVISOR_NAME, 
        ProspectFtHolding.AS_ON_DATE, 
        ProspectFtHolding.SCHEME_CODE,  
        ProspectFtHolding.QUANTITY, 
        ProspectFtHolding.RATE, 
        ProspectFtHolding.INVESTMENT_AMOUNT, 
        ProspectFtHolding.CURRENT_VALUE
            ).filter(
                ProspectFtHolding.CLIENT_CODE == client_id, 
                ProspectFtHolding.SOURCE_ID == 3,
                ProspectFtHolding.AS_ON_DATE == as_on_date,
                ).all()
    if existing_data_db:
        existed_data = pd.DataFrame(existing_data_db, columns=['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'SCHEME_CODE', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE'])
        existed_data[['CLIENT_CODE']] = existed_data[['CLIENT_CODE']].apply(pd.to_numeric, errors='coerce', downcast='integer')
        existed_data['AS_ON_DATE'] = pd.to_datetime(existed_data['AS_ON_DATE'], errors="coerce")
        existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
        existed_data['ADVISOR_NAME'] = existed_data['ADVISOR_NAME'].astype(str)
        comparison_columns = ['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'SCHEME_CODE', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']
        existed_data['SCHEME_CODE'] = existed_data['SCHEME_CODE'].astype(int)
        duplicates = pd.merge(merged_excel_df, existed_data, on=comparison_columns, how='inner')
        merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'REMARK'] = 'Duplicate record found in the database'
        merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'STATUS'] = 'Duplicate'       


    zero_value_rows = (merged_excel_df['QUANTITY'] == 0) & (merged_excel_df['RATE'] == 0) & (merged_excel_df['INVESTMENT_AMOUNT'] == 0)
    merged_excel_df.loc[zero_value_rows, 'STATUS'] = "Failure"
    merged_excel_df.loc[zero_value_rows, 'REMARK'] = 'All values are zero'
    merged_excel_df['CREATED_BY_ID'] = user_id
    merged_excel_df['SOURCE_ID'] = 3
    merged_excel_df['SOURCE_TABLE'] = "DM_SECURITY_MASTER"
    merged_excel_df['UPDATED_BY_ID'] = user_id
    merged_excel_df['CREATED_DATE'] = datetime.now()
    merged_excel_df['UPDATED_DATE'] = datetime.now()
    merged_excel_df['REMARK'] = merged_excel_df['REMARK'].astype(str) 
    merged_excel_df['FILE_NAME'] = file_name 
    merged_excel_df['REMARK'].replace({"nan":None}, inplace=True)

    # Save to database
    data = excel_df.to_dict(orient='records')
    response, message = await upload_bulk_file(merged_excel_df, 'PROSPECT_FT_HOLDING')
    if not response:
        app_logger.error(f"PROSPECT MUTUAL FUNDS UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
        raise HTTPException(status_code=400, detail="Error while saving the data")            
    app_logger.info(f"Mutual funds transactions | uploaded successfully | client {client_id}")
    return {"message": "File Uploaded Successfully"}


@prospect_upload_router.post("/holdings/other_products/", dependencies=[Depends(JWTBearer())])
async def upload_other_products(
    header_request: Request, 
    client_id: int = Form(...), 
    file: UploadFile = File(...), 
    db: Session = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        raise HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        raise HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        raise HTTPException(status_code=403, detail=msg)
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")

    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB.")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")
    file_name = None
    # Read the uploaded file into a DataFrame
    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        file_name = file.filename
    except Exception as e:
        app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    excel_df.columns = excel_df.columns.str.strip()
    required_columns = ['Client Code', 'Advisor Name', 'Scheme Code', 'Quantity', 'Rate', 'Investment Amount', 'Current Value', 'As On Date']
    missing_columns = [col for col in required_columns if col not in excel_df.columns]
    if missing_columns:
        app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
    
    excel_df.rename(columns={
        'Client Code': 'CLIENT_CODE',
        'Advisor Name': 'ADVISOR_NAME',
        'Scheme Code': 'SCHEME_CODE',
        'Quantity': 'QUANTITY',
        'Rate': 'RATE',
        'Folio No.': 'FOLIO_NO',
        'Investment Amount': 'INVESTMENT_AMOUNT',
        'Current Value': 'CURRENT_VALUE',
        'As On Date': 'AS_ON_DATE'
    }, inplace=True)

    excel_df['STATUS'] = 'Success'
    excel_df["FILE_NAME"] = file.filename

    if excel_df['CLIENT_CODE'].isnull().any():
        raise HTTPException(status_code=400, detail="Client Code should not be blank")

    if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
        raise HTTPException(status_code=400, detail="Client Code should be an integer")
    
    if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
        raise HTTPException(status_code=400, detail="Client Code should not be greater than 20 digits")
    
    if len(excel_df['CLIENT_CODE'].unique()) > 1:
        raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

    if client_id not in excel_df['CLIENT_CODE'].unique():
        raise HTTPException(status_code=400, detail="Client Code does not match with the selected Client")
    
    if excel_df['ADVISOR_NAME'].isnull().any() or excel_df['ADVISOR_NAME'].str.strip().eq('').any():
        raise HTTPException(status_code=400, detail="Advisor Name should not be blank")

    if excel_df['ADVISOR_NAME'].str.len().max() > 100:
        raise HTTPException(status_code=400, detail="Advisor Name should not be greater than 100 characters")

    if excel_df['FOLIO_NO'].isnull().any():
        app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | MISSING FOLIO NO. | FOLIO NO. COLUMN IS EMPTY")
        raise HTTPException(status_code=400, detail="Folio No. column cannot be empty.")

    if excel_df['AS_ON_DATE'].isnull().any():
        app_logger.error(f"PROSPECT OTHER PRODUCTS HOLDINGS UPLOAD | MISSING AS ON DATE | AS ON DATE COLUMN IS EMPTY")
        raise HTTPException(status_code=400, detail="As On Date should not be blank")
    
    try:
        excel_df['AS_ON_DATE'] = pd.to_datetime(excel_df['AS_ON_DATE'], errors='raise')
    except ValueError:
        raise HTTPException(status_code=400, detail="As On Date contains invalid date values")
    
    if (excel_df['AS_ON_DATE'] > pd.Timestamp.now()).any():
        app_logger.error(f"PROSPECT OTHER PRODUCTS HOLDINGS UPLOAD | INVALID AS ON DATE | AS ON DATE SHOULD NOT BE GREATER THAN CURRENT DATE")
        raise HTTPException(status_code=400, detail="As On Date should not be greater than the current date")
    
    
    if excel_df['QUANTITY'].isnull().any() or excel_df['QUANTITY'].eq('').any() or (excel_df['QUANTITY'] == 0).any():
        raise HTTPException(status_code=400, detail="Quantity should not be blank or 0")

    if not excel_df['QUANTITY'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Quantity should be of type int or float")

    if excel_df['RATE'].isnull().any() or excel_df['RATE'].eq('').any() or (excel_df['RATE'] == 0).any():
        raise HTTPException(status_code=400, detail="Rate should not be blank or 0")
    
    if not excel_df['RATE'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Rate should be of type int or float")
    
    if excel_df['INVESTMENT_AMOUNT'].isnull().any() or excel_df['INVESTMENT_AMOUNT'].eq('').any():
        raise HTTPException(status_code=400, detail="Investment Amount should not be blank.")
    
    if not excel_df['INVESTMENT_AMOUNT'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Investment Amount should be of type int or float")
    
    if excel_df['CURRENT_VALUE'].isnull().any() or excel_df['CURRENT_VALUE'].eq('').any() or excel_df['CURRENT_VALUE'].eq(0).any():
        raise HTTPException(status_code=400, detail="Current Value should not be blank or zero")
    
    if not excel_df['CURRENT_VALUE'].apply(lambda x: isinstance(x, (int, float))).all():
        raise HTTPException(status_code=400, detail="Current Value should be of type int or float")

    excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = excel_df[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
    
    unique_scheme_codes = excel_df['SCHEME_CODE'].unique().tolist()
    # Fetching scheme_code data from DMSecurityMaster and ProspectSecurityMaster
    security_scheme_code = db.query(
        DMSecurityMaster.SCRIP_KEY
        ).filter(
            # DMSecurityMaster.ASSET_CLASS == "Equity",
            func.trim(DMSecurityMaster.SCRIP_KEY).in_(unique_scheme_codes),
            DMSecurityMaster.SOURCE_ID == 5
        ).all()
    # security_scheme_code = db.query(DMSecurityMaster.scheme_code, DMSecurityMaster.SCRIP_KEY).filter(DMSecurityMaster.ASSET_CLASS == "Equity").all()
    secmaster_excel_df = pd.DataFrame(security_scheme_code, columns=['SCHEME_CODE']).drop_duplicates()
    excel_df1 = pd.merge(excel_df, secmaster_excel_df, how='right', on='SCHEME_CODE')

    # excel_df1 = sec_scheme_code_excel_df.dropna(subset=['SCHEME_CODE']).copy()
    excel_df1['STATUS'] = "Success"
    excel_df1["SOURCE_TABLE"] = "DM_SECURITY_MASTER"

    excel_df2 = excel_df[~excel_df['SCHEME_CODE'].isin(secmaster_excel_df["SCHEME_CODE"].drop_duplicates().to_list())].copy()
    excel_df2['REMARK'] = 'SCHEME CODE not found in Security Master Table'
    excel_df2['STATUS'] = "Failure"

    prospect_scheme_code = db.query(ProspectSecurityMaster.SCHEME_CODE).filter(ProspectSecurityMaster.SOURCE_ID == 5).all()
    prospect_data = pd.DataFrame()  # Initialize prospect_data as an empty DataFrame
    if prospect_scheme_code:
        prospect_scheme_code_excel_df = pd.DataFrame(prospect_scheme_code, columns=['SCHEME_CODE'])
        prospect_data = pd.merge(excel_df2, prospect_scheme_code_excel_df, how='left', on='SCHEME_CODE', indicator=True)
        prospect_data = prospect_data[prospect_data['_merge'] == 'both']
        prospect_data['STATUS'] = "Success"
        prospect_data['REMARK'] = ''
        prospect_data["SOURCE_TABLE"] = "PROSPECT_SECURITY_MASTER"

        prospect_data.drop('_merge', axis=1, inplace=True)
        excel_df2 = excel_df2[~excel_df2['SCHEME_CODE'].isin(prospect_data['SCHEME_CODE'])]

    merged_excel_df = pd.concat([excel_df1, excel_df2, prospect_data], ignore_index=True)
    
    as_on_date = excel_df['AS_ON_DATE'].min()
    existing_data_db = db.query(
        ProspectFtHolding.CLIENT_CODE,                                 
        ProspectFtHolding.ADVISOR_NAME, 
        ProspectFtHolding.AS_ON_DATE, 
        ProspectFtHolding.SCHEME_CODE,  
        ProspectFtHolding.QUANTITY, 
        ProspectFtHolding.RATE, 
        ProspectFtHolding.INVESTMENT_AMOUNT, 
        ProspectFtHolding.CURRENT_VALUE
            ).filter(
                ProspectFtHolding.CLIENT_CODE == client_id, 
                ProspectFtHolding.SOURCE_ID == 5,
                ProspectFtHolding.AS_ON_DATE == as_on_date,
                ).all()
    if existing_data_db:
        existed_data = pd.DataFrame(existing_data_db, columns=['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'SCHEME_CODE', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE'])
        existed_data[['CLIENT_CODE']] = existed_data[['CLIENT_CODE']].apply(pd.to_numeric, errors='coerce', downcast='integer')
        existed_data['AS_ON_DATE'] = pd.to_datetime(existed_data['AS_ON_DATE'], errors="coerce")
        existed_data['SCHEME_CODE'] = existed_data['SCHEME_CODE'].astype(int)
        existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']] = existed_data[['QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']].astype(float).round(8)
        existed_data['ADVISOR_NAME'] = existed_data['ADVISOR_NAME'].astype(str)
        comparison_columns = ['CLIENT_CODE', 'ADVISOR_NAME', 'AS_ON_DATE', 'SCHEME_CODE', 'QUANTITY', 'RATE', 'INVESTMENT_AMOUNT', 'CURRENT_VALUE']
        duplicates = pd.merge(merged_excel_df, existed_data, on=comparison_columns, how='inner')
        merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'REMARK'] = 'Duplicate record found in the database'
        merged_excel_df.loc[merged_excel_df.index.isin(duplicates.index), 'STATUS'] = 'Duplicate'       

    zero_value_rows = (merged_excel_df['QUANTITY'] == 0) & (merged_excel_df['RATE'] == 0) & (merged_excel_df['INVESTMENT_AMOUNT'] == 0)
    merged_excel_df.loc[zero_value_rows, 'STATUS'] = "Failure"
    merged_excel_df.loc[zero_value_rows, 'REMARK'] = 'All values are zero'
    merged_excel_df['CREATED_BY_ID'] = user_id
    merged_excel_df['SOURCE_ID'] = 5
    # merged_excel_df['SOURCE_TABLE'] = "DM_SECURITY_MASTER"
    merged_excel_df['UPDATED_BY_ID'] = user_id
    merged_excel_df['CREATED_DATE'] = datetime.now()
    merged_excel_df['UPDATED_DATE'] = datetime.now()
    merged_excel_df['REMARK'] = merged_excel_df['REMARK'].astype(str) 
    merged_excel_df['REMARK'].replace({"nan":None}, inplace=True) 
    merged_excel_df['FILE_NAME'] = file_name

    # Save to database
    # excel_df["SOURCE_ID"] = 5
    data = excel_df.to_dict(orient='records')
    response, message = await upload_bulk_file(merged_excel_df, 'PROSPECT_FT_HOLDING')
    if not response:
        app_logger.error(f"PROSPECT OTHER PRODUCTS UPLOAD | ERROR WHILE SAVING THE DATA | {message}")
        raise HTTPException(status_code=400, detail="Error while saving the data")
    app_logger.info(f"Other products | uploaded successfully | client {client_id}")
    return {"message": "File Uploaded Successfully"}


@prospect_upload_router.post("/holdings/insurance/ulip/", dependencies=[Depends(JWTBearer())])
async def upload_insurance_holdings_ulip(
    header_request: Request,
    client_id: int = Form(...), 
    file: UploadFile = File(...),  
    db = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        return HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        return HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)
    status_value = 200
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    
    # Read the file into memory
    file.file.seek(0)  # Reset file pointer to the beginning
    file_size = await file.read()
    if len(file_size) > 50 * 1024 * 1024:  # 50 MB
        raise HTTPException(status_code=400, detail="The file size should not exceed 50 MB")
    
    client_exist = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE == client_id).first()
    if not client_exist:
        raise HTTPException(status_code=400, detail="Client Code does not exist in Prospect Client Master")
    
    try:
        file.file.seek(0)
        file_bytes = await file.read()  # Read the file into memory
        excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
        excel_df.columns = excel_df.columns.str.strip()
    except Exception as e:
        app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | ERROR WHILE READING THE FILE | {e}")
        raise HTTPException(status_code=400, detail="Error while reading the file")
    
    
    if excel_df.empty:
        raise HTTPException(status_code=400, detail="Uploaded File is Empty")

    # excel_df['FILE_NAME'] = file.filename
    excel_df.columns = excel_df.columns.str.strip()
    required_columns = ['Client Code','Insurance Company', 'Insured Person', 'Sum Assured', 'Policy Code', 'Policy Name', 
                    'Policy Number', 'Policy Start Date', 'Maturity Date', 'Policy Tenure', 'Premium Amount', 
                    'Frequency', 'Annual Premium', 'FundValue', 'FundValue As On Date']

    missing_columns = [col for col in required_columns if col not in excel_df.columns]
    if missing_columns:
        app_logger.error(f"PROSPECT INSURANCE ULIP UPLOAD | MISSING COLUMNS | Missing columns: {', '.join(missing_columns)}")
        raise HTTPException(status_code=400, detail=f"Missing columns: {', '.join(missing_columns)}")
   														
    if not excel_df.empty:
        excel_df.rename(columns={
            'Client Code':'CLIENT_CODE', 
            'Insurance Company':'INSURANCE_COMPANY',
            'Insured Person':'INSURED_PERSON',
            'Sum Assured':"SUM_ASSURED",
            'Policy Code':'POLICY_CODE',
            'Policy Name':'POLICY_NAME',
            'Policy Number':'POLICY_NUMBER',
            'Policy Start Date':'POLICY_START_DATE',
            'Maturity Date':'MATURITY_DATE',
            'Policy Tenure':'POLICY_TENURE',
            'Premium Amount':'PREMIUM_AMOUNT',
            'Frequency':'FREQUENCY',
            'Annual Premium':'ANNUAL_PREMIUM',
            'FundValue':'FUND_VALUE',
            'FundValue As On Date':'FUND_VALUE_AS_ON_DATE',
            }, inplace=True)
        
        # # Validating mandatory columns
        # mandatory_columns = ['CLIENT_CODE', 'INSURANCE_COMPANY', 'INSURED_PERSON', 'SUM_ASSURED', 'POLICY_CODE',
        #                      'POLICY','POLICY_NUMBER', 'POLICY_START_DATE', 'MATURITY_DATE','POLICY_TENURE',
        #                      'PREMIUM_AMOUNT','FREQUENCY','ANNUAL_PREMIUM','FUND_VALUE','FUND_VALUE_AS_ON_DATE ',]
        # # if excel_df[mandatory_columns].isnull().any().any():
        # #     raise HTTPException(status_code=400, detail='One or more mandatory fields are missing')
        # missing_columns = excel_df[mandatory_columns].columns[excel_df[mandatory_columns].isnull().any()].tolist()
        # if missing_columns:
        #     raise HTTPException(status_code=400, detail=f"One or more mandatory fields are missing: {', '.join(missing_columns)}")
        
        excel_df['STATUS'] = 'Success'
        excel_df['POLICY_TYPE'] = 'Ulip'
        excel_df['FILE_NAME'] =  file.filename
        
        if excel_df['CLIENT_CODE'].isnull().any():
            raise HTTPException(status_code=400, detail="Client Code should not be blank")
    
        if not excel_df['CLIENT_CODE'].apply(lambda x: isinstance(x, int)).all():
            raise HTTPException(status_code=400, detail="Client Code must be an integer.")
        
        if not excel_df['CLIENT_CODE'].apply(lambda x: len(str(x)) <= 20).all():
            raise HTTPException(status_code=400, detail="Client Code must not be greater than 20 digits.")
        
        if len(excel_df['CLIENT_CODE'].unique()) > 1:
            raise HTTPException(status_code=400, detail="Multiple Client Code found in the uploaded file")

        if client_id not in excel_df['CLIENT_CODE'].unique():
            raise HTTPException(status_code=400, detail="Client Code not found in the uploaded file")
 
        if excel_df['POLICY_CODE'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY_CODE | POLICY_CODE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Policy Code column cannot be empty.")
        
        if excel_df['POLICY_NUMBER'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY_CODE | POLICY_CODE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Policy Code column cannot be empty.")
        
        if excel_df['INSURANCE_COMPANY'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING INSURANCE COMPANY | INSURANCE COMPANY COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Insurance Company column cannot be empty.")
        
        if excel_df['INSURED_PERSON'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING INSURED_PERSON | INSURED_PERSON COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Insured Person column cannot be empty.")
        
        # if excel_df['POLICY'].isnull().any():
        #     app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY NAME | POLICY NAME COLUMN IS EMPTY")
        #     raise HTTPException(status_code=400, detail="Policy Name column cannot be empty.")
        if excel_df['POLICY_START_DATE'].isnull().any():
            raise HTTPException(status_code=400, detail="Policy Start Date should not be blank")
        
        try:
            excel_df['POLICY_START_DATE'] = pd.to_datetime(excel_df['POLICY_START_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Policy Start Date contains invalid date values.") 
        
        if (excel_df['POLICY_START_DATE'] > pd.Timestamp.now()).any():
            raise HTTPException(status_code=400, detail="Policy Start Date should not be greater than the current date.")
        
        if excel_df["MATURITY_DATE"].isnull().any():
        # if excel_df["PAYMENT_DATE"].isnull().any() or (excel_df["PAYMENT_DATE"] >= datetime.now().date()).any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING MATURITY_DATE | MATURITY_DATE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Maturity Date column cannot be empty.")
        
                
        try:
            excel_df['MATURITY_DATE'] = pd.to_datetime(excel_df['MATURITY_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Maturity Date contains invalid date values.")
        
        if excel_df['FUND_VALUE_AS_ON_DATE'].isnull().any():
            raise HTTPException(status_code=400, detail="Fund Value As On Date should not be blank")
        
        # if excel_df["FUND_VALUE_AS_ON_DATE"].isnull().any() or (excel_df["FUND_VALUE_AS_ON_DATE"] >= datetime.now().date()).any():
        #     app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING FUND_VALUE_AS_ON_DATE | FUND_VALUE_AS_ON_DATE COLUMN IS EMPTY")
        #     raise HTTPException(status_code=400, detail="Fund Value As On Date column cannot be empty.")
        
        try:
            excel_df['FUND_VALUE_AS_ON_DATE'] = pd.to_datetime(excel_df['FUND_VALUE_AS_ON_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="FundValue As On Date contains invalid date values.")
        
        if (excel_df['FUND_VALUE_AS_ON_DATE'] > pd.Timestamp.now()).any():
            raise HTTPException(status_code=400, detail="FundValue As On Date should not be greater than the current date.")
            
        
        
        # Clean and validate the FREQUENCY column
        # excel_df['FREQUENCY'] = excel_df['FREQUENCY'].str.strip().str.title()
        # valid_frequencies = ['Daily', 'Weekly','Monthly','Quarterly','Fortnightly','Yearly']
        # if not excel_df['FREQUENCY'].isin(valid_frequencies).all():
        #     app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID FREQUENCY TYPE | FOUND INVALID FREQUENCY TYPES")
        #     raise HTTPException(status_code=400, detail=f"Invalid frequency types found.")

        
        if not excel_df["SUM_ASSURED"].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Sum Assured' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Sum Assured must be number.")
        if not excel_df["PREMIUM_AMOUNT"].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Premium Amount' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Premium Amount must be number.")
        if not excel_df['ANNUAL_PREMIUM'].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Annual Premium' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Annual Premium must be number.")
        if not excel_df['FUND_VALUE'].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Fundvalue' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Fundvalue must be number.")
       
        excel_df["POLICY_CODE"] = excel_df["POLICY_CODE"].astype(str)
        excel_df["POLICY_NUMBER"] = excel_df["POLICY_NUMBER"].astype(str)
        excel_df[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'ANNUAL_PREMIUM', 'FUND_VALUE']] = excel_df[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'ANNUAL_PREMIUM', 'FUND_VALUE']].astype(float).round(8)
        # Check if the Policy codes in the uploaded file are present in the database
        policy_code_from_file = excel_df['POLICY_CODE'].unique()
        policy_code_default_db = db.query(TblFpInsurancePolicy.POLICY_CODE).filter(TblFpInsurancePolicy.POLICY_CODE.in_(policy_code_from_file)).all()
        policy_code_default_db = pd.DataFrame(policy_code_default_db, columns=['POLICY_CODE'])['POLICY_CODE'].tolist()  # Flatten the list of tuples to list
        policy_code_from_db = db.query(ProspectInsurancePolicyMaster.POLICY_CODE).filter(ProspectInsurancePolicyMaster.POLICY_CODE.in_(policy_code_from_file)).all()
        policy_code_from_db = pd.DataFrame(policy_code_from_db, columns=['POLICY_CODE'])['POLICY_CODE'].tolist()  # Flatten the list of tuples to list

        missing_policy_number = set(policy_code_from_file) - set(policy_code_from_db) - set(policy_code_default_db)
        if missing_policy_number:
            excel_df.loc[excel_df['POLICY_CODE'].isin(missing_policy_number), 'STATUS'] = 'Failure'
            excel_df.loc[excel_df['POLICY_CODE'].isin(missing_policy_number), 'REMARK'] = 'Policy Code not found in Policy Master Table'
        
        start_date = excel_df['POLICY_START_DATE'].min()
        end_date = excel_df['POLICY_START_DATE'].max()
        # excel_df.drop(columns=['POLICY_NAME'], inplace=True)
        
        get_data_for_client = db.query(ProspectInsuranceHolding.CLIENT_CODE,
                                       ProspectInsuranceHolding.INSURANCE_COMPANY,
                                       ProspectInsuranceHolding.INSURED_PERSON,
                                       ProspectInsuranceHolding.POLICY_CODE,
                                       ProspectInsuranceHolding.POLICY_NUMBER ,
                                       ProspectInsuranceHolding.SUM_ASSURED,
                                       ProspectInsuranceHolding.PREMIUM_AMOUNT,
                                       ProspectInsuranceHolding.ANNUAL_PREMIUM,
                                       ProspectInsuranceHolding.FUND_VALUE,
                                       ProspectInsuranceHolding.POLICY_START_DATE,
                                       ProspectInsuranceHolding.FUND_VALUE_AS_ON_DATE,
                                       ).filter(ProspectInsuranceHolding.CLIENT_CODE == client_id,
                    ProspectInsuranceHolding.POLICY_START_DATE >= start_date,
                    ProspectInsuranceHolding.POLICY_START_DATE <= end_date).all()
        existing_records = pd.DataFrame.from_records(get_data_for_client, columns=['CLIENT_CODE', 'INSURANCE_COMPANY',
                'INSURED_PERSON', 'POLICY_CODE', 'POLICY_NUMBER', 'SUM_ASSURED','PREMIUM_AMOUNT', 
                'ANNUAL_PREMIUM', 'FUND_VALUE','POLICY_START_DATE','FUND_VALUE_AS_ON_DATE'])

        if not existing_records.empty:
            existing_records['CLIENT_CODE'] = existing_records['CLIENT_CODE'].astype(int)
            existing_records[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'ANNUAL_PREMIUM', 'FUND_VALUE']] = existing_records[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'ANNUAL_PREMIUM', 'FUND_VALUE']].astype(float).round(8)
            # Merge existing records with uploaded data to find duplicates
            existing_records["POLICY_CODE"] = existing_records["POLICY_CODE"].astype(str)
            existing_records["POLICY_NUMBER"] = existing_records["POLICY_NUMBER"].astype(str)
            merged_df = pd.merge(excel_df, existing_records, on=['CLIENT_CODE', 'INSURED_PERSON', 'POLICY_CODE', 'POLICY_NUMBER', 'SUM_ASSURED', 
                        'PREMIUM_AMOUNT', 'ANNUAL_PREMIUM','FUND_VALUE'], how='inner')
            if not merged_df.empty:
                # Mark duplicates in the uploaded data
                excel_df.loc[excel_df.index.isin(merged_df.index), 'STATUS'] = 'Duplicate'
                excel_df.loc[excel_df.index.isin(merged_df.index), 'REMARK'] = 'Duplicate record found in the database'

        # excel_df["SOURCE_TABLE"] = "DM_MUTUALFUND"
        excel_df["CREATED_BY_ID"]  = user_id
        excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = datetime.now()
        excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_INSURANCE_HOLDING')
        if not response:
            raise HTTPException(status_code=400, detail="Error while uploading the file in database")

    app_logger.info("PROSPECT INSURANCE ULIP HOLDING UPLOAD | NO ERROR FOUND | UPLOADED SUCESSFULLY")
    return {"message":"File Uploaded Successfully"}



@prospect_upload_router.post("/holdings/insurance/non_ulip/", dependencies=[Depends(JWTBearer())])
async def upload_insurance_holdings_non_ulip(
    header_request: Request,
    client_id: int = Form(...), 
    file: UploadFile = File(...),  
    db = Depends(get_db)):
    token_header = header_request.headers.get("Authorization")
    token_list = token_header.split(" ")
    token_data = await decodeJWT(token_list[1])
    if not token_data:
        return HTTPException(status_code=403, detail="Please log in to access")
    user_id = token_data.get('userid', None)
    if not user_id:
        return HTTPException(status_code=403, detail="Please log in to access")
    has_permission, msg = await check_if_permission(db, user_id, "prospect_client")
    if not has_permission:
        return HTTPException(status_code=403, detail=msg)
    status_value = 200
    if file.content_type != 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload an xlsx file.")
    file.file.seek(0)
    file_bytes = await file.read()  # Read the file into memory
    excel_df = pd.read_excel(BytesIO(file_bytes))  # Create a BytesIO object and read it with pandas
    updated_record_len = 0
    input_data_length = len(excel_df)
    excel_df.columns = excel_df.columns.str.strip()
												
    if not excel_df.empty:
        excel_df.rename(columns={
            'Client Code':'CLIENT_CODE', 
            'Insurance Company':'INSURANCE_COMPANY',
            'Insured Person':'INSURED_PERSON',
            'Sum Assured':"SUM_ASSURED",
            'Policy Code':'POLICY_CODE',
            'Policy Name':'POLICY_NAME',
            'Policy Number':'POLICY_NUMBER',
            'Policy Start Date':'POLICY_START_DATE',
            'Maturity Date':'MATURITY_DATE',
            'Policy Tenure':'POLICY_TENURE',
            'Premium Paying Term':'PREMIUM_PAYING_TERM',
            'Premium Amount':'PREMIUM_AMOUNT',
            'Frequency':'FREQUENCY',
            'Fund Value':'FUND_VALUE',
            'FundValue As On Date':'FUND_VALUE_AS_ON_DATE',
            }, inplace=True)
        
        # Validating mandatory columns
        mandatory_columns = ['CLIENT_CODE', 'INSURANCE_COMPANY', 'INSURED_PERSON', 'SUM_ASSURED', 'POLICY_CODE',
                             'POLICY_NAME','POLICY_NUMBER', 'POLICY_START_DATE', 'MATURITY_DATE','POLICY_TENURE','PREMIUM_PAYING_TERM',
                             'PREMIUM_AMOUNT','FREQUENCY','FUND_VALUE','FUND_VALUE_AS_ON_DATE']
        # if excel_df[mandatory_columns].isnull().any().any():
        #     raise HTTPException(status_code=400, detail='One or more mandatory fields are missing')
        missing_columns = excel_df[mandatory_columns].columns[excel_df[mandatory_columns].isnull().any()].tolist()
        if missing_columns:
            raise HTTPException(status_code=400, detail=f"One or more mandatory fields are missing: {', '.join(missing_columns)}")
        excel_df['STATUS'] = 'Success'
        excel_df['POLICY_TYPE'] = 'Non Ulip'
        excel_df["FILE_NAME"] = file.filename
        unique_client_codes = excel_df['CLIENT_CODE'].unique()
        if len(unique_client_codes) > 1 or unique_client_codes[0] != client_id:
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID CLIENT CODE | ALL RECORDS MUST HAVE THE SAME CLIENT CODE AND MUST MATCH THE PROVIDED CLIENT ID")
            raise HTTPException(status_code=400, detail="All records must have the same client code and must match the provided client ID.")

        client_data_check = db.query(ProspectClientMaster.CLIENT_CODE).filter(ProspectClientMaster.CLIENT_CODE==unique_client_codes[0]).first()
        if not client_data_check:
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID CLIENT CODE | CLIENT CODE SHOULD BE AN INTEGER OR SHOULD EXIST IN PROSPECT CLIENT MASTER")
            raise HTTPException(status_code=400, detail="Client Code should be an integer or should exist in Prospect Client Master")
 
        if excel_df['POLICY_CODE'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY_CODE | POLICY_CODE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Policy Code column cannot be empty.")
        
        if excel_df['POLICY_NUMBER'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY_CODE | POLICY_CODE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Policy Code column cannot be empty.")
        
        if excel_df['INSURANCE_COMPANY'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING INSURANCE COMPANY | INSURANCE COMPANY COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Insurance Company column cannot be empty.")
        
        if excel_df['INSURED_PERSON'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING INSURED_PERSON | INSURED_PERSON COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Insured Person column cannot be empty.")
        
        if excel_df['PREMIUM_PAYING_TERM'].isnull().any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING PREMIUM_PAYING_TERM | PREMIUM_PAYING_TERM COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Premium Paying Term column cannot be empty.")
        
        # if excel_df['POLICY'].isnull().any():
        #     app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY NAME | POLICY NAME COLUMN IS EMPTY")
        #     raise HTTPException(status_code=400, detail="Policy Name column cannot be empty.")
        try:
            excel_df['POLICY_START_DATE'] = pd.to_datetime(excel_df['POLICY_START_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Policy Start Date contains invalid date values.")
        
        if excel_df["POLICY_START_DATE"].isnull().any() or (excel_df["POLICY_START_DATE"] >= Timestamp(datetime.now().date())).any():
        # if excel_df["PAYMENT_DATE"].isnull().any() or (excel_df["PAYMENT_DATE"] >= datetime.now().date()).any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING POLICY_START_DATE | POLICY_START_DATE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Policy Start Date column cannot be empty.")
        
        try:
            excel_df['MATURITY_DATE'] = pd.to_datetime(excel_df['MATURITY_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="Maturity Date contains invalid date values.")
        
        if excel_df["MATURITY_DATE"].isnull().any():
        # if excel_df["PAYMENT_DATE"].isnull().any() or (excel_df["PAYMENT_DATE"] >= datetime.now().date()).any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING MATURITY_DATE | MATURITY_DATE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Maturity Date column cannot be empty.")
        
        try:
            excel_df['FUND_VALUE_AS_ON_DATE'] = pd.to_datetime(excel_df['FUND_VALUE_AS_ON_DATE'], errors='raise')
        except ValueError:
            raise HTTPException(status_code=400, detail="FundValue As On Date contains invalid date values.")
        
        if excel_df["FUND_VALUE_AS_ON_DATE"].isnull().any() or (excel_df["FUND_VALUE_AS_ON_DATE"] >= pd.Timestamp.now()).any():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | MISSING FUNDVALUE_AS_ON_DATE | FUNDVALUE_AS_ON_DATE COLUMN IS EMPTY")
            raise HTTPException(status_code=400, detail="Fundvalue as on date column cannot be empty.")
        # Clean and validate the FREQUENCY column
        excel_df['FREQUENCY'] = excel_df['FREQUENCY'].str.strip().str.title()
        valid_frequencies = ['Daily', 'Weekly','Monthly','Quarterly','Fortnightly','Yearly']
        if not excel_df['FREQUENCY'].isin(valid_frequencies).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID FREQUENCY TYPE | FOUND INVALID FREQUENCY TYPES")
            raise HTTPException(status_code=400, detail=f"Invalid frequency types found.")

        
        if not excel_df["SUM_ASSURED"].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Sum Assured' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Sum Assured must be number.")
        if not excel_df["PREMIUM_AMOUNT"].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Premium Amount' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Premium Amount must be number.")
        if not excel_df['FUND_VALUE'].apply(lambda x: isinstance(x, (int, float))).all():
            app_logger.error(f"PROSPECT INSURANCE ULIP HOLDINGS UPLOAD | INVALID DATA TYPES | 'Fund Value' contains non-numeric values")
            raise HTTPException(status_code=400, detail="Fund Value must be number.")
        
        excel_df[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'FUND_VALUE']] = excel_df[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'FUND_VALUE']].astype(float).round(8)
        
        # Check if the Policy codes in the uploaded file are present in the database
        policy_code_from_file = excel_df['POLICY_CODE'].unique()
        policy_code_default_db = db.query(TblFpInsurancePolicy.POLICY_CODE).filter(TblFpInsurancePolicy.POLICY_CODE.in_(policy_code_from_file)).all()
        policy_code_default_db = pd.DataFrame(policy_code_default_db, columns=['POLICY_CODE'])['POLICY_CODE'].tolist()  # Flatten the list of tuples to list
        policy_code_from_db = db.query(ProspectInsurancePolicyMaster.POLICY_CODE).filter(ProspectInsurancePolicyMaster.POLICY_CODE.in_(policy_code_from_file)).all()
        policy_code_from_db = pd.DataFrame(policy_code_from_db, columns=['POLICY_CODE'])['POLICY_CODE'].tolist()  # Flatten the list of tuples to list

        missing_policy_number = set(policy_code_from_file) - set(policy_code_from_db) - set(policy_code_default_db)
        if missing_policy_number:
            excel_df.loc[excel_df['POLICY_CODE'].isin(missing_policy_number), 'STATUS'] = 'Failure'
            excel_df.loc[excel_df['POLICY_CODE'].isin(missing_policy_number), 'REMARK'] = 'Policy Code not found in Policy Master Table'
        
        start_date = excel_df['POLICY_START_DATE'].min()
        end_date = excel_df['POLICY_START_DATE'].max()
        # excel_df.drop(columns=['POLICY_NAME'], inplace=True)
        
        get_data_for_client = db.query(ProspectInsuranceHolding.CLIENT_CODE,
                                       ProspectInsuranceHolding.INSURANCE_COMPANY,
                                       ProspectInsuranceHolding.INSURED_PERSON,
                                       ProspectInsuranceHolding.POLICY_CODE,
                                       ProspectInsuranceHolding.POLICY_NUMBER ,
                                       ProspectInsuranceHolding.SUM_ASSURED,
                                       ProspectInsuranceHolding.PREMIUM_AMOUNT,
                                       ProspectInsuranceHolding.POLICY_TYPE,
                                       ProspectInsuranceHolding.FUND_VALUE,
                                       ProspectInsuranceHolding.POLICY_START_DATE,
                                       ProspectInsuranceHolding.FUND_VALUE_AS_ON_DATE,
                                       ).filter(ProspectInsuranceHolding.CLIENT_CODE == client_id,
                    ProspectInsuranceHolding.POLICY_START_DATE >= start_date,
                    ProspectInsuranceHolding.POLICY_START_DATE <= end_date).all()
        existing_records = pd.DataFrame.from_records(get_data_for_client, columns=['CLIENT_CODE', 'INSURANCE_COMPANY',
                'INSURED_PERSON', 'POLICY_CODE', 'POLICY_NUMBER', 'SUM_ASSURED','PREMIUM_AMOUNT','POLICY_TYPE',
                'FUND_VALUE','POLICY_START_DATE','FUND_VALUE_AS_ON_DATE'])

        if not existing_records.empty:
            existing_records['CLIENT_CODE'] = existing_records['CLIENT_CODE'].astype(int)
            existing_records[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'FUND_VALUE']] = existing_records[['SUM_ASSURED', 'PREMIUM_AMOUNT', 'FUND_VALUE']].astype(float).round(8)
            # Merge existing records with uploaded data to find duplicates
            merged_df = pd.merge(excel_df, existing_records, on=['CLIENT_CODE', 'INSURED_PERSON', 'POLICY_CODE', 'POLICY_NUMBER', 'SUM_ASSURED', 
                        'PREMIUM_AMOUNT', 'POLICY_TYPE','FUND_VALUE'], how='inner')
            if not merged_df.empty:
                # Mark duplicates in the uploaded data
                excel_df.loc[excel_df.index.isin(merged_df.index), 'STATUS'] = 'Duplicate'
                excel_df.loc[excel_df.index.isin(merged_df.index), 'REMARK'] = 'Duplicate record found in the database'
        # excel_df["SOURCE_TABLE"] = "DM_MUTUALFUND"
        excel_df["CREATED_BY_ID"] = user_id
        excel_df["UPDATED_BY_ID"] = user_id
        excel_df["CREATED_DATE"] = datetime.now()
        excel_df["UPDATED_DATE"] = datetime.now()
        excel_df.replace({np.nan: None, pd.NaT: None, "nan": None}, inplace=True)
        response, message = await upload_bulk_file(excel_df, 'PROSPECT_INSURANCE_HOLDING')
        if not response:
            raise HTTPException(status_code=400, detail="Error while uploading the file in database")

    app_logger.info("PROSPECT INSURANCE NON-ULIP HOLDINGS UPLOAD | NO ERROR FOUND | UPLOADED SUCESSFULLY")
    return {"message":"File Uploaded Successfully"}
