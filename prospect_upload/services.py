from models.models import AnswerList
import pandas as pd
import numpy as np
from utils.services import app_logger
import traceback
from sqlalchemy.orm import session
from models.models import DMSecurityMaster, DmAccount
from db_module.connection import engine

async def upload_bulk_file(data_df, table_name):
    try:
        data_df.to_sql(table_name, con=engine, if_exists='append',index=False)
        return True, "Data uploaded successfully"
    except Exception as e:
        return None, f"{e}"


async def process_pms_underlying_file(db, input_df:pd.DataFrame):
    security_master_df = await get_dm_security_master_scrip_data(db)
    if security_master_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    account_master_df = await get_dm_accounts_master_data(db)
    if account_master_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    isin_code_df = await get_dm_security_master_data(db)
    if isin_code_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Filter out rows where 'L4_account_code' is not in dm accounts and append to error_df
    try:
        # Filter out rows where 'centrum_scheme_code' is not numeric
        # Remove null columns first to error_df
        error_df = input_df[input_df['CLIENT_CODE'].isnull()]
        input_df = input_df[input_df['CLIENT_CODE'].notnull()]
        error_df["error_column"] = 'Centrum Scheme Code is null'
        if input_df.empty:
            return input_df, error_df
        # input_df["Centrum Scheme Code"] = input_df["Centrum Scheme Code"].astype(str)

        temp_error_df = input_df[~input_df['Centrum Scheme Code'].isin(security_master_df["Centrum Scheme Code"])]
        temp_error_df["error_column"] = 'Centrum Scheme Code not in DM security table'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['Centrum Scheme Code'].isin(security_master_df["Centrum Scheme Code"])]
        if input_df.empty:
            return input_df, error_df
        # Filter out rows where 'L4_account_code' is not numeric and append to error_df
        temp_error_df = input_df[input_df['L4 Account Code'].isnull()]
        input_df = input_df[input_df['L4 Account Code'].notnull()]
        temp_error_df["error_column"] = 'L4 Account Code is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        if input_df.empty:
            return input_df, error_df
    
        temp_error_df = input_df[~input_df['L4 Account Code'].isin(account_master_df["L4 Account Code"])]
        temp_error_df["error_column"] = 'L4 Account Code not in DM account table'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['L4 Account Code'].isin(account_master_df["L4 Account Code"])]
        if input_df.empty:
            return input_df, error_df
        
        #Filter out row where 'ISIN' column is not in specified format
        temp_error_df = input_df[input_df['ISIN'].isnull()]
        input_df = input_df[input_df['ISIN'].notnull()]
        temp_error_df["error_column"] = 'ISIN is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)

        if input_df.empty:
            return input_df, error_df        

        # input_df["ISIN"] = input_df["ISIN"].astype(str) 
        temp_error_df = input_df[~input_df['ISIN'].isin(isin_code_df["ISIN"])]
        temp_error_df["error_column"] = 'ISIN not in DM security table'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['ISIN'].isin(isin_code_df["ISIN"])]

        if input_df.empty:
            return input_df, error_df

        # Filter out rows where 'scrip_code' is not a string and append to error_df
        temp_error_df = input_df[input_df['Scrip Name'].isnull()]
        input_df = input_df[input_df['Scrip Name'].notnull()]
        temp_error_df["error_column"] = 'Scrip Name is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)

        if input_df.empty:
            return input_df, error_df
        input_df['Scrip Name'] = input_df['Scrip Name'].astype(str) 
        
        # Filter out rows where 'weight' is not numeric and append to error_df
        input_df['Weight'] = input_df['Weight'].astype(float, errors="ignore")
        temp_error_df = input_df[input_df['Weight'].isnull()]
        input_df = input_df[input_df['Weight'].notnull()]
        temp_error_df["error_column"] = 'Weight column is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        if input_df.empty:
            return input_df, error_df

        temp_error_df = input_df[~input_df['Weight'].apply(lambda x: isinstance(x, (int, float)))]
        temp_error_df["error_column"] = 'Weight is not float'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['Weight'].apply(lambda x: isinstance(x, (int, float)))]
        if input_df.empty:
            return input_df, error_df
        print("input_df",input_df, error_df)
        

        input_df['Weight'] = input_df['Weight'].astype(float, errors="ignore").round(8)


        # Filter out rows where 'Date' is not a valid date and append to error_df
        temp_error_df = input_df[input_df['Date'].isnull()]
        input_df = input_df[input_df['Date'].notnull()]
        temp_error_df["error_column"] = 'Date is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        if input_df.empty:
            return input_df, error_df

        # Filter out rows where 'Date' is not the last day of the month and append to error_df
        temp_error_df = input_df[~input_df['Date'].apply(lambda x: pd.to_datetime(x, format='%d-%m-%Y', errors='coerce').is_month_end)]
        temp_error_df["error_column"] = 'Date not last day of month or cell not in Date format'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['Date'].apply(lambda x: pd.to_datetime(x, format='%d-%m-%Y', errors='coerce').is_month_end)]
        # Group the duplicate rows in error_df and concatenate the error_column data with comma separation

        # Dropping duplicates while keeping the last occurrence and adding them to error_df
        duplicates = input_df[input_df.duplicated(subset=['Centrum Scheme Code', 'L4 Account Code', 'ISIN', 'Date'], keep='last')]
        duplicates["error_column"] = 'Duplicate rows based on Centrum Scheme Code, L4 Account Code, ISIN, Date'
        error_df = pd.concat([error_df, duplicates], axis=0, ignore_index=True)
        input_df = input_df.drop_duplicates(subset=['Centrum Scheme Code', 'L4 Account Code', 'ISIN', 'Date'], keep='last')

        return input_df, error_df
    except Exception as e:
        app_logger.trace(f"ERROR IN FILE PROCESSING | PMS UNDERLYING | {traceback.format_exc()}")
        return pd.DataFrame(), pd.DataFrame()



async def get_dm_security_master_data(db):
    try:
        query = db.query(DMSecurityMaster.SCRIP_KEY, 
                    DMSecurityMaster.ISIN, 
                    DMSecurityMaster.PRODUCT_NAME).all()
        dm_security_df = pd.DataFrame(query, columns=['Centrum Scheme Code', "ISIN", "Product Name"])
        return dm_security_df
    except Exception as e:
        app_logger.trace(f" ERROR WHILE FETCHING DM SECURITY | |{traceback.format_exc()}")
        return pd.DataFrame()


async def get_dm_security_master_scrip_data(db):
    try:
        query = db.query(DMSecurityMaster.SCRIP_KEY, 
                    DMSecurityMaster.ISIN, 
                    DMSecurityMaster.PRODUCT_NAME).filter(
                        DMSecurityMaster.SOURCE_ID == 5
                    ).all()
        dm_security_df = pd.DataFrame(query, columns=['Centrum Scheme Code', "ISIN", "Product Name"])
        return dm_security_df
    except Exception as e:
        app_logger.trace(f" ERROR WHILE FETCHING DM SECURITY | |{traceback.format_exc()}")
        return pd.DataFrame()


async def get_dm_accounts_master_data(db):
    try:
        query = db.query(DmAccount.ACCOUNT_CODE).filter(
                        DmAccount.ACCOUNT_STATUS.in_(["Active", "Newly Created"])
                    ).all()
        dm_account_df = pd.DataFrame(query, columns=['L4 Account Code'])
        return dm_account_df
    except Exception as e:
        app_logger.trace(f" ERROR WHILE FETCHING DM ACCOUNTS | |{traceback.format_exc()}")
        return pd.DataFrame()


async def process_pms_rolling_return_file(db, input_df:pd.DataFrame):
    security_master_df = await get_dm_security_master_data(db)
    if security_master_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    try:
        # Filter out rows where 'centrum_scheme_code' is not numeric
        # Remove null columns first to error_df
        error_df = input_df[input_df['Centrum Scheme Code'].isnull()]
        input_df = input_df[input_df['Centrum Scheme Code'].notnull()]
        error_df["error_column"] = 'Centrum Scheme Code is null'
        if input_df.empty:
            return input_df, error_df
        # input_df["Centrum Scheme Code"] = input_df["Centrum Scheme Code"].astype(str)

        temp_error_df = input_df[~input_df['Centrum Scheme Code'].isin(security_master_df["Centrum Scheme Code"])]
        temp_error_df["error_column"] = 'Centrum Scheme Code not in DM security table'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['Centrum Scheme Code'].isin(security_master_df["Centrum Scheme Code"])]
        if input_df.empty:
            return input_df, error_df
        
        input_df['AUM'] = input_df['AUM'].astype(float, errors="ignore")

        # Filter out rows where 'AUM' is not numeric and append to error_df
        temp_error_df = input_df[input_df['AUM'].isnull()]
        input_df = input_df[input_df['AUM'].notnull()]
        temp_error_df["error_column"] = 'AUM column is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        if input_df.empty:
            return input_df, error_df
        
        temp_error_df = input_df[~input_df['AUM'].apply(lambda x: isinstance(x, (int, float)))]
        temp_error_df["error_column"] = 'AUM is not in float format'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['AUM'].apply(lambda x: isinstance(x, (int, float)))]
        if input_df.empty:
            return input_df, error_df
        

        input_df['AUM'] = input_df['AUM'].astype(float, errors="ignore").round(8)      
        

        # Filter out rows where 'scrip_code' is not a string and append to error_df
        temp_error_df = input_df[input_df['Fund Manager'].isnull()]
        input_df = input_df[input_df['Fund Manager'].notnull()]
        temp_error_df["error_column"] = 'Fund Manager is null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        if input_df.empty:
            return input_df, error_df

        input_df['Fund Manager'] = input_df['Fund Manager'].astype(str) 

        columns_to_filter = ['3m Ret', '6m Ret', '1Y Ret', '3Y Ret', 'PE', 'PB']
        for column in columns_to_filter:
            # Filter out rows where column is not numeric and append to error_df
            temp_error_df = input_df[input_df[column].isnull()]
            

            temp_error_df = input_df[~input_df[column].apply(lambda x: pd.isna(x) or isinstance(x, float))]
            temp_error_df["error_column"] = f"{column} is in wrong format"
            error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
            input_df = input_df[input_df[column].apply(lambda x: pd.isna(x) or isinstance(x, float))]
            # Round column to 2 decimal places
            input_df[column] = input_df[column].astype(float, errors="ignore").round(8)
            
        if input_df.empty:
            return input_df, error_df

        # Filter out rows where 'Date' is not a valid date and append to error_df
        temp_error_df = input_df[input_df['Date'].isnull()]
        input_df = input_df[input_df['Date'].notnull()]
        temp_error_df["error_column"] = 'Date column null'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        if input_df.empty:
            return input_df, error_df
        
        # Filter out rows where 'Date' is not the last day of the month and append to error_df
        temp_error_df = input_df[~input_df['Date'].apply(lambda x: pd.to_datetime(x, format='%d-%m-%Y', errors='coerce').is_month_end)]
        temp_error_df["error_column"] = 'Date not last day of month or cell not in date format'
        error_df = pd.concat([error_df, temp_error_df], axis=0, ignore_index=True)
        input_df = input_df[input_df['Date'].apply(lambda x: pd.to_datetime(x, format='%d-%m-%Y', errors='coerce').is_month_end)]
        # Group the duplicate rows in error_df and concatenate the error_column data with comma separation
        return input_df, error_df
    except Exception as e:
        app_logger.trace(f"ERROR IN FILE PROCESSING | PMS ROLLING RETURN | {traceback.format_exc()}")
        return pd.DataFrame(), pd.DataFrame()
