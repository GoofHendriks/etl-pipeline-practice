import pandas as pd
import logging

def clean(raw_df: pd.DataFrame) -> pd.DataFrame :
    """
    Cleans raw e-commerce order data by handling missing values, 
    data type conversions, and indexing.

    Args:
        raw_df (pd.DataFrame): The raw DataFrame extracted from the CSV file.

    Returns:
        pd.DataFrame: A cleaned DataFrame with:
            - Rows removed where 'order_amount' is missing or zero.
            - 'order_amount' converted to integer type.
            - 'order_date' converted to datetime, dropping invalid dates.
            - 'order_id' set as the index.
    """
    logging.info(f"Cleaning raw_df starting with {len(raw_df)} rows.")
    
    try:
        # Drop rows with missing or zero order_amount
        df_clean = raw_df.dropna(subset=["order_amount"])
        df_clean = df_clean[df_clean["order_amount"] != 0]
        df_clean["order_amount"] = df_clean["order_amount"].astype(int)

        # Convert order_date to datetime, drop rows with invalid dates
        df_clean["order_date"] = pd.to_datetime(df_clean["order_date"], errors="coerce")
        df_clean = df_clean.dropna(subset=["order_date"])

        # Set order_id as the index
        df_clean = df_clean.set_index('order_id')

        logging.info(f"Done cleaning data: {len(df_clean)} rows left after cleaning.")

        return df_clean
    
    except Exception as e:
        logging.error(f"unexpected error during data cleaning: {e}")
        return None

def aggregate(df_clean: pd.DataFrame) -> pd.DataFrame:
    """"
    aggregated clean dataset:
    - aggregates total amount spent and number of orders by customer
    - adds a vip_customer ("total_spent" > 1000) collumn

    args:
        df_clean (pd.DataFrame): cleaned dataset
    returns:
        df_aggregated (pd.DataFrame): aggregated dataset
    """

    logging.info(f"Transforming clean data starting rows: {len(df_clean)}.")
    
    try:
        df_aggregated = (
            df_clean.groupby(["customer_id", "customer_name"])
            .agg(
                total_spent=("order_amount", "sum"),
                order_count=("order_amount", "count")
            )
        )
        
        df_aggregated['vip_customer'] = df_aggregated["total_spent"] >= 1000

        df_aggregated = df_aggregated.reset_index()

        logging.info(f'Done transforming data, number of rows after transformation: {len(df_aggregated)}.')
        return df_aggregated
    
    except Exception as e:
        logging.error(f"Unexpected error during aggregationn of data: {e}")
        return None

def transform(raw_df: pd.DataFrame) -> pd.DataFrame:
    """
    cleans and aggregates raw data

    args:
        raw_df (pd.DataFrame): df with raw data
    
    returns:
        df_transformed (pd.DataFrame): df with cleaned and aggregated data
    """
    try:
        logging.info(f"Transforming raw data, starting with {len(raw_df)} rows.")
        
        df_clean = clean(raw_df)
        if df_clean is None:
            logging.error(f"Cleaning data failed. exiting transform step.")
            return None
        
        df_transformed = aggregate(df_clean)
        if df_transformed is None:
            logging.error(f"Aggregating data failed. exiting transform step.")
            return None
        
        logging.info(f"Done transforming data, number of rows: {len(df_transformed)}.")
        return df_transformed
    
    except Exception as e:
        logging.error(f'Unknown error during data transformation: {e}.')
        return None
