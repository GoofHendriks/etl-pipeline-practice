import pandas as pd
import logging

def load(df_transformed: pd.DataFrame, output_file: str) -> bool:
    """
    loads transformed dat to a csv file

    args:
        df_transformed (pd.DataFrame): transformed data in a df
        output_file (str): path to where the csv file needs to be loaded to
    returns:
        bool: True if the data was successfully saved, False if an error occurred.
    """
    try:
        logging.info(f"Loading transformed data to: {output_file}...")
        df_transformed.to_csv(output_file, index=False)
        logging.info(f"Succesfully saved {len(df_transformed)} rows to {output_file}.")
        return True
    except Exception as e:
        logging.error(f"Failed to load data due to unexpected error: {e}")
        return False
        