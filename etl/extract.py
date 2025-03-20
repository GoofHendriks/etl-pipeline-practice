import pandas as pd
import logging

def extract_csv(input_file: str) -> pd.DataFrame:
    logging.info(f"Extracting data from: {input_file}...")
    try:
        raw_df = pd.read_csv(input_file)
        logging.info(f"Successfully extracted {len(raw_df)} rows from {input_file}")
        return raw_df
    except FileNotFoundError as e:
        logging.error(f"File not found: {input_file}, error: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to extract data: {e}")
        return None
