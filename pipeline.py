import logging
from etl.extract import extract_csv
from etl.transform import transform
from etl.load import load
from config import INPUT_FILE, OUTPUT_FILE  # optional: use hardcoded strings for now if you want

def run_pipeline(input_file: str, output_file: str) -> bool:
    logging.info("Starting ETL pipeline...")

    raw_df = extract_csv(input_file)
    if raw_df is None:
        logging.error("Pipeline aborted: failed at extraction step.")
        return False

    transformed_df = transform(raw_df)
    if transformed_df is None:
        logging.error("Pipeline aborted: failed at transformation step.")
        return False

    if not load(transformed_df, output_file):
        logging.error("Pipeline aborted: failed at loading step.")
        return False

    logging.info("Successfully ran pipeline!")
    return True

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers= [logging.FileHandler("log/pipeline.log"),
                   logging.StreamHandler()]
    )
    
    success = run_pipeline(INPUT_FILE, OUTPUT_FILE)

    if success:
        logging.info("ETL pipeline completed successfully")
    else:
        logging.error("ETL pipeline failed")

