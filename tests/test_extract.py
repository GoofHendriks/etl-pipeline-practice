import pandas as pd
import pytest
from etl.extract import extract_csv

def test_extract_csv_success(tmp_path):
    # Create a temporary CSV file
    data = {'order_id': [1], 'order_amount': [100]}
    df_expected = pd.DataFrame(data)
    temp_file = tmp_path / "test_orders.csv"
    df_expected.to_csv(temp_file, index=False)

    df_result = extract_csv(temp_file)

    assert df_result.equals(df_expected)

def test_extract_csv_file_not_found():
    result = extract_csv("non_existent_file.csv")
    assert result is None
