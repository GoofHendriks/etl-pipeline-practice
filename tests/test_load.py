import pandas as pd
import os
from etl.load import load

def test_load_creates_file(tmp_path):
    df = pd.DataFrame({
        'customer_id': [1],
        'customer_name': ['Alice'],
        'total_spent': [500],
        'order_count': [2],
        'vip_customer': [False]
    })

    output_file = tmp_path / "output.csv"

    result = load(df, output_file)

    assert result is True
    assert os.path.exists(output_file)

    # Check if data was written
    df_loaded = pd.read_csv(output_file)
    assert df_loaded.shape[0] == 1
