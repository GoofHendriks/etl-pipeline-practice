import pandas as pd
from etl.transform import clean, aggregate

def test_clean_removes_invalid_rows():
    raw_data = pd.DataFrame({
        'order_id': [1, 2, 3],
        'order_amount': [100, None, 0],
        'order_date': ['2024-01-01', 'invalid_date', '2024-01-03'],
        'customer_id': [1, 2, 3],
        'customer_name': ['Alice', 'Bob', 'Charlie']
    })

    cleaned_df = clean(raw_data)

    assert cleaned_df.shape[0] == 1  # Only one valid row should remain
    assert cleaned_df['order_amount'].iloc[0] == 100

def test_aggregate_sums_orders():
    cleaned_df = pd.DataFrame({
        'order_id': [1, 2],
        'customer_id': [1, 1],
        'customer_name': ['Alice', 'Alice'],
        'order_amount': [800, 300],
        'order_date': ['2024-01-01', '2024-01-02']
    }).set_index('order_id')

    aggregated_df = aggregate(cleaned_df)

    assert aggregated_df['total_spent'].iloc[0] == 1100
    assert aggregated_df['order_count'].iloc[0] == 2
    assert aggregated_df['vip_customer'].iloc[0]

