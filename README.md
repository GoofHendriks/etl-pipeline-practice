This project demonstrates an ETL (Extract, Transform, Load) pipeline built with Python.
It processes e-commerce order data by:

Extracting raw CSV data
- Cleaning and transforming it (handling missing data, data types, aggregations)
- Loading the cleaned and aggregated data into a new CSV file.

The pipeline features:

Modular code structure (extract, transform, load)
- Logging (console + file)
- Error handling
- Unit tests using pytest


```bash
etl_pipeline_project/
├── etl/
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── data/
│   ├── ecommerce_orders.csv          # Synthetic sample data (for demo/testing)
├── logs/
│   └── pipeline.log                  # Log output (optional example)
├── pipeline.py                       # Pipeline orchestrator
├── config.py                         # Configurations (file paths, constants)
├── requirements.txt                  # Python dependencies
└── README.md
```

ETL Flow:

Extract
Reads ecommerce_orders.csv into a Pandas DataFrame.

Transform
Cleans the data:
Drops rows with missing or invalid values.
Converts data types.

Aggregates:
Calculates total sales and number of orders per customer.
Flags VIP customers (those spending over 1000 units).

Load
Writes the transformed data into transformed_data.csv.

Sample Data:

The file ecommerce_orders.csv contains synthetic data generated for demonstration purposes.
It includes fields:

order_id
customer_id
customer_name
order_amount
order_date

Getting started

clone the repo:
```bash
git clone https://github.com/yourusername/etl_pipeline_project.git
cd etl_pipeline_project
```

install dependencies:
```bash
pip install -r requirements.txt
```

run the pipeline:
```bash
python pipeline.py

```

run unit tests:
```bash
pytest
```





