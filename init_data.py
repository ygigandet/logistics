# pylint: disable=missing-module-docstring

import duckdb
import kagglehub
import pandas as pd

# Download latest version
path = kagglehub.dataset_download("sujalsuthar/amazon-delivery-dataset")

# Read the data
data = pd.read_csv(f"{path}/amazon_delivery.csv")

# Save data in the project
data.to_csv("data/raw/amazon_delivery.csv", index=False)

### Creation of the database for SQL queries.

con = duckdb.connect(database="data/raw/amazon_delivery.duckdb", read_only=False)

con.execute(
    """
    CREATE OR REPLACE TABLE amazon_delivery AS
    SELECT * FROM read_csv_auto('data/raw/amazon_delivery.csv')
"""
)
