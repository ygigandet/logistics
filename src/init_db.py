# pylint: disable=missing-module-docstring

import duckdb

# Creation of the database for SQL queries.

con = duckdb.connect(
    database="data/processed/amazon_delivery_clean.duckdb", read_only=False
)

con.execute(
    """
    CREATE OR REPLACE TABLE amazon_delivery AS
    SELECT * FROM read_csv_auto('data/processed/amazon_delivery_clean.csv')
"""
)
