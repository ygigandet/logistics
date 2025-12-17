"""
Compute and update straight-line distances for Amazon delivery dataset.
Modify incorrect latitude and longitude points.
"""

import duckdb
from datetime import datetime, date, time
from geopy.distance import geodesic


def compute_distance(lat1, lon1, lat2, lon2):
    """
    Compute geodesic distance in meters.
    :return: distance in meters
    """
    return int(geodesic((lat1, lon1), (lat2, lon2)).meters)


# https://api.openrouteservice.org/ for the API

# First, I simulate that the data came from a database: amazon_delivery

con = duckdb.connect(
    database="data/processed/amazon_delivery_clean.duckdb", read_only=False
)

# Compute the 'straight' distance between two coordinates

# First create a new column in the database

con.execute("ALTER TABLE amazon_delivery ADD COLUMN IF NOT EXISTS distance INTEGER")

# Compute the straight distance between coordinates, not ideal, but enough for now

rows = con.execute(
    """
    SELECT Order_ID, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude
    FROM amazon_delivery
    WHERE distance IS NULL
"""
).fetchall()

for row in rows:
    Order_ID, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude = row
    # Compute geodesic distance
    d = compute_distance(Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude)
    # Update DuckDB directly
    con.execute(
        "UPDATE amazon_delivery SET distance = ? WHERE Order_ID = ?", (d, Order_ID)
    )

# pylint: disable=fixme
# TODO: Validate distances for last-mile delivery.
# Typical threshold: 30 km (30,000 meters)

# Check for data consistency for distance

df = con.execute(
    """
    SELECT Order_ID, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude, distance
    FROM amazon_delivery
"""
).df()

# For the assumptions, the last-mile delivery should not exceed 50km.

incorrect_distance = df[df["distance"] > 50000]
print(f"{incorrect_distance["Order_ID"].count()} seem out of range for the distance")

print(incorrect_distance)

# Cleaning latitude and longitude

rows = con.execute(
    """
    SELECT Order_ID, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude
    FROM amazon_delivery
    WHERE distance > 50000
"""
).fetchall()

# Update the latitude and longitude
for row in rows:
    Order_ID, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude = row
    # Check if latitudes are way over limit
    if abs(Store_Latitude - Drop_Latitude) > 2:
        Drop_Latitude = -Drop_Latitude
        # Update DuckDB directly
        con.execute(
            "UPDATE amazon_delivery SET Drop_Latitude = ? WHERE Order_ID = ?",
            (Drop_Latitude, Order_ID),
        )
    if abs(Store_Longitude - Drop_Longitude) > 2:
        Drop_Longitude = -Drop_Longitude
        # Update DuckDB directly
        con.execute(
            "UPDATE amazon_delivery SET Drop_Longitude = ? WHERE Order_ID = ?",
            (Drop_Longitude, Order_ID),
        )
    # Compute geodesic distance
    d = compute_distance(Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude)
    # Update DuckDB directly
    con.execute(
        "UPDATE amazon_delivery SET distance = ? WHERE Order_ID = ?", (d, Order_ID)
    )
