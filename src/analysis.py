"""
Compute and update straight-line distances for Amazon delivery dataset.
Modify incorrect latitude and longitude points.
"""

from datetime import date, datetime, time

import duckdb
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
MAX_LAST_METERS_THRESHOLD = 50000

incorrect_distance = df[df["distance"] > MAX_LAST_METERS_THRESHOLD]
print(f"{incorrect_distance['Order_ID'].count()} seem out of range for the distance")

print(incorrect_distance)

# Cleaning latitude and longitude

rows = con.execute(
    f"""
    SELECT Order_ID, Store_Latitude, Store_Longitude, Drop_Latitude, Drop_Longitude
    FROM amazon_delivery
    WHERE distance > {MAX_LAST_METERS_THRESHOLD}
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

# Check if problem solved -> yes
df = con.execute("SELECT * FROM amazon_delivery").df()
summary = df.describe(include="all").T
print(summary)

# Check the Order-to-Pickup Time (02P)

con.execute(
    "ALTER TABLE amazon_delivery ADD COLUMN IF NOT EXISTS Order_Pickup_Time INTEGER"
)

rows = con.execute(
    """
    SELECT Order_ID, Order_Time, Pickup_Time
    FROM amazon_delivery
    WHERE Order_Pickup_Time IS NULL
"""
).fetchall()

for row in rows:
    Order_ID, order_time, pickup_time = row
    # Convert strings to datetime
    if isinstance(order_time, str):
        order_time = datetime.strptime(order_time, "%H:%M:%S").replace(
            year=2025, month=1, day=1
        )
    elif isinstance(order_time, time):
        order_time = datetime.combine(date(2025, 1, 1), order_time)

    if isinstance(pickup_time, str):
        pickup_time = datetime.strptime(pickup_time, "%H:%M:%S").replace(
            year=2025, month=1, day=1
        )
    elif isinstance(pickup_time, time):
        pickup_time = datetime.combine(date(2025, 1, 1), pickup_time)

    # Handle pickup crossing midnight
    if pickup_time < order_time:
        pickup_time = pickup_time.replace(day=2)  # next day

    # Compute Order_Pickup_Time in minutes
    order_pickup_minutes = round((pickup_time - order_time).total_seconds() / 60)
    # Update DuckDB
    con.execute(
        "UPDATE amazon_delivery SET Order_Pickup_Time = ? WHERE Order_ID = ?",
        (order_pickup_minutes, Order_ID),
    )

df = con.execute(
    "SELECT Order_Time, Pickup_Time, Order_Pickup_Time FROM amazon_delivery"
).df()
print(df)
