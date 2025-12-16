# pylint: disable=missing-module-docstring

# --------- DATA CLEANING ---------

import pandas as pd

# Load dataset
df = pd.read_csv("data/raw/amazon_delivery.csv")


# Quick overview of the data
print(f"The raw dataset has {df.shape[0]} rows and {df.shape[1]} columns \n")

print("Initial dataset overview: \n")
print(df.head(10))

# Quick information about the data

print("Initial dataset information: \n")
print(df.info())

# Summary statistics
print("Some statistics about the initial dataset \n")
summary = df.describe(include="all").T
summary["missing_values"] = df.isna().sum()
print(summary)

# Handling missing values
## For the sake of the project, I removed the missing values for the weather.
## The other variable containing missing values is agent_rating.
## There is no agent_id, so I assume that they do not have ratings as they are new.

df_clean = df.dropna(subset=["Weather"])

print(
    f"The cleaned dataset has {df_clean.shape[0]} rows and {df_clean.shape[1]} columns \n"
)

# Order_ID is unique? Yes

print(f"Unique ID: {df_clean.Order_ID.nunique()} must be equal to {df_clean.shape[0]}")

summary_clean = df_clean.describe(include="all").T
summary_clean["missing_values"] = df_clean.isna().sum()
print(summary_clean)

print(type(df_clean))

# Save the clean data

df_clean.to_csv("data/processed/amazon_delivery_clean.csv", index=False)
