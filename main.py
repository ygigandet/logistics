import kagglehub
import pandas as pd

# Download latest version
path = kagglehub.dataset_download("sujalsuthar/amazon-delivery-dataset")

# Read the data
data = pd.read_csv(f"{path}/amazon_delivery.csv")

# Save data in the project
data.to_csv("data/raw/amazon_delivery.csv", index=False)
