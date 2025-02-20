# -*- coding: utf-8 -*-
"""Untitled0.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1gC0GqKp9zwcTPPr8JzoQ8xIw_6vQo0_M
"""



import pandas as pd

# Load the CSV file
df = pd.read_csv("volume_turnover_aum_data - Backfill Try 2 full doc (1).csv", parse_dates=["DATE"], dayfirst=True)

# Convert date format to YYYY-MM-DD
df["DATE"] = df["DATE"].dt.strftime("%Y-%m-%d")

# Rename columns to match the required format
df.columns = ["date", "volume_9008", "volume_9042", "volume_9439", "turnover_9008", "turnover_9042", "turnover_9439"]

# Reorder columns
df = df[["date", "volume_9008", "turnover_9008", "volume_9042", "turnover_9042", "volume_9439", "turnover_9439"]]

# Save the modified CSV
df.to_csv("formatted_file.csv", index=False)

print("CSV file has been reformatted and saved as formatted_file.csv")

