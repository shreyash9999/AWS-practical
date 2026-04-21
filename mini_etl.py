import pandas as pd
import boto3

# ---- CONFIG ----
bucket_name = "student-etl-shreyash"
input_key = "sales_file.csv"   # path in S3
output_key = "output/result/filtered_data.csv"

# ---- DOWNLOAD FROM S3 ----
s3 = boto3.client('s3')
s3.download_file(bucket_name, input_key, "sales_file.csv")

# ---- READ & FILTER ----
df = pd.read_csv("sales_file.csv")

# assuming column name is 'amount'
filtered_df = df[df['amount'] > 1000]

# ---- SAVE LOCALLY ----
filtered_df.to_csv("filtered_data.csv", index=False)

# ---- UPLOAD BACK TO S3 ----
s3.upload_file("filtered_data.csv", bucket_name, output_key)

print("Upload successful to:", output_key)
