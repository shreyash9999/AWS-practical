import json
import boto3
import csv
from datetime import datetime
import urllib.parse
import pymysql
import os

s3 = boto3.client('s3')
sns = boto3.client('sns')

TOPIC_ARN = "arn:aws:sns:us-east-1:471112764802:student-pipeline-topic"

# ✅ Use environment variables
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']

def lambda_handler(event, context):

    print("Lambda triggered")

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'])

    print("Bucket:", bucket)
    print("Key:", key)

    local_input = '/tmp/input.csv'

    # Download file
    s3.download_file(bucket, key, local_input)

    try:
        connection = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        cursor = connection.cursor()

        with open(local_input, 'r') as infile:
            reader = csv.DictReader(infile)

            for row in reader:
                processed_time = datetime.utcnow().isoformat()

                cursor.execute(
                    "INSERT INTO processed_data (id, name, email, processed_timestamp) VALUES (%s, %s, %s, %s)",
                    (row['id'], row['name'], row['email'], processed_time)
                )

        connection.commit()
        cursor.close()
        connection.close()

        print("Data inserted into RDS")

        # SNS notification
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject="File Processed",
            Message=f"File {key} processed and inserted into RDS"
        )

        print("SNS sent")

    except Exception as e:
        print("Error:", str(e))
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
