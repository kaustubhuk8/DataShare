import os
import json
import csv
from datetime import datetime
import boto3
import snowflake.connector

from datetime import datetime

LOG_FILE = 'logs/pipeline_log.txt'

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(LOG_FILE, 'a') as log_file:
        log_file.write(f"[{timestamp}] {message}\n")


def load_config():
    """Load configuration files"""
    try:
        with open('configs/aws_config.json') as f:
            aws_config = json.load(f)
        with open('configs/snowflake_config.json') as f:
            snowflake_config = json.load(f)
        return aws_config, snowflake_config
    except Exception as e:
        print(f"Error loading config files: {e}")
        return None, None

def process_transaction_data(input_file, output_file):
    """
    Process original transaction data to fit Snowflake schema:
    - Rename and map fields
    - Remove '$' from amount, convert to float
    - Output only required columns
    """
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w', newline='') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = ['user_id', 'transaction_id', 'amount', 'timestamp', 'merchant_category', 'region']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                try:
                    processed_row = {
                        'user_id': row['client_id'],
                        'transaction_id': row['id'],
                        'amount': float(row['amount'].replace('$', '').replace(',', '')),
                        'timestamp': row['date'],
                        'merchant_category': row['mcc'],
                        'region': row['merchant_state']
                    }
                    writer.writerow(processed_row)
                except Exception as inner_e:
                    print(f"Skipping row due to error: {inner_e}")
        return True
    except Exception as e:
        print(f"Error processing transaction data: {e}")
        return False


def upload_to_s3(aws_config, file_path):
    """Upload file to S3 bucket"""
    try:
        s3 = boto3.client('s3',
                        aws_access_key_id=aws_config['aws_access_key_id'],
                        aws_secret_access_key=aws_config['aws_secret_access_key'],
                        region_name=aws_config['region'])
        s3.upload_file(file_path, aws_config['bucket_name'], f"transactions/{os.path.basename(file_path)}")
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False

def load_to_snowflake(snowflake_config, file_name):
    """Load data to Snowflake with detailed debug steps and object checks."""
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            user=snowflake_config['username'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema'],
            warehouse=snowflake_config['warehouse']
        )
        cursor = conn.cursor()

        print("🧊 Connected to Snowflake.")
        print("📌 Setting context...")

        # Set context
        cursor.execute(f"USE WAREHOUSE {snowflake_config['warehouse']}")
        cursor.execute(f"USE DATABASE {snowflake_config['database']}")
        cursor.execute(f"USE SCHEMA {snowflake_config['schema']}")
        print("✅ Context set.")

        # Check if table exists
        print(f"🔍 Checking if table '{snowflake_config['table']}' exists...")
        cursor.execute(f"SHOW TABLES LIKE '{snowflake_config['table']}'")
        if not cursor.fetchall():
            raise Exception(f"Table '{snowflake_config['table']}' does not exist.")

        # Check if stage exists
        print(f"🔍 Checking if stage '{snowflake_config['stage']}' exists...")
        cursor.execute(f"SHOW STAGES LIKE '{snowflake_config['stage']}'")
        if not cursor.fetchall():
            raise Exception(f"Stage '{snowflake_config['stage']}' does not exist.")

        # Check if file is visible from stage
        print(f"🔍 Checking if file '{file_name}' is visible in stage...")
        cursor.execute(f"LIST @{snowflake_config['stage']}")
        stage_files = [row[0].split('/')[-1] for row in cursor.fetchall()]
        print(f"📁 Files in stage: {stage_files}")
        if file_name not in stage_files:
            raise Exception(f"File '{file_name}' not found in @{snowflake_config['stage']}")

        # Perform COPY INTO
        print("🚀 Running COPY INTO...")
        cursor.execute(f"""
            COPY INTO {snowflake_config['table']}
            FROM @{snowflake_config['stage']}
            FILES = ('{file_name}')
            FILE_FORMAT = (TYPE = 'CSV' PARSE_HEADER = TRUE)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)
        print("✅ COPY INTO completed successfully.")

        cursor.close()
        conn.close()
        return True

    except Exception as e:
        print(f"❌ Error loading to Snowflake: {e}")
        return False



def main():
    """Main function to run the pipeline"""
    print("🔧 Loading configuration files...")
    aws_config, snowflake_config = load_config()
    if not aws_config or not snowflake_config:
        print("❌ Failed to load configuration files. Exiting.")
        return

    input_file = 'data/transactions_data.csv'
    output_file = f"processed_transaction_{datetime.now().strftime('%Y%m%d')}.csv"

    print(f"📂 Processing input file: {input_file} → {output_file}")
    log(f"Processing input file: {input_file} → {output_file}")
    if not process_transaction_data(input_file, output_file):
        print("❌ Failed during CSV processing.")
        return
    print("✅ Data processing complete.")

    print("☁️ Uploading to S3...")
    log("Uploading to S3...")
    if not upload_to_s3(aws_config, output_file):
        print("❌ Upload to S3 failed.")
        return
    print("✅ File uploaded to S3.")

    print("🧊 Loading to Snowflake...")
    log("Loading to Snowflake...")
    print("Snowflake config received:")
    print(snowflake_config)

    if not load_to_snowflake(snowflake_config, os.path.basename(output_file)):
        print("❌ Snowflake load failed.")
        return
    print("✅ Snowflake load complete.")

    print("🎉 Pipeline completed successfully.")
    log("Pipeline completed successfully.")


if __name__ == "__main__":
    main()
