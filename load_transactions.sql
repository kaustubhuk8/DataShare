-- ===========================
-- 1. Storage Integration (One-time setup)
-- ===========================

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<your-account-id>:role/<your-role-name>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://dcr-demo-data/transactions/');

-- ===========================
-- 2. External Stage
-- ===========================

CREATE OR REPLACE STAGE transactions_stage
  URL = 's3://dcr-demo-data/transactions/'
  STORAGE_INTEGRATION = s3_int
  FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TIMESTAMP_FORMAT = 'AUTO'
  );

-- ===========================
-- 3. Raw Transactions Table
-- ===========================

CREATE OR REPLACE TABLE transactions_raw (
  user_id STRING,
  transaction_id STRING,
  amount FLOAT,
  timestamp TIMESTAMP_NTZ,
  merchant_category STRING,
  region STRING,
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ===========================
-- 4. Stream for Change Tracking (optional)
-- ===========================

CREATE OR REPLACE STREAM transactions_stream ON TABLE transactions_raw;

-- ===========================
-- 5. Scheduled Task for Daily Loading
-- ===========================

CREATE OR REPLACE TASK load_transactions
  WAREHOUSE = YOUR_WAREHOUSE
  SCHEDULE = 'USING CRON 0 8 * * * America/Los_Angeles'
AS
  COPY INTO transactions_raw (user_id, transaction_id, amount, timestamp, merchant_category, region)
  FROM @transactions_stage
  PATTERN = '.*transactions_.*[.]csv';

-- ===========================
-- 6. View for Aggregate-Only Access (Clean Room Simulation)
-- ===========================

CREATE OR REPLACE VIEW safe_transaction_aggregates AS
SELECT 
  region,
  merchant_category,
  COUNT(DISTINCT user_id) AS user_count,
  SUM(amount) AS total_spend,
  DATE_TRUNC('day', timestamp) AS day
FROM transactions_raw
GROUP BY region, merchant_category, day;
