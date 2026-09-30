-- 02_stage_and_file_format.sql
--
-- IMPORTANT: Replace the two placeholder values below with your real AWS
-- access key / secret key when you run this in Snowsight. Do NOT commit
-- a version of this file with real credentials filled in — keep this
-- file with placeholders in git, and only ever paste real values
-- directly into the Snowsight worksheet at run time.
--
-- (A credential-free alternative — a Snowflake STORAGE INTEGRATION backed
-- by an IAM role — is a good later hardening step; this direct-credential
-- approach is the faster path to get moving now.)

USE DATABASE RETAILPULSE;
USE SCHEMA RAW;

CREATE FILE FORMAT IF NOT EXISTS RETAILPULSE.RAW.PARQUET_FORMAT
    TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS RETAILPULSE.RAW.RETAILPULSE_S3_STAGE
    URL = 's3://retailpulse-data/'
    CREDENTIALS = (
        AWS_KEY_ID = '<YOUR_AWS_ACCESS_KEY_ID>'
        AWS_SECRET_KEY = '<YOUR_AWS_SECRET_ACCESS_KEY>'
    )
    FILE_FORMAT = RETAILPULSE.RAW.PARQUET_FORMAT
    COMMENT = 'Points at the RetailPulse S3 data lake bucket root';

-- Quick sanity check: this should list the raw/<table>/... Parquet files
-- sitting in your bucket, confirming the stage can actually see them.
LIST @RETAILPULSE.RAW.RETAILPULSE_S3_STAGE/raw/customers/;