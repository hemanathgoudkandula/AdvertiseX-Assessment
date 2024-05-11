-- Storage integration with s3 bucket and snowflake
CREATE STORAGE INTEGRATION advertisex_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::IAM_role:role/snowflake_access'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('*')
  ;
