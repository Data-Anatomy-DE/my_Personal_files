# Connecting Snowflake to an AWS S3 Account — Step-by-Step

## Why a "Storage Integration," and not just access keys?

The old way to let Snowflake read S3 was to hand it a static AWS access key + secret directly in the stage definition. It works, but it means a long-lived credential sits inside Snowflake that has to be manually rotated and can leak. The modern, recommended approach uses a **Storage Integration**: Snowflake authenticates to AWS by *assuming an IAM role* — no long-lived keys stored anywhere. This guide sets that up end to end.

## Step 1 — Identify the S3 bucket

Decide (or create) the bucket and prefix Snowflake needs to read/write, e.g.:
```
s3://procurement-analytics-data/raw/
```
Least-privilege principle: scope everything that follows to this exact bucket/prefix, not `*`.

## Step 2 — Create an IAM Policy (AWS side)

In the AWS Console → IAM → Policies → Create Policy (or via CLI/Terraform):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion"],
      "Resource": "arn:aws:s3:::procurement-analytics-data/raw/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::procurement-analytics-data",
      "Condition": {
        "StringLike": { "s3:prefix": ["raw/*"] }
      }
    }
  ]
}
```
Name it something like `snowflake-procurement-s3-read-policy`.

## Step 3 — Create an IAM Role (AWS side)

Create a role (e.g., `snowflake-procurement-role`) and attach the policy from Step 2. For the trust policy, use a **placeholder** for now — Snowflake's actual IAM identity isn't known until Step 4:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::123456789012:root" },
      "Action": "sts:AssumeRole",
      "Condition": { "StringEquals": { "sts:ExternalId": "0000_TEMPORARY" } }
    }
  ]
}
```
Note the Role ARN, e.g. `arn:aws:iam::123456789012:role/snowflake-procurement-role` — you'll need it next.

## Step 4 — Create the Storage Integration (Snowflake side)

```sql
CREATE STORAGE INTEGRATION procurement_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-procurement-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://procurement-analytics-data/raw/');
```

## Step 5 — Retrieve Snowflake's generated AWS identity

```sql
DESC STORAGE INTEGRATION procurement_s3_integration;
```
From the output, copy two values:
- `STORAGE_AWS_IAM_USER_ARN` — the actual IAM user ARN Snowflake will use to assume your role.
- `STORAGE_AWS_EXTERNAL_ID` — a unique external ID for this specific integration.

## Step 6 — Update the IAM Role's trust policy (back on AWS)

Replace the placeholder from Step 3 with the real values from Step 5:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN from Step 5>" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from Step 5>" }
      }
    }
  ]
}
```
The `ExternalId` condition matters — without it, anyone who ever learns the role ARN could ask AWS to let them assume it too (the "confused deputy" problem). This scopes the trust to *only* requests carrying Snowflake's specific external ID for *this* integration.

## Step 7 — Create a File Format and an External Stage (Snowflake side)

```sql
CREATE FILE FORMAT procurement_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

CREATE STAGE procurement_db.raw.s3_stage
  URL = 's3://procurement-analytics-data/raw/'
  STORAGE_INTEGRATION = procurement_s3_integration
  FILE_FORMAT = procurement_csv_format;
```

## Step 8 — Test the connection end to end

```sql
LIST @procurement_db.raw.s3_stage;   -- confirms Snowflake can SEE the files

COPY INTO procurement_db.raw.purchase_orders
FROM @procurement_db.raw.s3_stage
PATTERN = '.*orders_.*[.]csv'
ON_ERROR = 'CONTINUE';
```
If `LIST` fails, the problem is almost always the trust policy/external ID (Step 6) or the bucket policy. If `LIST` works but `COPY INTO` fails, check the file format definition against the actual file structure.

## Step 9 (optional) — Continuous ingestion with Snowpipe

Once the stage works, event-driven auto-ingestion reuses the same integration:

1. Enable S3 event notifications on the bucket (object-created events) targeting an SQS queue.
2. Create a pipe referencing the same stage:
```sql
CREATE PIPE procurement_db.raw.orders_pipe
  AUTO_INGEST = TRUE
AS
  COPY INTO procurement_db.raw.purchase_orders
  FROM @procurement_db.raw.s3_stage
  PATTERN = '.*orders_.*[.]csv';
```
3. Retrieve the pipe's notification channel ARN (`SHOW PIPES`) and configure it as the SQS queue's event destination on the AWS side.

## Common failure points, quick reference

| Symptom | Likely cause |
|---|---|
| `LIST @stage` returns nothing / access denied | Trust policy principal or external ID doesn't match `DESC STORAGE INTEGRATION` output exactly |
| Works for some files, not others | IAM policy `Resource` prefix doesn't cover the actual object path |
| `COPY INTO` loads 0 rows, no error | File format mismatch (e.g., wrong delimiter, header not skipped) rather than an access problem |
| Works today, fails after a bucket policy change | Bucket policy explicitly denying the role, which overrides an otherwise-correct IAM policy |
