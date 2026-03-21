# Databricks notebook source
# MAGIC %md
# MAGIC # RUNZ Lambda — Batch Test & Dev Autoloader
# MAGIC
# MAGIC **Part 1:** Pull email S3 keys from existing autoloader table, invoke Lambda for each  
# MAGIC **Part 2:** Autoloader reads Lambda test output into `raw_us_corporates_dev` for comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Batch Invoke Lambda

# COMMAND ----------

import boto3
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

lambda_client = boto3.client("lambda", region_name="us-east-1")
FUNCTION_NAME = "runz-parser-prod"
INGEST_BUCKET = "use1-s3-bcq-prod-bcqemail-ingest"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from raw_us_corporates.runz_parser_results where email_id='2dead775-3b1f-4ed4-8497-56e92d8961a0'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull test emails from existing catalog
# MAGIC Adjust the WHERE clause to target specific dates, senders, or formats.

# COMMAND ----------

# Pull email S3 keys from your existing autoloader/catalog table
# Adjust date range and LIMIT as needed

# 20 emails per trader for any trader active on 19 Mar 2026 — all statuses,
# both HTML-table dealers (runz_parser_results) and JPM space-delimited
# (sp_runz_parser_results). UNIONed before ranking so each trader gets at
# most 20 across both sources.
test_emails_df = spark.sql("""
    WITH combined AS (
        -- Standard HTML-table dealers
        SELECT
            replace(file_path,'s3://use1-s3-bcq-prod-bcqemail-ingest/','') AS s3_key,
            email_timestamp,
            get_json_object(message, '$.mailTrader')                        AS from_email,
            get_json_object(message, '$.quoteType')                         AS format,
            status,
            'html'                                                          AS source_table
        FROM raw_us_corporates.runz_parser_results
        WHERE email_timestamp LIKE '%19 Mar 2026%'

        UNION ALL

        -- JPM space-delimited parser
        SELECT
            replace(file_path,'s3://use1-s3-bcq-prod-bcqemail-ingest/','') AS s3_key,
            email_timestamp,
            from_email,
            get_json_object(message, '$.quoteType')                         AS format,
            status,
            'jpm'                                                           AS source_table
        FROM raw_us_corporates.sp_runz_parser_results
        WHERE email_timestamp LIKE '%19 Mar 2026%'
    ),
    ranked AS (
        SELECT
            s3_key, email_timestamp, from_email, format, status, source_table,
            ROW_NUMBER() OVER (
                PARTITION BY from_email
                ORDER BY email_timestamp DESC
            ) AS rn
        FROM combined
    )
    SELECT s3_key, email_timestamp, from_email, format, status, source_table
    FROM ranked
    WHERE rn <= 20
    ORDER BY from_email, email_timestamp DESC
""")

print(f"Found {test_emails_df.count()} test emails across {test_emails_df.select('from_email').distinct().count()} traders")
display(test_emails_df.groupBy("source_table", "status").count().orderBy("source_table", "status"))
display(test_emails_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC If you don't have s3_key in runz_parser_results, you can pull from the email catalog:
# MAGIC ```sql
# MAGIC SELECT s3_key FROM raw_us_corporates.runz_email_autoloader_data2
# MAGIC WHERE processing_timestamp >= '2026-02-17'
# MAGIC LIMIT 200
# MAGIC ```
# MAGIC Or list directly from S3 for a specific date:

# COMMAND ----------

# Alternative: list S3 keys directly for a date range
import boto3

def list_email_keys(prefix="processed/20260224/", max_keys=100):
    s3 = boto3.client("s3", region_name="us-east-1")
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=INGEST_BUCKET, Prefix=prefix, MaxKeys=max_keys):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys

# Uncomment to use S3 listing instead of SQL query:
# s3_keys = list_email_keys("processed/20260219/", max_keys=50)
# print(f"Found {len(s3_keys)} emails in S3")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Invoke Lambda for each email

# COMMAND ----------

def invoke_lambda(s3_key):
    """Invoke the parser Lambda for a single email. Returns result dict."""
    payload = {
        "Records": [{
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": INGEST_BUCKET},
                        "object": {"key": s3_key}
                    }
                }]
            })
        }]
    }
    
    try:
        response = lambda_client.invoke(
            FunctionName=FUNCTION_NAME,
            Payload=json.dumps(payload),
        )
        result = json.loads(response["Payload"].read().decode("utf-8"))
        body = json.loads(result.get("body", "{}"))
        
        if body.get("results"):
            r = body["results"][0]
            return {
                "s3_key": s3_key,
                "status": r.get("status", "UNKNOWN"),
                "from_email": r.get("from_email", ""),
                "format": r.get("format", ""),
                "header_source": r.get("header_source", ""),
                "quote_count": r.get("quote_count", 0),
                "parsed_header": r.get("parsed_header", ""),
                "failure_class": r.get("failure_class", ""),
                "error": r.get("error", ""),
            }
        else:
            return {
                "s3_key": s3_key,
                "status": "ERROR",
                "error": str(body),
            }
    except Exception as e:
        return {
            "s3_key": s3_key,
            "status": "EXCEPTION",
            "error": str(e),
        }

# COMMAND ----------



# COMMAND ----------

# Collect the S3 keys to process
# Option A: from SQL query above
rows = test_emails_df.select("s3_key").distinct().collect()
s3_keys = [row.s3_key for row in rows if row.s3_key]

# Option B: from S3 listing (uncomment if using that approach)
# s3_keys = list_email_keys("processed/20260219/", max_keys=50)

print(f"Invoking Lambda for {len(s3_keys)} emails...")

# COMMAND ----------

# Run invocations in parallel (10 at a time to avoid throttling)
results = []
start_time = time.time()

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(invoke_lambda, key): key for key in s3_keys}
    
    for i, future in enumerate(as_completed(futures)):
        result = future.result()
        results.append(result)
        
        # Progress update every 25 emails
        if (i + 1) % 25 == 0 or (i + 1) == len(s3_keys):
            elapsed = time.time() - start_time
            success = len([r for r in results if r.get("status") == "SUCCESS"])
            errors = len([r for r in results if r.get("status") != "SUCCESS"])
            print(f"  [{i+1}/{len(s3_keys)}] {elapsed:.0f}s elapsed | {success} success | {errors} errors")

elapsed = time.time() - start_time
print(f"\nDone! {len(results)} emails in {elapsed:.1f}s")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Results Summary

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(results)

# Summary stats
print("=== STATUS COUNTS ===")
print(results_df["status"].value_counts().to_string())
print()

print("=== HEADER SOURCE (how headers were resolved) ===")
if "header_source" in results_df.columns:
    print(results_df[results_df["status"] == "SUCCESS"]["header_source"].value_counts().to_string())
print()

print("=== FORMAT DISTRIBUTION ===")
if "format" in results_df.columns:
    print(results_df[results_df["status"] == "SUCCESS"]["format"].value_counts().to_string())
print()

total_quotes = results_df[results_df["status"] == "SUCCESS"]["quote_count"].sum()
print(f"=== TOTAL QUOTES PARSED: {int(total_quotes)} ===")

# Show errors if any
errors_df = results_df[results_df["status"] != "SUCCESS"]
if len(errors_df) > 0:
    print(f"\n=== {len(errors_df)} ERRORS ===")
    display(errors_df[["s3_key", "status", "error"]])

# COMMAND ----------



# COMMAND ----------

# Full results as Spark DataFrame for deeper analysis
results_spark = spark.createDataFrame(pd.DataFrame(results))
results_spark.createOrReplaceTempView("lambda_test_results")
display(results_spark)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare quote counts: Lambda vs Production
# MAGIC
# MAGIC Join Lambda results with existing production parser results to compare.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare quote counts between Lambda and production parser (both HTML and JPM tables)
# MAGIC SELECT
# MAGIC     l.s3_key,
# MAGIC     l.from_email                               AS lambda_sender,
# MAGIC     l.status                                   AS lambda_status,
# MAGIC     l.quote_count                              AS lambda_quotes,
# MAGIC     l.header_source,
# MAGIC     l.format                                   AS lambda_format,
# MAGIC     l.failure_class,
# MAGIC     p.quote_count                              AS prod_quotes,
# MAGIC     p.source_table                             AS prod_source,
# MAGIC     l.quote_count - coalesce(p.quote_count, 0) AS diff
# MAGIC FROM lambda_test_results l
# MAGIC LEFT JOIN (
# MAGIC     -- HTML-table dealers
# MAGIC     SELECT
# MAGIC         replace(file_path,'s3://use1-s3-bcq-prod-bcqemail-ingest/','') AS s3_key,
# MAGIC         CAST(get_json_object(message, '$.quoteCount') AS INT)           AS quote_count,
# MAGIC         'html'                                                           AS source_table
# MAGIC     FROM raw_us_corporates.runz_parser_results
# MAGIC     WHERE status IN ('SUCCESS', 'PARTIAL_PARSE')
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     -- JPM space-delimited parser
# MAGIC     SELECT
# MAGIC         replace(file_path,'s3://use1-s3-bcq-prod-bcqemail-ingest/','') AS s3_key,
# MAGIC         CAST(get_json_object(message, '$.quoteCount') AS INT)           AS quote_count,
# MAGIC         'jpm'                                                            AS source_table
# MAGIC     FROM raw_us_corporates.sp_runz_parser_results
# MAGIC     WHERE status IN ('SUCCESS', 'PARTIAL_PARSE')
# MAGIC ) p ON l.s3_key = p.s3_key
# MAGIC WHERE l.status IN ('SUCCESS', 'PARTIAL_PARSE', 'FAILURE')
# MAGIC ORDER BY diff DESC NULLS LAST

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Reset Test Environment
# MAGIC
# MAGIC Run these cells to go back to a **blank slate** before re-testing:
# MAGIC 1. Inspect S3 source folders (see what Lambda wrote)
# MAGIC 2. Wipe S3 source files (so autoloader starts fresh)
# MAGIC 3. Drop dev Delta tables
# MAGIC 4. Delete autoloader checkpoints

# COMMAND ----------

import boto3

OUTPUT_BUCKET = "use1-s3-bcq-prod-elt-raw"
TEST_PREFIX = "runz_lambda_test"
DEV_SCHEMA = "raw_us_corporates_dev"

_s3 = boto3.client("s3", region_name="us-east-1")

# All S3 prefixes the Lambda writes to
_test_prefixes = {
    "pretrade_quotes":    f"{TEST_PREFIX}/pretrade_quotes/",
    "parser_results":     f"{TEST_PREFIX}/runz_parser_results/",
    "email_catalog":      f"{TEST_PREFIX}/runz_email_catalog/",
}

# Autoloader checkpoint prefixes
_checkpoint_prefixes = {
    "pretrade_quotes":    f"_checkpoints/{TEST_PREFIX}/pretrade_quotes/",
    "parser_results":     f"_checkpoints/{TEST_PREFIX}/runz_parser_results/",
    "email_catalog":      f"_checkpoints/{TEST_PREFIX}/runz_email_catalog/",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Inspect S3 source folders
# MAGIC
# MAGIC Lists files in each Lambda test output prefix so you can see what's there.

# COMMAND ----------

for label, prefix in _test_prefixes.items():
    keys = []
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=OUTPUT_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append((obj["Key"], obj["Size"], str(obj["LastModified"])))

    print(f"\n=== {label} ({prefix}) — {len(keys)} files ===")
    if keys:
        import pandas as pd
        df = pd.DataFrame(keys, columns=["key", "size_bytes", "last_modified"])
        df = df.sort_values("last_modified", ascending=False)
        display(df.head(30))
    else:
        print("  (empty)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Wipe S3 source files
# MAGIC
# MAGIC Deletes all Lambda test output files from S3.
# MAGIC **⚠️ Destructive** — run intentionally.

# COMMAND ----------

for label, prefix in _test_prefixes.items():
    deleted = 0
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=OUTPUT_BUCKET, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            _s3.delete_objects(Bucket=OUTPUT_BUCKET, Delete={"Objects": objects})
            deleted += len(objects)
    print(f"Deleted {deleted} files from s3://{OUTPUT_BUCKET}/{prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Drop dev Delta tables

# COMMAND ----------

for table_name in [
    f"{DEV_SCHEMA}.pretrade_runz_quote_norm",
    f"{DEV_SCHEMA}.runz_parser_results",
    f"{DEV_SCHEMA}.runz_email_catalog",
]:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        print(f"Dropped {table_name}")
    except Exception as e:
        print(f"Could not drop {table_name}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Delete autoloader checkpoints
# MAGIC
# MAGIC Removes checkpoint folders from S3 so autoloader re-processes all files.

# COMMAND ----------

for label, prefix in _checkpoint_prefixes.items():
    deleted = 0
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=OUTPUT_BUCKET, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            _s3.delete_objects(Bucket=OUTPUT_BUCKET, Delete={"Objects": objects})
            deleted += len(objects)
    print(f"Deleted {deleted} checkpoint files from s3://{OUTPUT_BUCKET}/{prefix}")

print("\nReset complete. Ready for a fresh test run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Dev Autoloader — Load Lambda Test Output
# MAGIC
# MAGIC Reads from `runz_lambda_test/` prefixes and writes to `raw_us_corporates_dev` tables.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

OUTPUT_BUCKET = "use1-s3-bcq-prod-elt-raw"
TEST_PREFIX = "runz_lambda_test"
DEV_SCHEMA = "raw_us_corporates_dev"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the dev schema if it doesn't exist
# MAGIC CREATE SCHEMA IF NOT EXISTS raw_us_corporates_dev;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quotes Autoloader

# COMMAND ----------

# Quotes — from Lambda test output
quotes_path = f"s3://{OUTPUT_BUCKET}/{TEST_PREFIX}/pretrade_quotes/"
quotes_checkpoint = f"s3://{OUTPUT_BUCKET}/_checkpoints/{TEST_PREFIX}/pretrade_quotes/"

quotes_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", quotes_checkpoint)
    .load(quotes_path)
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(
    quotes_df.writeStream
    .format("delta")
    .option("checkpointLocation", quotes_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{DEV_SCHEMA}.pretrade_runz_quote_norm")
)

print(f"Quotes autoloader writing to {DEV_SCHEMA}.pretrade_runz_quote_norm")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parser Results Autoloader

# COMMAND ----------

# Parser results metadata
results_path = f"s3://{OUTPUT_BUCKET}/{TEST_PREFIX}/runz_parser_results/"
results_checkpoint = f"s3://{OUTPUT_BUCKET}/_checkpoints/{TEST_PREFIX}/runz_parser_results/"

results_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", results_checkpoint)
    .load(results_path)
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(
    results_df.writeStream
    .format("delta")
    .option("checkpointLocation", results_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{DEV_SCHEMA}.runz_parser_results")
)

print(f"Results autoloader writing to {DEV_SCHEMA}.runz_parser_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Email Catalog Autoloader

# COMMAND ----------

# Email catalog
catalog_path = f"s3://{OUTPUT_BUCKET}/{TEST_PREFIX}/runz_email_catalog/"
catalog_checkpoint = f"s3://{OUTPUT_BUCKET}/_checkpoints/{TEST_PREFIX}/runz_email_catalog/"

catalog_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", catalog_checkpoint)
    .load(catalog_path)
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(
    catalog_df.writeStream
    .format("delta")
    .option("checkpointLocation", catalog_checkpoint)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(f"{DEV_SCHEMA}.runz_email_catalog")
)

print(f"Catalog autoloader writing to {DEV_SCHEMA}.runz_email_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick Comparison Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count quotes in dev vs prod
# MAGIC SELECT 'Lambda (dev)' as source, count(*) as quote_count 
# MAGIC FROM raw_us_corporates_dev.pretrade_runz_quote_norm
# MAGIC UNION ALL
# MAGIC SELECT 'Prod' as source, count(*) as quote_count 
# MAGIC FROM raw_us_corporates.pretrade_runz_quote_norm
# MAGIC WHERE email_timestamp >= '2026-02-17'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample dev quotes
# MAGIC SELECT * FROM raw_us_corporates_dev.pretrade_runz_quote_norm LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Parser results summary
# MAGIC SELECT 
# MAGIC     status,
# MAGIC     header_source,
# MAGIC     format,
# MAGIC     count(*) as email_count,
# MAGIC     sum(quote_count) as total_quotes
# MAGIC FROM raw_us_corporates_dev.runz_parser_results
# MAGIC GROUP BY status, header_source, format
# MAGIC ORDER BY email_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find emails where Lambda and Prod quote counts differ
# MAGIC -- Requires s3_key in both tables for joining
# MAGIC SELECT 
# MAGIC     d.from_email,
# MAGIC     d.subject,
# MAGIC     d.quote_count as lambda_quotes,
# MAGIC     p_count as prod_quotes,
# MAGIC     d.quote_count - p_count as diff,
# MAGIC     d.header_source,
# MAGIC     d.parsed_header
# MAGIC FROM raw_us_corporates_dev.runz_parser_results d
# MAGIC LEFT JOIN (
# MAGIC     SELECT 
# MAGIC         s3_key, 
# MAGIC         CAST(get_json_object(message, '$.quoteCount') AS INT) as p_count
# MAGIC     FROM raw_us_corporates.runz_parser_results
# MAGIC     WHERE status = 'SUCCESS'
# MAGIC ) p ON d.s3_key = p.s3_key
# MAGIC WHERE d.status = 'SUCCESS'
# MAGIC ORDER BY abs(d.quote_count - p_count) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## DynamoDB Utilities
# MAGIC
# MAGIC Standalone cells for managing the DynamoDB tables used by the parser.
# MAGIC Run these individually as needed — they are **not** part of the batch test flow above.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup: DynamoDB clients

# COMMAND ----------

import boto3

_ddb = boto3.resource("dynamodb", region_name="us-east-1")
_header_table = _ddb.Table("runz-header-mappings-prod")
_review_table = _ddb.Table("runz-header-review-queue-prod")
_dealer_table = _ddb.Table("runz-dealer-config-prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Inspect DynamoDB — Header Mappings
# MAGIC
# MAGIC Browse current header mappings. Use `from_email_filter` to narrow results.

# COMMAND ----------

# Set to a sender email to filter, or None to see all
from_email_filter = None   # e.g. "jmarek10@bloomberg.net"
max_display = 50

from boto3.dynamodb.conditions import Key as DKey

if from_email_filter:
    resp = _header_table.query(KeyConditionExpression=DKey("from_email").eq(from_email_filter))
    items = resp.get("Items", [])
else:
    items = []
    resp = _header_table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = _header_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))

print(f"Header mappings found: {len(items)}")

# Show as a table
if items:
    import pandas as pd
    display_cols = [
        "from_email", "orig_header", "parsed_header",
        "email_format", "created_by", "sample_count",
        "needs_review", "last_seen",
    ]
    rows = [{c: item.get(c, "") for c in display_cols} for item in items]
    display(pd.DataFrame(rows).head(max_display))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Inspect DynamoDB — Review Queue
# MAGIC
# MAGIC Items here were resolved by Claude API or had low-confidence synonym matches.

# COMMAND ----------

review_items = []
resp = _review_table.scan()
review_items.extend(resp.get("Items", []))
while "LastEvaluatedKey" in resp:
    resp = _review_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
    review_items.extend(resp.get("Items", []))

print(f"Review queue items: {len(review_items)}")

if review_items:
    import pandas as pd
    display_cols = [
        "review_id", "from_email", "orig_header", "parsed_header",
        "source", "status", "subject", "created_at",
    ]
    rows = [{c: item.get(c, "") for c in display_cols} for item in review_items]
    display(pd.DataFrame(rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Wipe DynamoDB Tables
# MAGIC
# MAGIC Clears header mappings and review queue so the next Lambda run starts
# MAGIC fresh (Tier 2 synonyms + Tier 3 Claude). **Does NOT touch dealer config.**
# MAGIC
# MAGIC **⚠️ Destructive** — only run this intentionally.

# COMMAND ----------

def wipe_table(table, key_schema):
    """Delete all items from a DynamoDB table."""
    key_names = [k["AttributeName"] for k in key_schema]
    deleted = 0
    scan = table.scan()
    while True:
        with table.batch_writer() as batch:
            for item in scan.get("Items", []):
                batch.delete_item(Key={k: item[k] for k in key_names})
                deleted += 1
        if "LastEvaluatedKey" not in scan:
            break
        scan = table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
    return deleted

# -- Wipe header mappings --
n = wipe_table(_header_table, _header_table.key_schema)
print(f"Deleted {n} header mappings")

# -- Wipe review queue --
n = wipe_table(_review_table, _review_table.key_schema)
print(f"Deleted {n} review queue items")

# -- Verify --
print(f"Header mappings remaining: {_header_table.scan(Select='COUNT')['Count']}")
print(f"Review queue remaining:    {_review_table.scan(Select='COUNT')['Count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Sync Header Mappings → Databricks Delta Table
# MAGIC
# MAGIC Pulls all header mappings from DynamoDB into
# MAGIC `temp_db.runz_header_mappings_sync` for SQL querying.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

# Full scan
items = []
resp = _header_table.scan()
items.extend(resp.get("Items", []))
while "LastEvaluatedKey" in resp:
    resp = _header_table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
    items.extend(resp.get("Items", []))

print(f"Scanned {len(items)} header mappings from DynamoDB")

schema = StructType([
    StructField("from_email",       StringType(),  True),
    StructField("orig_header_hash", StringType(),  True),
    StructField("orig_header",      StringType(),  True),
    StructField("parsed_header",    StringType(),  True),
    StructField("email_format",     StringType(),  True),
    StructField("sample_count",     IntegerType(), True),
    StructField("last_seen",        StringType(),  True),
    StructField("created_by",       StringType(),  True),
    StructField("needs_review",     BooleanType(), True),
])

rows = [
    (
        item.get("from_email", ""),
        item.get("orig_header_hash", ""),
        item.get("orig_header", ""),
        item.get("parsed_header", ""),
        item.get("email_format", ""),
        int(item.get("sample_count", 0)),
        item.get("last_seen", ""),
        item.get("created_by", ""),
        bool(item.get("needs_review", False)),
    )
    for item in items
]

sync_table = "temp_db.runz_header_mappings_sync"
df = spark.createDataFrame(rows, schema)
df.write.format("delta").mode("overwrite").saveAsTable(sync_table)
print(f"Synced {len(rows)} mappings → {sync_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_db.runz_header_mappings_sync
# MAGIC ORDER BY last_seen DESC
