# Databricks notebook source
# MAGIC %md
# MAGIC # RUNZ Parser — Databricks Integration
# MAGIC
# MAGIC This notebook handles:
# MAGIC 1. **Autoloader** for parsed quotes from Lambda (S3 → Delta table)
# MAGIC 2. **Autoloader** for parser results/metadata from Lambda
# MAGIC 3. **Autoloader** for email catalog entries from Lambda
# MAGIC 4. **DynamoDB sync** — periodic read of header mappings for review/query

# COMMAND ----------

db_raw = "raw_us_corporates"
db_trusted = "trusted_us_corporates"
output_bucket = "use1-s3-bcq-prod-elt-raw"
checkpoint_base = f"s3://{output_bucket}/raw_us_corporates/checkpoints"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Autoloader: Parsed Quotes
# MAGIC Reads JSON files written by Lambda → loads into pretrade_runz_quote_norm

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

quotes_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/quotes_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"s3://{output_bucket}/pretrade_quotes_prod/")
)

# Add ingestion timestamp
quotes_df = quotes_df.withColumn("_ingested_at", current_timestamp())

(
    quotes_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/pretrade_runz_quote_norm")
    .option("mergeSchema", "true")
    .queryName("runz_quotes_autoloader")
    .start(f"s3://{output_bucket}/raw_us_corporates/pretrade_runz_quote_norm/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Autoloader: Parser Results (Metadata)

# COMMAND ----------

results_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/results_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"s3://{output_bucket}/runz_parser_results/")
)

results_df = results_df.withColumn("_ingested_at", current_timestamp())

(
    results_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/runz_parser_results")
    .option("mergeSchema", "true")
    .queryName("runz_results_autoloader")
    .start(f"s3://{output_bucket}/raw_us_corporates/tbl_delta_runz_parser_results/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Autoloader: Email Catalog
# MAGIC Replaces the old `runz_email_autoloader_data2` table.

# COMMAND ----------

catalog_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{checkpoint_base}/catalog_schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(f"s3://{output_bucket}/runz_email_catalog/")
)

catalog_df = catalog_df.withColumn("_ingested_at", current_timestamp())

(
    catalog_df.writeStream
    .format("delta")
    .option("checkpointLocation", f"{checkpoint_base}/runz_email_catalog")
    .option("mergeSchema", "true")
    .queryName("runz_catalog_autoloader")
    .start(f"s3://{output_bucket}/raw_us_corporates/runz_email_catalog/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. DynamoDB Sync — Header Mappings
# MAGIC
# MAGIC Run this on a schedule (e.g., hourly) to sync DynamoDB header mappings
# MAGIC into a Delta table for easy SQL querying and review.
# MAGIC
# MAGIC Uses boto3 directly — no external connector needed.

# COMMAND ----------

import boto3
import json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)

def sync_header_mappings():
    """Scan DynamoDB header mappings table and write to Delta."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("runz-header-mappings-prod")

    # Full scan (small table, <1000 rows)
    items = []
    response = table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    print(f"Scanned {len(items)} header mappings from DynamoDB")

    # Convert to Spark DataFrame
    schema = StructType([
        StructField("from_email", StringType(), True),
        StructField("orig_header_hash", StringType(), True),
        StructField("orig_header", StringType(), True),
        StructField("parsed_header", StringType(), True),
        StructField("email_format", StringType(), True),
        StructField("sample_count", IntegerType(), True),
        StructField("last_seen", StringType(), True),
        StructField("created_by", StringType(), True),
        StructField("needs_review", BooleanType(), True),
    ])

    rows = []
    for item in items:
        rows.append((
            item.get("from_email", ""),
            item.get("orig_header_hash", ""),
            item.get("orig_header", ""),
            item.get("parsed_header", ""),
            item.get("email_format", ""),
            int(item.get("sample_count", 0)),
            item.get("last_seen", ""),
            item.get("created_by", ""),
            bool(item.get("needs_review", False)),
        ))

    df = spark.createDataFrame(rows, schema)

    # Overwrite the sync table
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{db_trusted}.runz_header_mappings_sync")
    )
    print(f"Synced {len(rows)} mappings to {db_trusted}.runz_header_mappings_sync")

# Run the sync
sync_header_mappings()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Review Queue Sync
# MAGIC
# MAGIC Same approach for the review queue — sync to Databricks for easy review.

# COMMAND ----------

def sync_review_queue():
    """Scan DynamoDB review queue and write to Delta."""
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("runz-header-review-queue-prod")

    items = []
    response = table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    print(f"Scanned {len(items)} review items from DynamoDB")

    schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("from_email", StringType(), True),
        StructField("orig_header", StringType(), True),
        StructField("parsed_header", StringType(), True),
        StructField("sample_data", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("source", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", StringType(), True),
    ])

    rows = []
    for item in items:
        rows.append((
            item.get("review_id", ""),
            item.get("from_email", ""),
            item.get("orig_header", ""),
            item.get("parsed_header", ""),
            item.get("sample_data", ""),
            item.get("subject", ""),
            item.get("source", ""),
            item.get("status", ""),
            item.get("created_at", ""),
        ))

    df = spark.createDataFrame(rows, schema)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{db_trusted}.runz_header_review_queue")
    )
    print(f"Synced {len(rows)} review items")

sync_review_queue()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Useful Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all header mappings, ordered by most recently seen
# MAGIC SELECT from_email, orig_header, parsed_header, created_by, sample_count, last_seen, needs_review
# MAGIC FROM trusted_us_corporates.runz_header_mappings_sync
# MAGIC ORDER BY last_seen DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View pending reviews (Claude-generated or low-confidence mappings)
# MAGIC SELECT *
# MAGIC FROM trusted_us_corporates.runz_header_review_queue
# MAGIC WHERE status = 'pending'
# MAGIC ORDER BY created_at DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Today's parser results
# MAGIC SELECT email_id, from_email, status,
# MAGIC        get_json_object(message, '$.mailSubject') as subject,
# MAGIC        get_json_object(message, '$.actualCt') as quote_count,
# MAGIC        get_json_object(message, '$.headerSource') as header_source,
# MAGIC        get_json_object(message, '$.message') as result_message
# MAGIC FROM raw_us_corporates.tbl_delta_runz_parser_results
# MAGIC WHERE _ingested_at >= current_date()
# MAGIC ORDER BY _ingested_at DESC
# MAGIC LIMIT 50
