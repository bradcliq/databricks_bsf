# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest Lambda Test Quotes → temp_db.runz_lambda_normalized_quotes
# MAGIC
# MAGIC Batch-reads all JSON files under `runz_lambda_test/pretrade_quotes/` (all date
# MAGIC subfolders) and writes them into the Delta table created by
# MAGIC `NB_Create_Lambda_Test_Table`.
# MAGIC
# MAGIC **Mode: overwrite** — safe to re-run; replaces all rows from previous runs.
# MAGIC Switch to `append` if you want incremental loads instead.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)
from pyspark.sql.functions import lit

# -- Config --
output_bucket  = "use1-s3-bcq-prod-elt-raw"
source_path    = f"s3://{output_bucket}/runz_lambda_test/pretrade_quotes/"
target_table   = "temp_db.runz_lambda_normalized_quotes"
write_mode     = "overwrite"   # change to "append" for incremental loads

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read all JSON files (all date subfolders)

# COMMAND ----------

schema = StructType([
    StructField("cusip",               StringType(), True),
    StructField("isin",                StringType(), True),
    StructField("tkr",                 StringType(), True),
    StructField("cpn",                 StringType(), True),
    StructField("mty",                 StringType(), True),
    StructField("side",                StringType(), True),
    StructField("price",               DoubleType(), True),
    StructField("spread",              DoubleType(), True),
    StructField("yield",               StringType(), True),
    StructField("quantity",            DoubleType(), True),
    StructField("firm",                StringType(), True),
    StructField("from_email",          StringType(), True),
    StructField("email_id",            StringType(), True),
    StructField("quote_id",            StringType(), True),
    StructField("quote_timestamp",     LongType(),   True),
    StructField("added_timestamp",     LongType(),   True),
    StructField("processing_instance", StringType(), True),
    StructField("file_path",           StringType(), True),
])

df = (
    spark.read
    .format("json")
    .schema(schema)
    .option("recursiveFileLookup", "true")   # descends into date subfolders
    .option("mode", "PERMISSIVE")            # bad records → nulls, not failures
    .load(source_path)
    # _rescued_data: columns not in schema land here (mirrors Autoloader behaviour)
    .withColumn("_rescued_data", lit(None).cast(StringType()))
)

print(f"Records read: {df.count():,}")
display(df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write to Delta table

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode(write_mode)
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Done. Mode='{write_mode}' → {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify

# COMMAND ----------

final_count = spark.table(target_table).count()
print(f"Total rows in {target_table}: {final_count:,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     processing_instance,
# MAGIC     COUNT(*)          AS quote_count,
# MAGIC     COUNT(DISTINCT email_id) AS email_count,
# MAGIC     MIN(from_timestamp(quote_timestamp / 1000))  AS earliest_quote,
# MAGIC     MAX(from_timestamp(quote_timestamp / 1000))  AS latest_quote
# MAGIC FROM temp_db.runz_lambda_normalized_quotes
# MAGIC GROUP BY processing_instance
# MAGIC ORDER BY quote_count DESC
