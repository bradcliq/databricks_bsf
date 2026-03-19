# Databricks notebook source
# MAGIC %md
# MAGIC # Create: temp_db.runz_lambda_normalized_quotes
# MAGIC
# MAGIC Run **once** to create the target Delta table for Lambda POC output.
# MAGIC Schema mirrors `raw_us_corporates.pretrade_runz_quote_norm`.
# MAGIC
# MAGIC Safe to re-run — uses `CREATE TABLE IF NOT EXISTS`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS temp_db

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS temp_db.runz_lambda_normalized_quotes (
# MAGIC   cusip               STRING,
# MAGIC   isin                STRING,
# MAGIC   tkr                 STRING,
# MAGIC   cpn                 STRING,
# MAGIC   mty                 STRING,
# MAGIC   side                STRING,
# MAGIC   price               DOUBLE,
# MAGIC   spread              DOUBLE,
# MAGIC   yield               STRING,
# MAGIC   quantity            DOUBLE,
# MAGIC   firm                STRING,
# MAGIC   from_email          STRING,
# MAGIC   email_id            STRING,
# MAGIC   quote_id            STRING,
# MAGIC   quote_timestamp     BIGINT,
# MAGIC   added_timestamp     BIGINT,
# MAGIC   processing_instance STRING,
# MAGIC   file_path           STRING,
# MAGIC   _rescued_data       STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Lambda POC parsed quotes — ingested from s3://use1-s3-bcq-prod-elt-raw/runz_lambda_test/pretrade_quotes/'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED temp_db.runz_lambda_normalized_quotes
