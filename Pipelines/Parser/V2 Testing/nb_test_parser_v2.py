# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC # RUNZ Parser Migration: Populate DynamoDB from Existing Data
# MAGIC
# MAGIC Run this ONCE before cutover to pre-populate:
# MAGIC 1. Header mappings from successful parser results
# MAGIC 2. Header mappings from the JPM regex mapping table
# MAGIC 3. Dealer config from the dealers table

# COMMAND ----------

import boto3
import hashlib
import json
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
header_table = dynamodb.Table("runz-header-mappings-prod")
dealer_table = dynamodb.Table("runz-dealer-config-prod")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Migrate Header Mappings from Successful Parser Results
# MAGIC
# MAGIC Every SUCCESS row in `runz_parser_results` has origHeader and parsedHeader.

# COMMAND ----------

# Extract unique successful header mappings
from pyspark.sql.functions import col

migration_df = spark.sql("""
    WITH ranked AS (
        SELECT 
            get_json_object(message, '$.mailTrader') as from_email,
            get_json_object(message, '$.origHeader') as orig_header,
            get_json_object(message, '$.parsedHeader') as parsed_header,
            get_json_object(message, '$.quoteType') as quote_type,
            to_timestamp(email_timestamp, 'd MMM yyyy HH:mm:ss Z') as email_ts,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    get_json_object(message, '$.mailTrader'),
                    get_json_object(message, '$.origHeader')
                ORDER BY to_timestamp(email_timestamp, 'd MMM yyyy HH:mm:ss Z') DESC
            ) as rn
        FROM raw_us_corporates.runz_parser_results
        WHERE status = 'SUCCESS'
        AND message IS NOT NULL
        AND get_json_object(message, '$.origHeader') IS NOT NULL
        AND get_json_object(message, '$.parsedHeader') IS NOT NULL
        AND to_timestamp(email_timestamp, 'd MMM yyyy HH:mm:ss Z') >= date_sub(current_date(), 90)
    )
    SELECT from_email, orig_header, parsed_header, quote_type
    FROM ranked
    WHERE rn = 1
    ORDER BY from_email
""")

print(f"Found {migration_df.count()} unique header mappings from last 90 days")
display(migration_df.limit(20))
 

# COMMAND ----------

# Write to DynamoDB
rows = migration_df.collect()
migrated = 0
errors = 0

for row in rows:
    try:
        from_email = row.from_email or ""
        orig_header = row.orig_header or ""
        parsed_header = row.parsed_header or ""

        if not from_email or not orig_header or not parsed_header:
            continue

        # Compute hash
        normalized = "^".join(
            h.strip().lower() for h in orig_header.split("^")
        )
        header_hash = hashlib.sha256(
            normalized.encode("utf-8")
        ).hexdigest()[:32]

        header_table.put_item(
            Item={
                "from_email": from_email,
                "orig_header_hash": header_hash,
                "orig_header": orig_header,
                "parsed_header": parsed_header,
                "email_format": "html_table",  # Core parser was HTML table
                "sample_count": int(row.sample_count),
                "last_seen": datetime.now(timezone.utc).isoformat(),
                "created_by": "migrated",
                "needs_review": False,
            }
        )
        migrated += 1
    except Exception as e:
        errors += 1
        if errors <= 5:
            print(f"Error migrating {row.from_email}: {e}")

print(f"Migrated {migrated} header mappings ({errors} errors)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Migrate JPM Header Regex Mappings

# COMMAND ----------

jpm_df = spark.sql("""
    SELECT traderEmail, origHeader, parsedHeader, regex
    FROM trusted_us_corporates.quotes_header_regex_mapping
""")

print(f"Found {jpm_df.count()} JPM header mappings")
display(jpm_df)

# COMMAND ----------

jpm_rows = jpm_df.collect()
jpm_migrated = 0

for row in jpm_rows:
    try:
        from_email = row.traderEmail or ""
        orig_header = row.origHeader or ""
        parsed_header = row.parsedHeader or ""

        if not from_email or not orig_header:
            continue

        normalized = "^".join(
            h.strip().lower() for h in orig_header.split("~")
        )
        header_hash = hashlib.sha256(
            normalized.encode("utf-8")
        ).hexdigest()[:32]

        item = {
            "from_email": from_email,
            "orig_header_hash": header_hash,
            "orig_header": orig_header.replace("~", "^"),
            "parsed_header": parsed_header.replace("~", "^"),
            "email_format": "space_delim",  # JPM parser was space-delimited
            "sample_count": 1,
            "last_seen": datetime.now(timezone.utc).isoformat(),
            "created_by": "migrated_jpm",
            "needs_review": False,
        }

        # Include regex overrides if present
        if row.regex:
            try:
                regex_data = json.loads(row.regex) if isinstance(row.regex, str) else row.regex
                item["regex_overrides"] = regex_data
            except Exception:
                pass

        header_table.put_item(Item=item)
        jpm_migrated += 1
    except Exception as e:
        print(f"Error migrating JPM {row.traderEmail}: {e}")

print(f"Migrated {jpm_migrated} JPM header mappings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Migrate Dealer Configuration
# MAGIC
# MAGIC Extract unique dealers and their known configs.

# COMMAND ----------

dealers_df = spark.sql("""
    SELECT email as from_email, firm, user as display_name
    FROM trusted_us_corporates.dealers
    WHERE email IS NOT NULL
""")

print(f"Found {dealers_df.count()} dealers")
display(dealers_df.limit(20))


# COMMAND ----------

# Known dealer-specific configs (from hardcoded logic in the parser)
SPECIAL_CONFIGS = {
    # TD/TDSAT: skip zero spreads
    "ang399@bloomberg.net": {"skip_zero_spread": True},
    "tdsat-ops@tdsecurities.com": {
        "skip_zero_spread": True,
        "is_csv_source": True,
    },
    # NTG: CSV source
    "ntgmops@ntglobalmarkets.com": {"is_csv_source": True},
    # pharsanyi: custom size divisor
    "pharsanyi3@bloomberg.net": {"custom_size_divisor": 1000},
}

dealer_rows = dealers_df.collect()
dealer_migrated = 0

for row in dealer_rows:
    try:
        from_email = row.from_email or ""
        if not from_email:
            continue

        item = {
            "from_email": from_email,
            "firm": row.firm,
            "display_name": row.display_name,
            "size_unit": "millions",
            "size_threshold": 27,
            "skip_zero_spread": False,
            "is_csv_source": False,
            "active": True,
        }

        # Apply special configs
        special = SPECIAL_CONFIGS.get(from_email, {})
        item.update(special)

        dealer_table.put_item(Item=item)
        dealer_migrated += 1
    except Exception as e:
        print(f"Error migrating dealer {row.from_email}: {e}")

print(f"Migrated {dealer_migrated} dealer configs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification
# MAGIC
# MAGIC Quick scan to verify data was written.

# COMMAND ----------

# Verify header mappings
response = header_table.scan(Select="COUNT")
print(f"Header mappings in DynamoDB: {response['Count']}")

response = dealer_table.scan(Select="COUNT")
print(f"Dealer configs in DynamoDB: {response['Count']}")


# COMMAND ----------

import boto3

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

# Wipe header mappings
header_table = dynamodb.Table("runz-header-mappings-prod")
scan = header_table.scan()
with header_table.batch_writer() as batch:
    for item in scan["Items"]:
        batch.delete_item(Key={
            "from_email": item["from_email"],
            "orig_header_hash": item["orig_header_hash"]
        })
    while "LastEvaluatedKey" in scan:
        scan = header_table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
        for item in scan["Items"]:
            batch.delete_item(Key={
                "from_email": item["from_email"],
                "orig_header_hash": item["orig_header_hash"]
            })

print(f"Wiped header mappings table")

# Verify
response = header_table.scan(Select="COUNT")
print(f"Header mappings remaining: {response['Count']}")

# COMMAND ----------

import boto3

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("runz-header-mappings-prod")

response = table.scan(Select="COUNT")
print(f"Header mappings: {response['Count']}")

# COMMAND ----------

response = table.scan(Limit=10)
for item in response["Items"]:
    print("---")
    for key, val in item.items():
        print(f"  {key:25s} | {str(val)[:100]}")

# COMMAND ----------

# Just wipe headers and run dealers only
import boto3

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

# Wipe header mappings
header_table = dynamodb.Table("runz-header-mappings-prod")
scan = header_table.scan()
with header_table.batch_writer() as batch:
    for item in scan["Items"]:
        batch.delete_item(Key={
            "from_email": item["from_email"],
            "orig_header_hash": item["orig_header_hash"]
        })
    while "LastEvaluatedKey" in scan:
        scan = header_table.scan(ExclusiveStartKey=scan["LastEvaluatedKey"])
        for item in scan["Items"]:
            batch.delete_item(Key={
                "from_email": item["from_email"],
                "orig_header_hash": item["orig_header_hash"]
            })

response = header_table.scan(Select="COUNT")
print(f"Header mappings remaining: {response['Count']}")