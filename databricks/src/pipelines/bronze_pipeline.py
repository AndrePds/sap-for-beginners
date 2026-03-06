# bronze_pipeline.py  –  SAP Medallion Bronze Layer
#
# Reads JSONL files using Auto Loader (one sub-folder per SAP table).
# Writes raw Delta streaming tables to  catalog.bronze_sap.bronze_<table>.
#
# DEPENDENCY: bronze_schemas.py must be listed BEFORE this file in the
# pipeline `libraries` so that BRONZE_SCHEMAS and ALL_BRONZE_TABLES are
# already defined in the DLT execution scope.
#
# Metadata columns added to every Bronze table:
#   _ingestion_time  –  when the row was ingested
#   _source_file     –  full path of the JSONL source file
#
# Paths are injected via spark.conf (configured in pipeline_bronze.yml):
#   source_base_path  –  root folder; sub-folders match SAP table names
#   schema_location   –  checkpoint path for Auto Loader schema evolution

import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

SOURCE_BASE = spark.conf.get("source_base_path")
SCHEMA_LOC  = spark.conf.get("schema_location")


def _register_bronze(table_name: str, schema):
    """
    Factory that registers one DLT streaming table per SAP source table.

    Auto Loader continuously monitors  {SOURCE_BASE}/{table_name}/
    and ingests new JSONL files incrementally.  The schema checkpoint at
    {SCHEMA_LOC}/{table_name}  tracks column evolution over time.

    When an explicit schema is provided (from BRONZE_SCHEMAS), it is
    enforced at read time.  For INFERRED_TABLES, cloudFiles infers types
    automatically from the first batch of files.
    """
    @dlt.table(
        name=f"bronze_{table_name}",
        comment=(
            f"Raw SAP {table_name.upper()} ingested from JSONL source files. "
            f"Append-only. No business logic. One row per source record."
        ),
        table_properties={
            "quality":   "bronze",
            "sap.table": table_name.upper(),
            "sap.mandt": "100",
        },
    )
    def _table():
        use_inference = schema is None
        reader = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",          "json")
            .option("cloudFiles.schemaLocation",  f"{SCHEMA_LOC}/{table_name}")
            .option("cloudFiles.inferColumnTypes", str(use_inference).lower())
        )
        if schema is not None:
            reader = reader.schema(schema)

        return (
            reader
            .load(f"{SOURCE_BASE}/{table_name}/")
            .withColumn("_ingestion_time", current_timestamp())
            .withColumn("_source_file",    input_file_name())
        )

    return _table


# Register all 41 SAP tables declared in bronze_schemas.py
for _tname in ALL_BRONZE_TABLES:
    _register_bronze(_tname, BRONZE_SCHEMAS.get(_tname))
