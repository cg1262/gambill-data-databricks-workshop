catalog = "silver_prod"  # Substitute your catalog and environment
new_schemas = ["sales_ops","sales","marketing"]
 
for schema in new_schemas:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
 