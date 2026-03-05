from pyspark import pipelines as dp
env = spark.conf.get("env", "dev")
@dp.table()
def silver_opportunity_line_item():
    df = spark.readStream.table(f"bronze_{env}.salesforce_datatune.opportunitylineitem")
    df = df.withColumnRenamed("Id", "opportunity_line_item_id") \
           .withColumnRenamed("OpportunityId", "opportunity_id") \
           .withColumnRenamed("SortOrder", "sort_order") \
           .withColumnRenamed("PricebookEntryId", "pricebook_entry_id") \
           .withColumnRenamed("Product2Id", "product2_id") \
           .withColumnRenamed("ProductCode", "product_code") \
           .withColumnRenamed("Name", "name") \
           .withColumnRenamed("Quantity", "quantity") \
           .withColumnRenamed("TotalPrice", "total_price") \
           .withColumnRenamed("UnitPrice", "unit_price") \
           .withColumnRenamed("ListPrice", "list_price") \
           .withColumnRenamed("ServiceDate", "service_date") \
           .withColumnRenamed("Description", "description") \
           .withColumnRenamed("CreatedDate", "created_date") \
           .withColumnRenamed("CreatedById", "created_by_id") \
           .withColumnRenamed("LastModifiedDate", "last_modified_date") \
           .withColumnRenamed("LastModifiedById", "last_modified_by_id") \
           .withColumnRenamed("SystemModstamp", "system_modstamp") \
           .withColumnRenamed("IsDeleted", "is_deleted") \
           .withColumnRenamed("LastViewedDate", "last_viewed_date") \
           .withColumnRenamed("LastReferencedDate", "last_referenced_date")
    df = df.dropDuplicates(["opportunity_line_item_id"])
    return df
