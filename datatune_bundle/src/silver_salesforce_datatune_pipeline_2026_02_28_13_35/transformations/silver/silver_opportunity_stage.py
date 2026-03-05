from pyspark import pipelines as dp
env = spark.conf.get("env", "dev")
@dp.table()
def silver_opportunity_stage():
    df = spark.readStream.table(f"bronze_{env}.salesforce_datatune.opportunitystage")
    df = df.withColumnRenamed("Id", "opportunity_stage_id")\
           .withColumnRenamed("MasterLabel", "master_label")\
           .withColumnRenamed("ApiName", "api_name")\
           .withColumnRenamed("IsActive", "is_active")\
           .withColumnRenamed("SortOrder", "sort_order")\
           .withColumnRenamed("IsClosed", "is_closed")\
           .withColumnRenamed("IsWon", "is_won")\
           .withColumnRenamed("ForecastCategory", "forecast_category")\
           .withColumnRenamed("ForecastCategoryName", "forecast_category_name")\
           .withColumnRenamed("DefaultProbability", "default_probability")\
           .withColumnRenamed("Description", "description")\
           .withColumnRenamed("CreatedById", "created_by_id")\
           .withColumnRenamed("CreatedDate", "created_date")\
           .withColumnRenamed("LastModifiedById", "last_modified_by_id")\
           .withColumnRenamed("LastModifiedDate", "last_modified_date")\
           .withColumnRenamed("SystemModstamp", "system_modstamp")
    df = df.dropDuplicates(["opportunity_stage_id"])
    return df
