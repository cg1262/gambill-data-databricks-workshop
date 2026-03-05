from pyspark import pipelines as dp

env = spark.conf.get("env", "dev")
@dp.table()
@dp.expect_or_drop("amount_valid", "amount IS NOT NULL AND amount > 0")
def silver_opportunity():
    df = spark.readStream.table(f"bronze_{env}.salesforce_datatune.opportunity")
    df = df.withColumnRenamed("Id", "opportunity_id") \
           .withColumnRenamed("IsDeleted", "is_deleted") \
           .withColumnRenamed("AccountId", "account_id") \
           .withColumnRenamed("IsPrivate", "is_private") \
           .withColumnRenamed("Name", "name") \
           .withColumnRenamed("Description", "description") \
           .withColumnRenamed("StageName", "stage_name") \
           .withColumnRenamed("Amount", "amount") \
           .withColumnRenamed("Probability", "probability") \
           .withColumnRenamed("ExpectedRevenue", "expected_revenue") \
           .withColumnRenamed("TotalOpportunityQuantity", "total_opportunity_quantity") \
           .withColumnRenamed("CloseDate", "close_date") \
           .withColumnRenamed("Type", "type") \
           .withColumnRenamed("NextStep", "next_step") \
           .withColumnRenamed("LeadSource", "lead_source") \
           .withColumnRenamed("IsClosed", "is_closed") \
           .withColumnRenamed("IsWon", "is_won") \
           .withColumnRenamed("ForecastCategory", "forecast_category") \
           .withColumnRenamed("ForecastCategoryName", "forecast_category_name") \
           .withColumnRenamed("CampaignId", "campaign_id") \
           .withColumnRenamed("HasOpportunityLineItem", "has_opportunity_line_item") \
           .withColumnRenamed("Pricebook2Id", "pricebook2_id") \
           .withColumnRenamed("OwnerId", "owner_id") \
           .withColumnRenamed("CreatedDate", "created_date") \
           .withColumnRenamed("CreatedById", "created_by_id") \
           .withColumnRenamed("LastModifiedDate", "last_modified_date") \
           .withColumnRenamed("LastModifiedById", "last_modified_by_id") \
           .withColumnRenamed("SystemModstamp", "system_modstamp") \
           .withColumnRenamed("LastActivityDate", "last_activity_date") \
           .withColumnRenamed("PushCount", "push_count") \
           .withColumnRenamed("LastStageChangeDate", "last_stage_change_date") \
           .withColumnRenamed("FiscalQuarter", "fiscal_quarter") \
           .withColumnRenamed("FiscalYear", "fiscal_year") \
           .withColumnRenamed("Fiscal", "fiscal") \
           .withColumnRenamed("ContactId", "contact_id") \
           .withColumnRenamed("LastViewedDate", "last_viewed_date") \
           .withColumnRenamed("LastReferencedDate", "last_referenced_date") \
           .withColumnRenamed("HasOpenActivity", "has_open_activity") \
           .withColumnRenamed("HasOverdueTask", "has_overdue_task") \
           .withColumnRenamed("LastAmountChangedHistoryId", "last_amount_changed_history_id") \
           .withColumnRenamed("LastCloseDateChangedHistoryId", "last_close_date_changed_history_id") \
           .withColumnRenamed("DeliveryInstallationStatus__c", "delivery_installation_status_c") \
           .withColumnRenamed("TrackingNumber__c", "tracking_number_c") \
           .withColumnRenamed("OrderNumber__c", "order_number_c") \
           .withColumnRenamed("CurrentGenerators__c", "current_generators_c") \
           .withColumnRenamed("MainCompetitors__c", "main_competitors_c")
    df = df.dropDuplicates(["opportunity_id"])
    return df
