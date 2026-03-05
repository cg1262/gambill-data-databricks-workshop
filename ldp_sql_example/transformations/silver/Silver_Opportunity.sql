CREATE OR REFRESH STREAMING TABLE ST_Silver_Opportunity
(
  CONSTRAINT valid_opportunity EXPECT (Id IS NOT NULL AND AccountId IS NOT NULL AND Name IS NOT NULL AND Amount > 0 AND IsDeleted = false) ON VIOLATION DROP ROW
)
AS
SELECT *
FROM STREAM bronze_dev.salesforce_datatune.opportunity;
