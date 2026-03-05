CREATE OR REFRESH STREAMING TABLE ST_Silver_Account
(
  CONSTRAINT valid_account EXPECT (Id IS NOT NULL AND Name IS NOT NULL AND IsDeleted = false) ON VIOLATION DROP ROW
)
AS
SELECT *
FROM STREAM bronze_dev.salesforce_datatune.account;
