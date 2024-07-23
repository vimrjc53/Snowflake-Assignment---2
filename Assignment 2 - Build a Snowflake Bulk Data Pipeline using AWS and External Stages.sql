

1.GOALS
/*In this project, we will learn how to use snowflake as a query engine. 
  We store our data from snowflake to aws s3 and we will learn various methods to query it from snowflake.*/
-- A. Query data in s3 from snowflake.
-- B. Create view over data in aws s3.
-- C. Disadvantages and advantages of this approach.

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE DATABASE DEMO_DB;

2.PREPARATION
/*Before we start, let’s establish connection between snowflake and s3, then
  upload some sample data from snowflake to s3. */ 

-- Create integration object with s3 
create or replace storage integration S3_INTEG_CUS_DATA_CSV
type = external_stage
storage_provider = s3
enabled = true
storage_aws_role_arn = 'arn:aws:iam::730335494857:role/aws-ext-bulkdata-pipeline-a2'
storage_allowed_locations = ('s3://snowflake-aws-ext-bulkdata-pipeline-a2/aws-cus-data-csv-a2/');
  
-- Create csv file format
create or replace file format demo_db.public.cus_data_csv_format
type = 'csv';

-- Create external stage object
create or replace stage demo_db.public.cus_data_ext_csv_stage
URL = 's3://snowflake-aws-ext-bulkdata-pipeline-a2/aws-cus-data-csv-a2/'
STORAGE_INTEGRATION = S3_INTEG_CUS_DATA_CSV
file_format = demo_db.public.cus_data_csv_format;
  
-- Describe integration object to fetch external_id and to be used in s3 Trust Partnership
DESC INTEGRATION S3_INTEG_CUS_DATA_CSV;

CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.CUSTOMER_TEST 
AS 
SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER";

-- Execute below copy command to copy snowflake customer data to s3,
COPY INTO 
@demo_db.public.cus_data_ext_csv_stage/aws-cus-data-csv-a2/
FROM demo_db.public.customer_test;

3.QUERY DATA IN S3 FROM SNOWFLAKE.
/*Now data got uploaded to s3. We have 100 Million records uploaded and data size is 4.5 GB. 
  Uploaded files will be csv compressed files.
  Let’s try to query this data in s3 from snowflake.*/
-- Unload data from s3, query in snowflake
-- Query-1d 01b5dae8-0001-27d8-0000-00054c2a23d9

SELECT 
$1 C_CUSTOMER_SK, 
$2 C_CUSTOMER_ID , 
$3 C_CURRENT_CDEMO_SK , 
$4 C_CURRENT_HDEMO_SK , 
$5 C_CURRENT_ADDR_SK , 
$6 C_FIRST_SHIPTO_DATE_SK , 
$7 C_FIRST_SALES_DATE_SK , 
$8 C_SALUTATION , 
$9 C_FIRST_NAME , 
$10 C_LAST_NAME, 
$11 C_PREFERRED_CUST_FLAG , 
$12 C_BIRTH_DAY , 
$13 C_BIRTH_MONTH , 
$14 C_BIRTH_YEAR,
$15 C_BIRTH_COUNTRY,
$16 C_LOGIN , 
$17 C_EMAIL_ADDRESS , 
$18 C_LAST_REVIEW_DATE 
FROM @demo_db.public.cus_data_ext_csv_stage/aws-cus-data-csv-a2/ ---replace it with new stage 
(file_format => demo_db.public.cus_data_csv_format);

-- Query ID: 01b5daef-0001-27da-0000-00054c2a3449
SELECT 
$1 C_CUSTOMER_SK, 
$2 C_CUSTOMER_ID , 
$3 C_CURRENT_CDEMO_SK , 
$4 C_CURRENT_HDEMO_SK , 
$5 C_CURRENT_ADDR_SK , 
$6 C_FIRST_SHIPTO_DATE_SK , 
$7 C_FIRST_SALES_DATE_SK , 
$8 C_SALUTATION , 
$9 C_FIRST_NAME , 
$10 C_LAST_NAME, 
$11 C_PREFERRED_CUST_FLAG , 
$12 C_BIRTH_DAY , 
$13 C_BIRTH_MONTH , 
$14 C_BIRTH_YEAR,
$15 C_BIRTH_COUNTRY,
$16 C_LOGIN , 
$17 C_EMAIL_ADDRESS , 
$18 C_LAST_REVIEW_DATE 
FROM @demo_db.public.cus_data_ext_csv_stage ---replace it with new stage 
(file_format => demo_db.public.cus_data_csv_format)
WHERE C_CUSTOMER_SK ='64596949';

-- Execute group by,
-- Query - 01b5daf4-0001-27d8-0000-00054c2a2441
SELECT $9 C_FIRST_NAME,$10 C_LAST_NAME,COUNT(*) 
FROM @demo_db.public.cus_data_ext_csv_stage/ 
(file_format => DEMO_DB.PUBLIC.CUS_DATA_CSV_FORMAT) 
GROUP BY $9,$10;

4.CREATE VIEW OVER S3 DATA

CREATE OR REPLACE VIEW V_CUSTOMER_DATA 
AS 
SELECT 
$1 C_CUSTOMER_SK, 
$2 C_CUSTOMER_ID , 
$3 C_CURRENT_CDEMO_SK , 
$4 C_CURRENT_HDEMO_SK , 
$5 C_CURRENT_ADDR_SK , 
$6 C_FIRST_SHIPTO_DATE_SK , 
$7 C_FIRST_SALES_DATE_SK , 
$8 C_SALUTATION , 
$9 C_FIRST_NAME , 
$10 C_LAST_NAME, 
$11 C_PREFERRED_CUST_FLAG , 
$12 C_BIRTH_DAY , 
$13 C_BIRTH_MONTH , 
$14 C_BIRTH_YEAR,
$15 C_BIRTH_COUNTRY,
$16 C_LOGIN , 
$17 C_EMAIL_ADDRESS , 
$18 C_LAST_REVIEW_DATE 
FROM @demo_db.public.cus_data_ext_csv_stage/aws-cus-data-csv-a2/ ---replace it with new stage 
(file_format => demo_db.public.cus_data_csv_format);

-- Query data directly on view,
select * from V_CUSTOMER_DATA;

Now we can directly query data from s3 through view. What is the disadvantage of using this approach ? 
Can you see partitions being scanned in the backend ?
View only look data from table and cannot store data itself. In this case,
V_CUSTOMER_DATA not scanning for partition. 

Considering Query Performance, Since we are Querying from a simple view, 
DML statements are allowed and data retrieval time minimum. 
But in complex view, group functions for aggregations, Distinct, joins, group by can take more time than querying directly from the table.

Dependency & Maintenance also occurs because VIEWS are dependent on the underlying table structures and we cannot do SCHEMABINDING to prevent changes in main table. 

We can query only SHOW VIEWS and SELECT GET_DDL ('VIEW', 'V_CUSTOMER_DATA') function to tracking and managing schema changes.


/*Now let’s try to Join the view we created with a table on snowflake*/
-- Create a sample snowflake table as below

CREATE OR REPLACE TRANSIENT TABLE CUSTOMER_SNOWFLAKE_TRA_TABLE 
AS 
SELECT * FROM CUSTOMER_TEST LIMIT 10000;

SELECT * FROM CUSTOMER_SNOWFLAKE_TRA_TABLE;

-- Join this with the view we created earlier,
-- Query - 01b5db05-0001-27da-0000-00054c2a3515
SELECT B.* FROM CUSTOMER_SNOWFLAKE_TRA_TABLE B 
LEFT OUTER JOIN
V_CUSTOMER_DATA A ON 
A.C_CUSTOMER_SK = B.C_CUSTOMER_SK; 

Now we successfully joined data in s3 with snowflake table. It may look simple but this 
approach has lot of potential. Can you mention few below, page and observe the execution plan.
Initially, we established connection between snowflake and s3 using storage integration and uploaded CUSTOMER_TEST data from snowflake to s3 with help of external stage.
In the above case, DEMO_DB.CUSTOMER_TEST data designed to query data from the perspective one-to-many relationship. So are fetch data from TRANSIENT table with limit 10000 and VIEW by using LEFT OUTER JOIN.
How many partitions got scanned from snowflake table :  355 

5.UNLOAD DATA BACK TO S3
/*This approach leverages micro partitions in snowflake for lookup table still giving 
  us the freedom to query data which we have stored in s3.
  Once we are done looking up we can copy data back to s3 with new derived lookup column.*/

-- We will try to query joined data in s3 and snowflake.*/
-- Query - 01b5db0d-0001-27d8-0000-00054c2a2515
COPY INTO @demo_db.public.cus_data_ext_csv_stage/aws-cus-joined-data-upload-csv-a2/
from
(
SELECT B.* FROM CUSTOMER_SNOWFLAKE_TRA_TABLE B 
LEFT OUTER JOIN
V_CUSTOMER_DATA A ON 
A.C_CUSTOMER_SK = B.C_CUSTOMER_SK
);

6.ADVANTAGES AND DISADVANTAGES
Advantages of using Snowflake querying from S3:
Scalability: Snowflake's cloud-based architecture allows it to scale up or down to handle large amounts of data from S3, making it an ideal choice for big data analytics & reporting.
Performance: Snowflake's columnar storage and parallel processing capabilities enable fast query performance, even on large datasets from S3.
Security: Snowflake provides robust security features, such as encryption and access controls, to ensure that data from S3 is protected during querying and analysis.
Flexibility: Snowflake supports a wide range of data formats and can handle semi-structured and unstructured data from S3, making it a versatile choice for data analytics.

Disadvantages of using Snowflake during querying from S3:
Cost: Snowflake can be a costly solution, especially for large-scale data analytics workloads that involve querying data from S3.
Complexity: Snowflake requires expertise in SQL and data warehousing, which can be a barrier for organizations without experienced personnel.
Data Ingestion: Snowflake requires data to be ingested from S3, which can be a time-consuming process, especially for large datasets.

