/*Create Database*/
CREATE TABLE telecom_customer_churn (
  customer_id VARCHAR(20),
  gender VARCHAR(10),
  age INTEGER,
  married VARCHAR(3),
  number_of_dependents INTEGER,
  city VARCHAR(100),
  zip_code VARCHAR(10),
  latitude NUMERIC,
  longitude NUMERIC,
  number_of_referrals INTEGER,
  tenure_in_months INTEGER,
  offer VARCHAR(10),
  phone_service VARCHAR(3),
  avg_monthly_long_distance_charges NUMERIC,
  multiple_lines VARCHAR(3),
  internet_service VARCHAR(10),
  internet_type VARCHAR(20),
  avg_monthly_gb_download INTEGER,
  online_security VARCHAR(3),
  online_backup VARCHAR(3),
  device_protection_plan VARCHAR(3),
  premium_tech_support VARCHAR(3),
  streaming_tv VARCHAR(3),
  streaming_movies VARCHAR(3),
  streaming_music VARCHAR(3),
  unlimited_data VARCHAR(3),
  contract VARCHAR(20),
  paperless_billing VARCHAR(3),
  payment_method VARCHAR(20),
  monthly_charge NUMERIC,
  total_charges NUMERIC,
  total_refunds NUMERIC,
  total_extra_data_charges NUMERIC,
  total_long_distance_charges NUMERIC,
  total_revenue NUMERIC,
  customer_status VARCHAR(10),
  churn_category VARCHAR(20),
  churn_reason TEXT
);
COPY telecom_customer_churn FROM 'D:\college\projects\customer_churn_SQl\The Raw Data\telecom_customer_churn.csv' DELIMITER ',' CSV HEADER;
-- Add a primary key constraint to the customer_id column
ALTER TABLE telecom_customer_churn
ADD PRIMARY KEY (customer_id);

select * from telecom_customer_churn limit 5;

CREATE TABLE zip_code (
zip_code INTEGER,
population INTEGER
);
COPY zip_code FROM 'D:\college\projects\customer_churn_SQl\The Raw Data\telecom_zipcode_population.csv' DELIMITER ',' CSV HEADER;
-- Add a primary key constraint to the customer_id column
ALTER TABLE zip_code
ADD PRIMARY KEY (zip_code);
select * from zip_code limit 5;

CREATE TABLE city_avg_income (
zip_code INTEGER,
state_name VARCHAR(25),
city  VARCHAR(25),
avg_income NUMERIC);
COPY city_avg_income FROM 'D:\college\projects\customer_churn_SQl\The Raw Data\city_income.csv' DELIMITER ',' CSV HEADER;
select * from city_avg_income limit 5 ;

CREATE TABLE dem_data (
  zip_code INTEGER,
  city VARCHAR(25),
  population INTEGER,
  avg_income NUMERIC
);
INSERT INTO dem_data (zip_code, city, population, avg_income)
SELECT z.zip_code, c.city, z.population, c.avg_income
FROM zip_code AS z
INNER JOIN city_avg_income AS c
ON z.zip_code = c.zip_code;

select * from dem_data limit 5;

----------------------------------------------------------------------------------------------------------------------------------------
/* Takes a look on the table's columns*/
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'telecom_customer_churn';
/*Why is the customer churning is a serious problem and a high risk to our Bussines*/
CREATE VIEW churned_Loses AS
  select ROUND(SUM(total_revenue)FILTER(where customer_status='Churned'),-5) AS total_lost,
    CAST(SUM(total_revenue) FILTER(where customer_status='Churned')*1.0 /SUM(total_revenue) *100 AS decimal (10,2)) as percentage_of_total_lost,
    CAST(COUNT(customer_id) FILTER(where customer_status='Churned')*1.0 /COUNT(*) *100 AS decimal (10,2)) tota_customer_charned,
    COUNT(customer_id) FILTER(where customer_status='Churned')/365 AS we_lose_customer_per_day
  from telecom_customer_churn;

/*Data Quality Assesment and Cleaning*/

-- Display the first few rows of the table
SELECT * FROM telecom_customer_churn LIMIT 5;
/*counting missing values*/
SELECT
 customer_id,
  COUNT(*) AS missing_count 
FROM
telecom_cleaned
WHERE
  total_charges is  null 
GROUP BY
customer_id

/*check the customer_id Ensuring it's Quality*/
select count(*)
from telecom_customer_churn
where customer_id NOT SIMILAR TO '[0-9]{4}-[A-Z]{5}'
/*Notice some issue in the gender column*/
UPDATE telecom_customer_churn
SET gender = LOWER(gender);
/*Ensuring the monthly_chrages column's Quality*/
UPDATE telecom_customer_churn
SET monthly_charge=
CASE WHEN monthly_charge <0 THEN 0 
     ELSE monthly_charge END;
/* Changine the None value into more Useful meaning*/
UPDATE telecom_customer_churn
SET offer = REPLACE (offer,'None','Got NO Offer');
					 
/*Calculate the total charges by combining different charge components*/
ALTER TABLE telecom_customer_churn
ADD COLUMN total_combined_charges NUMERIC;
UPDATE telecom_customer_churn
SET total_combined_charges = monthly_charge + total_extra_data_charges + total_long_distance_charges - total_refunds;
-- Encode categorical variables using one-hot encoding
ALTER TABLE telecom_customer_churn
ADD COLUMN internet_service_encoded BOOLEAN;
UPDATE telecom_customer_churn
SET internet_service_encoded = (LOWER(internet_service) = 'fiber optic');
--Explore updated dataset
select * from telecom_customer_churn limit 5;
-- Create a new table to store the cleaned dataset
CREATE TABLE tele_clean AS
SELECT *
FROM telecom_customer_churn;
-- Export the cleaned dataset to a new CSV file
COPY tele_clean TO 'D:\college\projects\customer_churn_SQl\The Raw Data\tele_clean.csv' DELIMITER ',' CSV HEADER;

/*Data Exploration, Begin identefyng churned customer pofile*/
--1)
-- Descriptive statistics for total_combined_charges column with respect to churn_category
CREATE VIEW Descriptive_statistics AS
SELECT churn_category,count(customer_id ) AS NO_Churned_customers,
  AVG(total_combined_charges) AS average_total_combined_charges,
  PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY total_combined_charges) AS median_total_combined_charges,
   MAX(total_combined_charges) AS maximum_monthly_charge,
   SUM(total_revenue) AS total_lost_revwnue
FROM tele_clean
Where 
customer_status = 'Churned'
GROUP BY churn_category
ORDER BY 2 DESC;

/*Churned Customer Characteristic analysis*/
CREATE VIEW Customer_Characteristic AS
SELECT CASE WHEN tenure_in_months < 6  THEN 'Less than 6 months'
            WHEN tenure_in_months <= 12 THEN '1 Year'
            WHEN tenure_in_months <= 24 THEN '2 Years'
            ELSE 'more than 2 years' END AS tenture_category,
        Count(churn_category) FILTER( where customer_status = 'Churned'),
cast(COUNT(customer_id) FILTER(where customer_status = 'Churned') * 1.0 / count(*) *100 as decimal (10,2)) as churnrate_precentage 
FROM tele_clean
GROUP BY tenture_category
ORDER BY 3  desc;
/*slide_2*/
---gender srgmentaion analysis
Select gender, count(*) as Totalcustomer,
COUNT(customer_id) FILTER(where customer_status = 'Churned') AS total_churned,
cast(COUNT(customer_id) FILTER(where customer_status = 'Churned') * 1.0 / count(*) *100 as decimal (10,2)) as churnrate 
from tele_clean 
group by gender
order by churnrate desc;
/*there is a slightley higher churned ratio in Female Customers*/
-----
/*Customer_Churn*/
SELECT offer,
COUNT(customer_id) FILTER(where customer_status = 'Churned') AS total_churned,
cast(COUNT(customer_id) FILTER(where customer_status = 'Churned') * 1.0 / (select count(*) FILTER(where customer_status = 'Churned') from tele_clean) *100 as decimal (10,2)) as churnrate_precentage,
cast(COUNT(customer_id) FILTER(where customer_status = 'Churned') * 1.0 / count(*) *100 as decimal (10,2)) as churnrate 
from tele_clean
group by offer 
order by 3 desc
/* AS it was expected Over 57% of the churned customers had no offer */
------
/*Perform advanced data analysis techniques to identify key drivers of customer churn*/
select PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_combined_charges) AS first_Quartile,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_combined_charges) AS Median,
PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_combined_charges) AS Third_Quartile
from tele_clean
-- Create customer segments based on relevant features (e.g., tenure, total_combined_charges,age_bins)
CREATE TABLE customer_segments AS
SELECT
    customer_id,
    CASE
        WHEN tenure_in_months < 6   THEN 'Less than 6 months'
        WHEN tenure_in_months <= 12 THEN '1 Year'
        WHEN tenure_in_months <= 24 THEN '2 Years'
        ELSE 'More than 2 years'
    END AS tenure_segment,
    CASE
        WHEN total_combined_charges < 136.5  THEN 'Low Value'
        WHEN total_combined_charges >= 136.5 AND total_combined_charges < 476.5 THEN 'Medium Value'
        WHEN total_combined_charges >= 476.5 AND total_combined_charges < 1277 THEN 'High Value'
        ELSE 'Elite Value'
    END AS value_segment,
    CASE
        WHEN age < 18 THEN 'Under 18'
        WHEN age >= 18 AND age < 25 THEN '18-24'
        WHEN age >= 25 AND age < 35 THEN '25-34'
        WHEN age >= 35 AND age < 45 THEN '35-44'
        WHEN age >= 45 AND age < 55 THEN '45-54'
        WHEN age >= 55 AND age < 65 THEN '55-64'
        ELSE '65 and above'
    END AS age_segment
FROM
    tele_clean
WHERE
    customer_status = 'Churned';
---------------
CREATE VIEW customer_segment  AS
  select tenure_segment,value_segment,age_segment,count(*),ROUND(count(customer_id)*1.0/(SELECT COUNT(*) FROM tele_clean)*100.0,2)
  from customer_segments
  group by tenure_segment,value_segment,age_segment
  order by 4 desc;
  
/* Develop personalized retention strategies for each customer segment */
--------
/*The lost revanue regrades to the segmentation */ 
SELECT tenure_segment,
  value_segment,
  age_segment,
  count(*),
  ROUND(count(customer_id)*1.0/(SELECT COUNT(*) FROM tele_clean)*100.0,2),
  SUM(total_revenue) as lost_revenue
from customer_segments
INNER JOIN tele_clean
USING(customer_id)
group by tenure_segment,value_segment,age_segment
order by lost_revenue desc
LIMIT 10;
/*begine to performe a demographic analysis */




----Implement a business intelligence (BI) tool (e.g., Tableau, Power BI) to create interactive dashboards and visualizations for churn metrics.
