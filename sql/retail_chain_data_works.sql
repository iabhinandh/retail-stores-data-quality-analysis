CREATE DATABASE internship;

SELECT *
FROM customer

SELECT *
FROM orders

SELECT *
FROM order_payment

SELECT *
FROM order_rating

SELECT *
FROM product_info

SELECT *
FROM stores

-- For all tables
EXEC sp_help 'customer';
EXEC sp_help 'orders';
EXEC sp_help 'order_payment';
EXEC sp_help 'order_rating';
EXEC sp_help 'product_info';
EXEC sp_help 'stores';

---Renaming spelling mistakes in column names

EXEC sp_rename'product_info.product_name_lenght','product_name_length','COLUMN';

EXEC sp_rename'product_info.product_description_lenght','product_description_length','COLUMN';




SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'customer'

SELECT COLUMN_NAME,DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'orders'

SELECT COLUMN_NAME,DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'order_payment'


SELECT COLUMN_NAME,DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='order_rating'

SELECT COLUMN_NAME,DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='product_info'

SELECT COLUMN_NAME,DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME ='stores'

-- Changing DataTypes in 'orders' Table


SELECT TOP 10 Bill_date_timestamp
FROM orders;

ALTER TABLE orders
ALTER COLUMN Bill_date_timestamp DATETIME2;   --- nvarchar to datetime2

ALTER TABLE orders
ALTER COLUMN MRP DECIMAL(10,2);     --- float to decimal

ALTER TABLE orders
ALTER COLUMN Cost_Per_Unit DECIMAL(10,2);      --- float to decimal


--- Changing DataTypes in 'order_payment' table

ALTER TABLE order_payment
ALTER COLUMN payment_value DECIMAL(10,2);     --- float to decimal (float for money will cause rounding error while calculation)


--- Changing DataTypes in 'product_info' table

ALTER TABLE product_info
ALTER COLUMN product_length_cm DECIMAL(10,2);


ALTER TABLE product_info
ALTER COLUMN product_height_cm DECIMAL(10,2);


ALTER TABLE product_info
ALTER COLUMN product_width_cm DECIMAL(10,2);

---customer table
---finding if there is null records



SELECT 
    SUM(CASE WHEN CustID IS NULL THEN 1 ELSE 0 END) AS null_CustID,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) AS null_customer_city,
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) AS null_customer_state,
    SUM(CASE WHEN Gender IS NULL THEN 1 ELSE 0 END) AS null_Gender
FROM customer;


---finding the number of duplicates found

SELECT *, COUNT(*) as cnt
FROM orders
GROUP BY Customer_id, order_id, product_id, Channel, Delivered_StoreID, Bill_date_timestamp, Quantity, Cost_Per_Unit, MRP, Discount, Total_Amount
HAVING COUNT(*) > 1;


---orders_table


SELECT 
    Customer_id,
    order_id,
    product_id,
    Channel,
    Delivered_StoreID,
    Bill_date_timestamp,
    Quantity,
    Cost_Per_Unit,
    MRP,
    Discount,
    Total_Amount,
    COUNT(*) AS duplicate_count
FROM orders
GROUP BY 
    Customer_id,
    order_id,
    product_id,
    Channel,
    Delivered_StoreID,
    Bill_date_timestamp,
    Quantity,
    Cost_Per_Unit,
    MRP,
    Discount,
    Total_Amount
HAVING COUNT(*) > 1;

---finding the number of duplicate record with columns Customer_id, order_id, product_id,Channel,Delivered_StoreID,Bill_date_timestamp.

SELECT 
    Customer_id,
    order_id,
    product_id,
    Channel,
    Delivered_StoreID,
    Bill_date_timestamp,
    COUNT(*) AS duplicate_count
FROM orders
GROUP BY 
    Customer_id,
    order_id,
    product_id,
    Channel,
    Delivered_StoreID,
    Bill_date_timestamp
    HAVING COUNT(*) > 1;

--- there is dfferent quantities sold with same product id and same order id at same time

 SELECT *
 FROM orders
 WHERE order_id = '64065ff7503353586b3dacf9aec56ae7'




---finding the null values found in orders table


SELECT 
    SUM(CASE WHEN Customer_id IS NULL THEN 1 ELSE 0 END) AS null_Customer_id,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN Channel IS NULL THEN 1 ELSE 0 END) AS null_Channel,
    SUM(CASE WHEN Delivered_StoreID IS NULL THEN 1 ELSE 0 END) AS null_Delivered_StoreID,
    SUM(CASE WHEN Bill_date_timestamp IS NULL THEN 1 ELSE 0 END) AS null_Bill_date_timestamp,
    SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END) AS null_Quantity,
    SUM(CASE WHEN Cost_Per_Unit IS NULL THEN 1 ELSE 0 END) AS null_Cost_Per_Unit,
    SUM(CASE WHEN MRP IS NULL THEN 1 ELSE 0 END) AS null_MRP,
    SUM(CASE WHEN Discount IS NULL THEN 1 ELSE 0 END) AS null_Discount,
    SUM(CASE WHEN Total_Amount IS NULL THEN 1 ELSE 0 END) AS null_Total_Amount
FROM orders;

---finding null values in order_payment table

SELECT 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS null_payment_type,
    SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM order_payment;


--finding duplicate records in order_payment table

SELECT 
    order_id,
    payment_type,
    payment_value,
    COUNT(*) AS duplicate_count
FROM order_payment
GROUP BY 
    order_id,
    payment_type,
    payment_value
HAVING COUNT(*) > 1;


--- Null values present in order_rating table

SELECT 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN Customer_Satisfaction_Score IS NULL THEN 1 ELSE 0 END) AS null_customer_satisfaction_score
FROM order_rating;

---Duplicate records in order_rating table

SELECT 
    order_id,
    Customer_Satisfaction_Score,
    COUNT(*) AS duplicate_count
FROM order_rating
GROUP BY 
    order_id,
    Customer_Satisfaction_Score
HAVING COUNT(*) > 1;

-- null values present in table 'product_info'

SELECT
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN product_name_length IS NULL THEN 1 ELSE 0 END) AS null_product_name_length,
    SUM(CASE WHEN product_description_length IS NULL THEN 1 ELSE 0 END) AS null_product_description_length,
    SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) AS null_product_photos_qty,
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) AS null_product_weight_g,
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) AS null_product_length_cm,
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) AS null_product_height_cm,
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) AS null_product_width_cm
FROM product_info;

---record level duplicates present in 'product info'

SELECT 
    product_id,
    Category,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    COUNT(*) AS duplicate_count
FROM product_info
GROUP BY 
    product_id,
    Category,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
HAVING COUNT(*) > 1;

---Null values in each column of stores table

SELECT
    SUM(CASE WHEN StoreID IS NULL THEN 1 ELSE 0 END) AS null_StoreID,
    SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) AS null_seller_city,
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS null_seller_state,
    SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END) AS null_region
FROM stores;


--- Record level duplicates in stores table

SELECT
    StoreID,
    seller_city,
    seller_state,
    Region,
    COUNT(*) AS duplicate_count
FROM stores
GROUP BY
    StoreID,
    seller_city,
    seller_state,
    Region
HAVING COUNT(*) > 1;



--- FINDING MISSING VALUES WHILE JOINING

-- Orders with missing Customer
SELECT o.*
FROM dbo.Orders o
LEFT JOIN dbo.Customer c ON o.Customer_id = c.CustID
WHERE c.CustID IS NULL;

-- Orders with missing Product
SELECT o.*
FROM dbo.Orders o
LEFT JOIN dbo.product_info p ON o.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Orders with missing Store
SELECT o.*
FROM dbo.Orders o
LEFT JOIN dbo.stores s ON o.Delivered_StoreID = s.StoreID
WHERE s.StoreID IS NULL;

-- Payments with missing Orders
SELECT  p.*
FROM dbo.order_payment p
LEFT JOIN dbo.Orders o ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Ratings with missing Orders
SELECT r.*
FROM dbo.order_rating r
LEFT JOIN dbo.Orders o ON r.order_id = o.order_id
WHERE o.order_id IS NULL;



---CUSTOMER TABLE INSPECTION

---Total records

SELECT COUNT(*) AS total_records
FROM customer;


---Check duplicates in customer id

SELECT Custid, COUNT(*) AS count_occurrences
FROM customer
GROUP BY Custid                                    ---there is no duplicates
HAVING COUNT(*) > 1;    


--number of males and females in the customer table

SELECT gender, COUNT(*) AS count_gender
FROM customer
GROUP BY gender;

--find unique states and how many times each state occurs in the customer table

SELECT customer_state, COUNT(*) AS customer_count
FROM customer
GROUP BY customer_state
ORDER BY customer_count DESC;

--find unique cities and the number of customers in each city 

SELECT customer_city, COUNT(*) AS customer_count
FROM customer
GROUP BY customer_city
ORDER BY customer_count DESC;


--order_payment table 

-- Total records

SELECT COUNT(*) AS total_records
FROM order_payment;


--Duplicate order_id

SELECT order_id, COUNT(*) AS count_occurrences
FROM order_payment
GROUP BY order_id
HAVING COUNT(*) > 1;

--Duplicate combination of order_id and payment_type

SELECT order_id, payment_type, COUNT(*) AS count_occurrences
FROM order_payment
GROUP BY order_id, payment_type
HAVING COUNT(*) > 1;

--Unique orderid

SELECT DISTINCT order_id
FROM order_payment;

--find the number of occurrences of each payment_type in the order_payment table

SELECT payment_type, COUNT(*) AS type_count
FROM order_payment
GROUP BY payment_type
ORDER BY type_count DESC;


-- Find total duplicate records 

SELECT COUNT(*) AS duplicate_count
FROM order_payment
GROUP BY order_id, payment_type, payment_value
HAVING COUNT(*) > 1

--total duplicate count

WITH DuplicateCounts AS (
    -- This is your original query to find duplicate groups
    SELECT COUNT(*) AS duplicate_count
    FROM order_payment
    GROUP BY order_id, payment_type, payment_value
    HAVING COUNT(*) > 1
)
-- This query sums the counts from the result above
SELECT SUM(duplicate_count) AS total_duplicate_rows
FROM DuplicateCounts;



--payment done through debit and credit

SELECT order_id
FROM order_payment
WHERE payment_type IN ('debit_card', 'credit_card')
GROUP BY order_id
HAVING COUNT(DISTINCT payment_type) = 2;

--how payment done

SELECT * 
FROM order_payment
WHERE order_id = 'a079628ac8002126e75f86b0f87332e4'

--payment done through credit and voucher

SELECT order_id
FROM order_payment
WHERE payment_type IN ('voucher', 'credit_card')
GROUP BY order_id
HAVING COUNT(DISTINCT payment_type) = 2;


--payment value column

--Total records

SELECT COUNT(*) AS total_records
FROM order_payment;

--NULL or Missing value

SELECT SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM order_payment;

--Minimum, maximum, average, sum 

SELECT 
    MIN(payment_value) AS min_value,
    MAX(payment_value) AS max_value,
    AVG(payment_value) AS avg_value,
    SUM(payment_value) AS total_value
FROM order_payment;


--Check for negative or zero values


SELECT COUNT(*) AS negative_or_zero_count
FROM order_payment
WHERE payment_value <= 0;

--See how payment amounts vary across payment types

SELECT payment_type, 
       MIN(payment_value) AS min_value,
       MAX(payment_value) AS max_value,
       AVG(payment_value) AS avg_value,
       SUM(payment_value) AS total_value
FROM order_payment
GROUP BY payment_type;


--order review rating table
--Total records


SELECT COUNT(*) AS total_records
FROM order_rating;

--Check for duplicates in order_id

SELECT order_id, COUNT(*) AS count_occurrences
FROM order_rating
GROUP BY order_id
HAVING COUNT(*) > 1;

--NULL values

SELECT 
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN Customer_Satisfaction_Score IS NULL THEN 1 ELSE 0 END) AS null_score
FROM order_rating;

--Distribution of scores 

SELECT Customer_Satisfaction_Score, COUNT(*) AS count_score
FROM order_rating
GROUP BY Customer_Satisfaction_Score
ORDER BY count_score DESC;

---Basic statistics of Customer_Satisfaction_Score

SELECT 
    MIN(Customer_Satisfaction_Score) AS min_score,
    MAX(Customer_Satisfaction_Score) AS max_score,
    AVG(Customer_Satisfaction_Score) AS avg_score
FROM order_rating;


--Product_info table
--total records

SELECT COUNT(*) AS total_records
FROM product_info;

-- Check for duplicates in product_id

SELECT product_id, COUNT(*) AS count_occurrences
FROM product_info
GROUP BY product_id
HAVING COUNT(*) > 1;

-- NULL count each column
SELECT 
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN Category IS NULL THEN 1 ELSE 0 END) AS null_category,
    SUM(CASE WHEN product_name_length IS NULL THEN 1 ELSE 0 END) AS null_name_length,
    SUM(CASE WHEN product_description_length IS NULL THEN 1 ELSE 0 END) AS null_description_length,
    SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) AS null_photos_qty,
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) AS null_weight_g,
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) AS null_length_cm,
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) AS null_height_cm,
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) AS null_width_cm
FROM product_info;

-- Distribution of categories

SELECT Category, COUNT(*) AS count_category
FROM product_info
GROUP BY Category
ORDER BY count_category DESC;

--Store table
--Total records

SELECT COUNT(*) AS total_records
FROM stores;

-- Check for duplicates in StoreID

SELECT StoreID, COUNT(*) AS count_occurrences
FROM stores
GROUP BY StoreID
HAVING COUNT(*) > 1;

--NULL values

SELECT 
    SUM(CASE WHEN StoreID IS NULL THEN 1 ELSE 0 END) AS null_storeid,
    SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) AS null_seller_city,
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS null_seller_state,
    SUM(CASE WHEN Region IS NULL THEN 1 ELSE 0 END) AS null_region
FROM stores;

-- Count of stores by city

SELECT seller_city, COUNT(*) AS store_count
FROM stores
GROUP BY seller_city
ORDER BY store_count DESC;

-- Count of stores by state

SELECT seller_state, COUNT(*) AS store_count
FROM stores
GROUP BY seller_state
ORDER BY store_count DESC;

--Count of stores by region 

SELECT Region, COUNT(*) AS store_count
FROM stores
GROUP BY Region
ORDER BY store_count DESC;


--orders table (fact table)
--total records

SELECT COUNT(*) AS total_records
FROM Orders;

--Check if (order_id, product_id) combination is unique

SELECT order_id, product_id, COUNT(*) AS count_occurrences
FROM Orders
GROUP BY order_id, product_id
HAVING COUNT(*) > 1;

-- Example of cumulative repeated entry

SELECT *
FROM orders
WHERE product_id = '002af88741ba70c7b5cf4e4a0ad7ef85'

--Check for negative or zero Quantity / Amount

SELECT *
FROM Orders
WHERE Quantity <= 0 OR Total_Amount <= 0;

--Verify price consistency

SELECT 
    order_id, 
    Quantity, 
    MRP, 
    Discount, 
    Total_Amount,
    (Quantity * (MRP - Discount)) AS expected_total
FROM Orders
WHERE ABS(Total_Amount - (Quantity * (MRP - Discount))) > 0.01;

-- Channel-wise order count

SELECT Channel, COUNT(*) AS order_count
FROM Orders
GROUP BY Channel
ORDER BY order_count DESC;

--orders by store

SELECT Delivered_StoreID, COUNT(*) AS order_count
FROM Orders
GROUP BY Delivered_StoreID
ORDER BY order_count DESC;

--Order trend over time

SELECT 
    FORMAT(Bill_date_timestamp, 'yyyy-MM') AS Month_Year,
    COUNT(*) AS order_count
FROM Orders
GROUP BY FORMAT(Bill_date_timestamp, 'yyyy-MM')
ORDER BY Month_Year  DESC






--Find orders with missing dimension rows:

SELECT COUNT(*) as missing
FROM Orders_Cleaned o
LEFT JOIN customer c ON o.Customer_id = c.Custid
WHERE c.Custid IS NULL;

--Check whether every store that appears in the Orders_Cleaned table exists in the Stores dimension table.

SELECT COUNT(*) AS Missing_Stores
FROM Orders_Cleaned o
LEFT JOIN Stores s
    ON o.Delivered_StoreID = s.StoreID
WHERE s.StoreID IS NULL;

-- find every product_id in your Orders_Cleaned table exists in the Product_Info dimension table.

SELECT COUNT(*) AS Missing_Products
FROM Orders_Cleaned o
LEFT JOIN Product_Info p
    ON o.Product_ID = p.Product_ID
WHERE p.Product_ID IS NULL;

--Check for ratings without matching orders


SELECT COUNT(*) AS Missing_Reviews
FROM Order_rating r
LEFT JOIN Orders o
    ON r.Order_ID = o.Order_ID
WHERE o.Order_ID IS NULL;


--Check for payments without matching orders


SELECT COUNT(*) AS Missing_Payments
FROM Order_Payment p
LEFT JOIN Orders o
    ON p.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Compare orphan OrderIDs from Order_Payment and Order_Rating(common_missing) 

WITH Missing_Payments AS (
    SELECT p.order_id
    FROM Order_Payment p
    LEFT JOIN Orders o
        ON p.order_id = o.order_id
    WHERE o.order_id IS NULL
),
Missing_Ratings AS (
    SELECT r.order_id
    FROM Order_Rating r
    LEFT JOIN Orders o
        ON r.order_id = o.order_id
    WHERE o.order_id IS NULL
)
SELECT 
    COUNT(DISTINCT p.order_id) AS Payment_Only,
    COUNT(DISTINCT r.order_id) AS Rating_Only,
    COUNT(DISTINCT CASE WHEN p.order_id = r.order_id THEN p.order_id END) AS Common_Orphans
FROM Missing_Payments p
FULL JOIN Missing_Ratings r
    ON p.order_id = r.order_id;


---Find if any customers have the same ORDER_ID



SELECT order_id,COUNT(DISTINCT Customer_id) AS Unique_customers
FROM orders
GROUP BY order_id
HAVING COUNT (DISTINCT Customer_id) > 1;


--find product cost per unit is equal to MRP.

SELECT *
FROM orders
WHERE MRP <= Cost_Per_Unit

--which are the products

SELECT DISTINCT product_id
FROM orders
WHERE MRP <= Cost_Per_Unit

--distinct order_id

SELECT DISTINCT order_id
FROM orders
WHERE MRP <= Cost_Per_Unit


-- mismatched total_amount and Payment_value

SELECT 
    DISTINCT (o.ORDER_ID),
    o.total_amount,
    p.PAYMENT_VALUE
FROM Orders o
JOIN Order_Payment p 
    ON o.ORDER_ID = p.ORDER_ID
WHERE o.Total_Amount <> p.PAYMENT_VALUE;

--matched total_amount and payment_value

SELECT 
    DISTINCT (o.ORDER_ID),
    o.Total_Amount,
    p.PAYMENT_VALUE
FROM Orders o
JOIN Order_Payment p 
    ON o.ORDER_ID = p.ORDER_ID
WHERE o.Total_Amount = p.payment_value;




----------------- DATA CLEANING ----------------------

--- STORES TABLE --

-- Drop 1 record having duplicate with storeid ST410.

SELECT * 
FROM stores
WHERE StoreID = 'ST410'

-- Deleting one duplicate 

DELETE TOP (1)
FROM stores
WHERE StoreID = 'ST410';

SELECT *
FROM stores


---- CLEANING PRODUCT_INFO TABLE --------

-- Distribution of categories

SELECT Category, COUNT(*) AS count_category
FROM product_info
GROUP BY Category
ORDER BY count_category DESC;

--- Changing the catogory name #N/A to Others.

UPDATE product_info
SET Category = 'Others'
WHERE Category = '#N/A'

SELECT *
FROM product_info


--- CLEANING ORDER REVIEW TABLE -----

--Check for duplicates in order_id

SELECT order_id, COUNT(*) AS count_occurrences
FROM order_rating
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT *
FROM order_rating
WHERE order_id = 'ca263afd88a8a1200605adbd4b63cd7d'

--- Creating new table by aggregating avg of customer satisfaction score where there is duplicate.

SELECT 
    order_id,
    AVG(Customer_Satisfaction_Score) AS Customer_Satisfaction_Score
INTO order_rating_clean
FROM order_rating
GROUP BY order_id;

-- total rows after cleaning 

SELECT COUNT(*) AS total_rows
FROM order_rating_clean;

SELECT *
FROM order_rating_clean
WHERE order_id = 'ca263afd88a8a1200605adbd4b63cd7d'

--- CLEANING ORDER PAYMENT TABLE ----

SELECT *
FROM order_payment

--- pivoting the order_payment table — converting multiple rows per order_id (different payment types)
-- into one row per order_id, with separate columns for each payment type and a total.

SELECT
    order_id,
    ISNULL([credit_card], 0) AS Credit_Card,
    ISNULL([debit_card], 0) AS Debit_Card,
    ISNULL([Voucher], 0) AS Voucher,
    ISNULL([UPI/Cash], 0) AS UPI_Cash,
    ISNULL([credit_card], 0) + ISNULL([debit_card], 0) + ISNULL([Voucher], 0) + ISNULL([UPI/Cash], 0) AS total_amount
    INTO order_payment_pivot
FROM
(
    SELECT order_id, payment_type, payment_value
    FROM order_payment
) AS src
PIVOT
(
    SUM(payment_value)
    FOR payment_type IN ([credit_card], [debit_card], [Voucher], [UPI/Cash])
) AS pvt;


-- CREATED A NEW TABLE NAME order_payment_pivot 

SELECT * 
FROM order_payment_pivot

------- CLEAN ORDERS TABLE --------

--- Cleaning the cummulative entries in orders table and making a cleaned copy

SELECT *
INTO Orders_Cleaned
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Order_ID, Product_ID ORDER BY Quantity DESC) AS rn
    FROM Orders
) t
WHERE rn = 1;




SELECT *
FROM Orders_Cleaned


--- finding dduplicates with same bill_date time

SELECT 
    order_id,
    Bill_date_timestamp,
    COUNT(*) AS duplicate_count
FROM Orders_Cleaned
GROUP BY order_id, Bill_date_timestamp
HAVING COUNT(*) > 5;

SELECT *
FROM Orders_Cleaned
WHERE order_id = '7d8f5bfd5aff648220374a2df62e84d5'

SELECT *
FROM product_info
WHERE product_id= '678c229b41c0e497d35a25a8be1cc631'

--- DELETING RECORDS BEFORE 2021-09-01 AND AFTER 2023-10-31.

SELECT *
FROM Orders_Cleaned
WHERE Bill_date_timestamp NOT  BETWEEN '2021-09-01' AND '2023-10-31';   --- THERE ARE 3 ROWS TO BE DELETED ---

DELETE FROM Orders_Cleaned
WHERE Bill_date_timestamp < '2021-09-01';  -- DELETED 3 ROWS


DELETE FROM Orders_Cleaned
WHERE Bill_date_timestamp > '2023-10-31';  -- 0 ROWS AFTER 2023 - 10 - 31

-- ORDER_ID WITH MULTIPLE STORE_ID

SELECT 
    order_id,
    COUNT (DISTINCT Delivered_StoreID) AS store_count
    FROM Orders_Cleaned
    GROUP BY order_id                                    ---- 1007 rows 
    HAVING COUNT (DISTINCT Delivered_StoreID) > 1      

--- ORDER_ID WITH MULTIPLE STORE_ID WHERE CHANNEL = INSTORE


SELECT 
    order_id,Channel,
    COUNT (DISTINCT Delivered_StoreID) AS store_count
    FROM Orders_Cleaned
    WHERE Channel = 'instore'                             ----- 911 rows
    GROUP BY order_id,Channel
    HAVING COUNT (DISTINCT Delivered_StoreID) > 1


--- ORDER_ID  WITH MULTIPLE BILLDATE

SELECT 
    order_id,
    COUNT (DISTINCT Bill_date_timestamp) AS store_count
    FROM Orders_Cleaned
    GROUP BY order_id                                       ---- 334rows
    HAVING COUNT (DISTINCT Bill_date_timestamp) > 1

--- CLEANING  ORDERID WITH MULTIPLE STOREID WHERE CHANNEL = INSTORE

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY Total_Amount DESC
           ) AS row_num
    FROM Orders_Cleaned
    WHERE Channel = 'instore'
      AND order_id IN (
          SELECT order_id
          FROM Orders_Cleaned
          WHERE Channel = 'instore'
          GROUP BY order_id
          HAVING COUNT(DISTINCT Delivered_StoreID) > 1
      )
)
SELECT *
FROM ranked
WHERE row_num > 1;

--DELETING THOSE RECORDS

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY Total_Amount DESC
           ) AS row_num
    FROM Orders_Cleaned
    WHERE Channel = 'instore'
      AND order_id IN (
          SELECT order_id
          FROM Orders_Cleaned
          WHERE Channel = 'instore'
          GROUP BY order_id
          HAVING COUNT(DISTINCT Delivered_StoreID) > 1
      )
)
DELETE FROM ranked
WHERE row_num > 1;


SELECT *
FROM Orders_Cleaned



 --- ORDER_ID  WITH MULTIPLE BILLDATE

SELECT 
    order_id,
    COUNT (DISTINCT Bill_date_timestamp) AS store_count
    FROM Orders_Cleaned
    GROUP BY order_id                                       ---- 93 rows (before it was 334, after i cleaned multiple storeid it changed)
    HAVING COUNT (DISTINCT Bill_date_timestamp) > 1

--- CLEANING ORDER_ID  WITH MULTIPLE BILLDATE

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY Bill_date_timestamp ASC
           ) AS row_number
    FROM Orders_Cleaned
    WHERE order_id IN (
        SELECT order_id
        FROM Orders_Cleaned
        GROUP BY order_id
        HAVING COUNT(DISTINCT Bill_date_timestamp) > 1
    )
)
SELECT * FROM ranked
WHERE row_number > 1;

-- DELETING THOSE RECORDS

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY Bill_date_timestamp ASC
           ) AS row_number
    FROM Orders_Cleaned
    WHERE order_id IN (
        SELECT order_id
        FROM Orders_Cleaned
        GROUP BY order_id
        HAVING COUNT(DISTINCT Bill_date_timestamp) > 1
    )
)
DELETE FROM ranked
WHERE row_number > 1;

SELECT *
FROM Order_payment_pivot

--Shows if any orders don’t have corresponding payments.

SELECT COUNT(DISTINCT o.order_id) AS Orders,
       COUNT(DISTINCT p.order_id) AS Payments,
       COUNT(DISTINCT o.order_id) - COUNT(DISTINCT p.order_id) AS Difference
FROM Orders_Cleaned o
LEFT JOIN order_payment_pivot p ON o.order_id = p.order_id
WHERE p.order_id IS NULL;

---check which specific order it is 

SELECT o.order_id
FROM Orders_Cleaned o
LEFT JOIN order_payment_pivot p ON o.order_id = p.order_id  ----- bfbd0f9bdef84302105ad712db648a6c -- only this orderid
WHERE p.order_id IS NULL;

DELETE FROM orders_cleaned
WHERE order_id = 'bfbd0f9bdef84302105ad712db648a6c';

----Identifies orders with missing reviews.

SELECT COUNT(DISTINCT o.order_id)
FROM Orders_Cleaned o
LEFT JOIN order_rating r ON o.order_id = r.order_id        ---- no orders with missing review
WHERE r.order_id IS NULL;

--- Finds orders not linked to a valid customer

SELECT COUNT(DISTINCT o.order_id)
FROM Orders_Cleaned o
LEFT JOIN customer c ON o.customer_id = c.custid          ---- no orders not linked to a valid customer
WHERE c.custid IS NULL;

SELECT COUNT(DISTINCT order_id) AS Unique_Orders, COUNT(*) AS Total_Rows
FROM orders_cleaned;

--- validating total amount and payment value

SELECT
    SUM(o.total_amount) AS Total_Order_Amount,
    SUM(p.total_amount) AS Total_Payment_Amount,
    SUM(o.total_amount - p.total_amount) AS Difference     -- Ignore ₹1 or smaller differences
FROM orders_cleaned o
LEFT JOIN order_payment_pivot p
    ON o.order_id = p.order_id
WHERE ABS(o.total_amount - p.total_amount) > 1; 

---- identify Record-Wise Mismatch Between Orders and Payments

-- Step 1: Aggregate total order amount at order level
WITH OrderTotals AS (
    SELECT 
        order_id,
        SUM(Total_Amount) AS Order_Total
    FROM orders_cleaned
    GROUP BY order_id
),
PaymentTotals AS (
    SELECT 
        order_id,
        SUM(total_amount) AS Payment_Total
    FROM order_payment_pivot
    GROUP BY order_id
)
-- Step 2: Compare aggregated totals                             -----3457 RECORDS
SELECT 
    o.order_id,
    o.Order_Total,
    ISNULL(p.Payment_Total, 0) AS Payment_Total,
    ROUND(o.Order_Total - ISNULL(p.Payment_Total, 0), 2) AS Difference
FROM OrderTotals o
LEFT JOIN PaymentTotals p 
    ON o.order_id = p.order_id
WHERE ABS(o.Order_Total - ISNULL(p.Payment_Total, 0)) > 1;


---Find Matched Records Between Orders and Payments

-- Step 1: Aggregate totals at order level
WITH OrderTotals AS (
    SELECT 
        order_id,
        SUM(Total_Amount) AS Order_Total
    FROM orders_cleaned
    GROUP BY order_id
),
PaymentTotals AS (
    SELECT 
        order_id,
        SUM(total_amount) AS Payment_Total
    FROM order_payment_pivot
    GROUP BY order_id
)
-- Step 2: Compare and keep only matched records                 -------95,206 ROWS
SELECT 
    o.order_id,
    o.Order_Total,
    ISNULL(p.Payment_Total, 0) AS Payment_Total,
    ROUND(o.Order_Total - ISNULL(p.Payment_Total, 0), 2) AS Difference
FROM OrderTotals o
LEFT JOIN PaymentTotals p 
    ON o.order_id = p.order_id
WHERE ABS(o.Order_Total - ISNULL(p.Payment_Total, 0)) <= 1;

----CREATING A NEW TABLE NAMED ORDER_PAYMENT_MISMATCH_AUDIT TO STORE MISMATCH VALUES FOR FURTHER CLARIFICATION

SELECT 
    c.*
INTO order_mismatch
FROM orders_cleaned AS c
LEFT JOIN (
    SELECT 
        o.order_id,
        SUM(ISNULL(o.Total_Amount, 0)) AS Order_Total,
        ISNULL(SUM(p.total_amount), 0) AS Payment_Total
    FROM orders_cleaned o
    LEFT JOIN order_payment_pivot p 
        ON o.order_id = p.order_id
    GROUP BY o.order_id
    HAVING ABS(SUM(ISNULL(o.Total_Amount, 0)) - ISNULL(SUM(p.total_amount), 0)) > 1
) mismatched
    ON c.order_id = mismatched.order_id
WHERE mismatched.order_id IS NOT NULL;

SELECT * FROM order_mismatch            -------- 6033 rows ------

---DELETING THIS MISMATCHED RECORDS FROM ORDER_CLEANED FOR FURTHER CALCULATION CLARITY

DELETE c
FROM orders_cleaned AS c
LEFT JOIN (
    SELECT 
        o.order_id,
        SUM(ISNULL(o.Total_Amount, 0)) AS Order_Total,
        ISNULL(SUM(p.total_amount), 0) AS Payment_Total
    FROM orders_cleaned o
    LEFT JOIN order_payment_pivot p 
        ON o.order_id = p.order_id
    GROUP BY o.order_id
    HAVING ABS(SUM(ISNULL(o.Total_Amount, 0)) - ISNULL(SUM(p.total_amount), 0)) > 1
) mismatched
    ON c.order_id = mismatched.order_id
WHERE mismatched.order_id IS NOT NULL;
--------------------------------------------------------------------------------------

--===================================================================================-
----------------- CUSTOMER 360 -------------------


-- Step 1: Combine order data with related info
WITH order_enriched AS (
    SELECT
        o.order_id,
        o.Customer_id,
        o.Bill_date_timestamp,
        o.channel,
        o.Quantity,
        o.Total_Amount,
        o.Discount,
        (o.Quantity * (o.MRP - o.[Cost_Per_Unit])) AS Profit,
        p.Credit_Card,
        p.Debit_Card,
        p.Voucher,
        p.UPI_Cash,
        p.total_amount AS Payment_Value,
        s.StoreID,
        s.seller_city,
        s.seller_state,
        pr.product_id,
        pr.Category,
        r.Customer_Satisfaction_Score
    FROM dbo.Orders_Cleaned o
    LEFT JOIN dbo.order_payment_pivot p ON o.order_id = p.order_id
    LEFT JOIN dbo.stores s ON o.Delivered_StoreID = s.StoreID
    LEFT JOIN dbo.product_info pr ON o.product_id = pr.product_id
    LEFT JOIN dbo.order_rating_clean r ON o.order_id = r.order_id
)

-- Step 2: Aggregate all details to Customer level
SELECT 
    c.Custid AS Customer_ID,
    c.customer_city,
    c.customer_state,
    c.Gender,

    MIN(o.Bill_date_timestamp) AS First_Transaction_Date,
    MAX(o.Bill_date_timestamp) AS Last_Transaction_Date,
    DATEDIFF(DAY, MIN(o.Bill_date_timestamp), MAX(o.Bill_date_timestamp)) AS Tenure_Days,
    DATEDIFF(DAY, MAX(o.Bill_date_timestamp), '2023-10-31') AS Inactive_Days,

    COUNT(DISTINCT o.order_id) AS Frequency,                     -- Number of orders
    SUM(o.Total_Amount) AS Monetary,                             -- Total revenue
    SUM(o.Profit) AS Total_Profit,
    SUM(o.Discount) AS Total_Discount,
    SUM(o.Quantity) AS Total_Quantity,
    COUNT(DISTINCT o.product_id) AS Distinct_Products,
    COUNT(DISTINCT o.Category) AS Distinct_Categories,
    COUNT(DISTINCT o.channel) AS Channels_Used,
    COUNT(DISTINCT o.StoreID) AS Stores_Visited,
    COUNT(DISTINCT o.seller_city) AS Cities_Purchased,

    SUM(CASE WHEN o.Discount > 0 THEN 1 ELSE 0 END) AS Transactions_With_Discount,
    SUM(CASE WHEN o.Profit < 0 THEN 1 ELSE 0 END) AS Transactions_With_Loss,

    SUM(CASE WHEN o.Voucher > 0 THEN 1 ELSE 0 END) AS Txn_Voucher,
    SUM(CASE WHEN o.Credit_Card > 0 THEN 1 ELSE 0 END) AS Txn_CreditCard,
    SUM(CASE WHEN o.Debit_Card > 0 THEN 1 ELSE 0 END) AS Txn_DebitCard,
    SUM(CASE WHEN o.UPI_Cash > 0 THEN 1 ELSE 0 END) AS Txn_UPI,
    COUNT(DISTINCT 
          CASE 
              WHEN o.Voucher > 0 THEN 'Voucher'
              WHEN o.Credit_Card > 0 THEN 'CreditCard'
              WHEN o.Debit_Card > 0 THEN 'DebitCard'
              WHEN o.UPI_Cash > 0 THEN 'UPI'
          END) AS Distinct_Payment_Types,

    CASE 
        WHEN SUM(o.Credit_Card) >= GREATEST(SUM(o.Debit_Card), SUM(o.Voucher), SUM(o.UPI_Cash)) THEN 'Credit Card'
        WHEN SUM(o.Debit_Card) >= GREATEST(SUM(o.Credit_Card), SUM(o.Voucher), SUM(o.UPI_Cash)) THEN 'Debit Card'
        WHEN SUM(o.Voucher) >= GREATEST(SUM(o.Credit_Card), SUM(o.Debit_Card), SUM(o.UPI_Cash)) THEN 'Voucher'
        ELSE 'UPI'
    END AS Preferred_Payment_Method,

    AVG(o.Customer_Satisfaction_Score) AS Avg_Customer_Rating
INTO Customer_360
FROM order_enriched o
JOIN dbo.customer c ON o.Customer_id = c.Custid
GROUP BY 
    c.Custid, c.customer_city, c.customer_state, c.Gender;


SELECT * FROM Customer_360

SELECT COUNT(*) AS Column_Count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Customer_360';


---------- ORDER 360 TABLE -----------------

-- Step 1: Create enriched order-level data
WITH order_enriched AS (
    SELECT
        o.order_id,
        o.Customer_id,
        o.Bill_date_timestamp,
        o.channel,
        o.Quantity,
        o.Total_Amount,
        o.Discount,
        o.MRP,
        o.Cost_Per_Unit,
        o.Delivered_StoreID,
        pr.product_id,
        pr.Category,
        s.seller_city,
        s.seller_state,
        r.Customer_Satisfaction_Score
    FROM dbo.Orders_Cleaned o
    LEFT JOIN dbo.product_info pr ON o.product_id = pr.product_id
    LEFT JOIN dbo.stores s ON o.Delivered_StoreID = s.StoreID
    LEFT JOIN dbo.order_rating_clean r ON o.order_id = r.order_id
)

-- Step 2: Aggregate at order level
SELECT
    o.order_id,
    o.Customer_id,
    COUNT(DISTINCT o.product_id) AS No_of_Items,
    SUM(o.Quantity) AS Total_Quantity,
    SUM(o.Total_Amount) AS Total_Amount,
    SUM(o.Discount) AS Total_Discount,
    SUM(CASE WHEN o.Discount > 0 THEN 1 ELSE 0 END) AS Items_With_Discount,
    SUM(o.Quantity * o.Cost_Per_Unit) AS Total_Cost,
    SUM(o.Quantity * (o.MRP - o.Cost_Per_Unit)) AS Total_Profit,
    CASE WHEN SUM(o.Quantity * (o.MRP - o.Cost_Per_Unit)) < 0 THEN 1 ELSE 0 END AS Flag_Loss_Making,
    CASE WHEN SUM(o.Quantity * (o.MRP - o.Cost_Per_Unit)) >= 1000 THEN 1 ELSE 0 END AS Flag_High_Profit,
    COUNT(DISTINCT o.Category) AS Distinct_Categories,
    CASE WHEN DATEPART(WEEKDAY, o.Bill_date_timestamp) IN (1,7) THEN 1 ELSE 0 END AS Weekend_Transaction_Flag,
    CASE 
        WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 6 AND 11 THEN 'Morning'
        WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS Hour_Flag,
    o.channel,
    o.Delivered_StoreID AS StoreID,
    MAX(o.seller_city) AS Seller_City,
    MAX(o.seller_state) AS Seller_State,
    AVG(o.Customer_Satisfaction_Score) AS Avg_Customer_Rating
INTO Order_360
FROM order_enriched o
GROUP BY 
    o.order_id,
    o.Customer_id,
    o.channel,
    o.Delivered_StoreID,
    o.Bill_date_timestamp;


SELECT *
FROM Order_360

SELECT *
FROM Order_360
WHERE Flag_Loss_Making <> 0



----------------- STORE 360 ------------------------

-- Step 1: Create enriched order data (store-level)
WITH order_enriched AS (
    SELECT
        o.order_id,
        o.Customer_id,
        o.Delivered_StoreID AS StoreID,
        o.Bill_date_timestamp,
        o.Quantity,
        o.Total_Amount,
        o.Discount,
        (o.Quantity * o.MRP) AS Total_MRP,
        (o.Quantity * o.Cost_Per_Unit) AS Total_Cost,
        (o.Quantity * (o.MRP - o.Cost_Per_Unit)) AS Profit,
        pr.product_id,
        pr.Category,
        s.seller_city,
        s.seller_state,
        p.total_amount AS Payment_Value,
        r.Customer_Satisfaction_Score
    FROM dbo.Orders_Cleaned o
    LEFT JOIN dbo.order_payment_pivot p ON o.order_id = p.order_id
    LEFT JOIN dbo.product_info pr ON o.product_id = pr.product_id
    LEFT JOIN dbo.stores s ON o.Delivered_StoreID = s.StoreID
    LEFT JOIN dbo.order_rating_clean r ON o.order_id = r.order_id
)

-- Step 2: Aggregate to Store level
SELECT
    o.StoreID,
    MAX(o.seller_city) AS Store_City,
    MAX(o.seller_state) AS Store_State,

    COUNT(DISTINCT o.product_id) AS No_of_Items,
    SUM(o.Quantity) AS Total_Quantity,
    SUM(o.Total_Amount) AS Total_Sales,
    SUM(o.Discount) AS Total_Discount,
    SUM(CASE WHEN o.Discount > 0 THEN 1 ELSE 0 END) AS Items_With_Discount,
    SUM(o.Total_Cost) AS Total_Cost,
    SUM(o.Profit) AS Total_Profit,

    -- Flags
    CASE WHEN SUM(o.Profit) < 0 THEN 1 ELSE 0 END AS Flag_Loss_Making,
    SUM(CASE WHEN o.Profit >= 1000 THEN 1 ELSE 0 END) AS Orders_With_High_Profit,

    COUNT(DISTINCT o.Category) AS Distinct_Categories,

    -- ✅ Aggregate weekend/weekday counts
    SUM(CASE WHEN DATENAME(WEEKDAY, o.Bill_date_timestamp) IN ('Saturday','Sunday') THEN 1 ELSE 0 END) AS Weekend_Transactions,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.Bill_date_timestamp) NOT IN ('Saturday','Sunday') THEN 1 ELSE 0 END) AS Weekday_Transactions,

    -- ✅ Aggregate time-of-day slots
    SUM(CASE WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 6 AND 11 THEN 1 ELSE 0 END) AS Morning_Transactions,
    SUM(CASE WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 12 AND 17 THEN 1 ELSE 0 END) AS Afternoon_Transactions,
    SUM(CASE WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 18 AND 23 THEN 1 ELSE 0 END) AS Evening_Transactions,
    SUM(CASE WHEN DATEPART(HOUR, o.Bill_date_timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END) AS Night_Transactions,

    -- Performance metrics
    ROUND(SUM(o.Total_Amount) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS Avg_Order_Value,
    ROUND(SUM(o.Profit) * 1.0 / COUNT(DISTINCT o.order_id), 2) AS Avg_Profit_Per_Transaction,
    ROUND(SUM(o.Profit) * 1.0 / COUNT(DISTINCT o.Customer_id), 2) AS Avg_Profit_Per_Customer,
    ROUND(COUNT(DISTINCT o.order_id) * 1.0 / COUNT(DISTINCT o.Customer_id), 2) AS Avg_Customer_Visits,
    ROUND(AVG(o.Customer_Satisfaction_Score), 2) AS Avg_Customer_Rating
INTO Store_360
FROM order_enriched o
GROUP BY o.StoreID;

SELECT *
FROM Store_360



--==============================================================--
        --   EXPLORATORY DATA ANALYSIS ---
--==============================================================--

--  Customer360 columns
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Customer_360'
ORDER BY ORDINAL_POSITION;

--  Order360 columns
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Order_360'
ORDER BY ORDINAL_POSITION;

--  Store360 columns
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'Store_360'
ORDER BY ORDINAL_POSITION;

--============= Summary combining Customer, Order, and Store metrics ====================


WITH
-- Customer-level metrics
Customer_Summary AS (
    SELECT
        COUNT(DISTINCT Customer_ID) AS Total_Customers,
        ROUND(AVG(Monetary), 2) AS Avg_Customer_Spending,
        ROUND(SUM(Monetary), 2) AS Total_Revenue_From_Customers,
        ROUND(SUM(Total_Profit), 2) AS Total_Profit_From_Customers,
        ROUND(SUM(CASE WHEN Transactions_With_Discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(Customer_ID), 2) AS Customers_Using_Discount_Percentage
    FROM customer_360
),
-- Order-level metrics
Order_Summary AS (
    SELECT
        COUNT(Order_ID) AS Total_Orders,
        ROUND(AVG(Total_Amount), 2) AS Avg_Order_Value,
        ROUND(SUM(Total_Amount), 2) AS Total_Order_Revenue,
        ROUND(SUM(Total_Profit), 2) AS Total_Order_Profit,
        ROUND(SUM(CASE WHEN Total_Discount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(Order_ID), 2) AS Orders_With_Discount_Percentage
    FROM order_360
),
-- Store-level metrics
Store_Summary AS (
    SELECT
        COUNT(StoreID) AS Total_Stores,
        ROUND(SUM(Total_Sales), 2) AS Total_Store_Revenue,
        ROUND(SUM(Total_Profit), 2) AS Total_Store_Profit,
        ROUND(AVG(Avg_Order_Value), 2) AS Avg_Store_Order_Value,
        ROUND(AVG(Avg_Profit_Per_Transaction), 2) AS Avg_Store_Profit_Per_Transaction,
        ROUND(AVG(Avg_Customer_Rating), 2) AS Avg_Store_Customer_Rating
    FROM store_360
)
-- Combine everything into one row
SELECT
    c.Total_Customers,
    c.Avg_Customer_Spending,
    c.Total_Revenue_From_Customers,
    c.Total_Profit_From_Customers,
    c.Customers_Using_Discount_Percentage,
    
    o.Total_Orders,
    o.Avg_Order_Value,
    o.Total_Order_Revenue,
    o.Total_Order_Profit,
    o.Orders_With_Discount_Percentage,
    
    s.Total_Stores,
    s.Total_Store_Revenue,
    s.Total_Store_Profit,
    s.Avg_Store_Order_Value,
    s.Avg_Store_Profit_Per_Transaction,
    s.Avg_Store_Customer_Rating
FROM Customer_Summary c
CROSS JOIN Order_Summary o
CROSS JOIN Store_Summary s;

-----=================== Customer_360 ====================-------

-- Total customers and basic stats

SELECT 
    COUNT(DISTINCT Customer_ID) AS Total_Customers,
    ROUND(AVG(Frequency), 2) AS Avg_Transactions_Per_Customer,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue_Per_Customer,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Discount), 2) AS Total_Discount
FROM customer_360;

-- Active vs Inactive Customers


SELECT 
    CASE 
        WHEN Inactive_Days <= 90 THEN 'Active (<90 days)'
        ELSE 'Inactive (>90 days)'
    END AS Activity_Status,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(AVG(Monetary), 2) AS Avg_Spending
FROM customer_360
GROUP BY 
    CASE 
        WHEN Inactive_Days <= 90 THEN 'Active (<90 days)'
        ELSE 'Inactive (>90 days)'
    END;


-- How long customers have been with us


SELECT 
    CASE 
        WHEN Tenure_Days <= 180 THEN 'New (≤6 months)'
        WHEN Tenure_Days BETWEEN 181 AND 365 THEN 'Established (6–12 months)'
        WHEN Tenure_Days BETWEEN 366 AND 730 THEN 'Loyal (1–2 years)'
        ELSE 'Very Loyal (>2 years)'
    END AS Tenure_Group,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(AVG(Monetary), 2) AS Avg_Spending
FROM customer_360
GROUP BY 
    CASE 
        WHEN Tenure_Days <= 180 THEN 'New (≤6 months)'
        WHEN Tenure_Days BETWEEN 181 AND 365 THEN 'Established (6–12 months)'
        WHEN Tenure_Days BETWEEN 366 AND 730 THEN 'Loyal (1–2 years)'
        ELSE 'Very Loyal (>2 years)'
    END
ORDER BY Num_Customers DESC;

---with and without discount

SELECT
    CASE 
        WHEN Total_Discount > 0 THEN 'With Discount'
        ELSE 'Without Discount'
    END AS Customer_Type,
    
    COUNT(DISTINCT Customer_ID) AS Total_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    
    ROUND(AVG(Monetary), 2) AS Avg_Revenue_Per_Customer,
    ROUND(AVG(Total_Profit), 2) AS Avg_Profit_Per_Customer,
    
    ROUND(
        100.0 * SUM(Total_Profit) / (SELECT SUM(Total_Profit) FROM Customer_360),
    2) AS Profit_Share_Percent

FROM Customer_360
GROUP BY 
    CASE 
        WHEN Total_Discount > 0 THEN 'With Discount'
        ELSE 'Without Discount'
    END
ORDER BY Total_Profit DESC;

--Revenue by Gender

SELECT 
    Gender,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue_Per_Customer,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Percentage
FROM customer_360
GROUP BY Gender
ORDER BY Total_Revenue DESC;

--- Top Performing Cities

SELECT 
    customer_city,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Percentage
FROM customer_360
GROUP BY customer_city
ORDER BY Total_Revenue DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;


--- Payment Behaviour

SELECT 
    Preferred_Payment_Method,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue_Per_Customer,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Percentage
FROM customer_360
GROUP BY Preferred_Payment_Method
ORDER BY Total_Revenue DESC;



---Discount & Loss Behavior

SELECT
    CASE 
        WHEN Transactions_With_Discount > 0 THEN 'Used Discounts'
        ELSE 'No Discounts'
    END AS Discount_Behavior,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue_Per_Customer,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Percentage
FROM customer_360
GROUP BY 
    CASE 
        WHEN Transactions_With_Discount > 0 THEN 'Used Discounts'
        ELSE 'No Discounts'
    END
ORDER BY Total_Revenue DESC;



---- Customer Rating Analysis


SELECT 
    ROUND(AVG(Avg_Customer_Rating), 2) AS Avg_Rating_Overall,
    ROUND(MIN(Avg_Customer_Rating), 2) AS Min_Rating,
    ROUND(MAX(Avg_Customer_Rating), 2) AS Max_Rating
FROM customer_360;

-- Ratings distribution (buckets)

SELECT 
    CASE 
        WHEN Avg_Customer_Rating < 2 THEN 'Low (<2)'
        WHEN Avg_Customer_Rating BETWEEN 2 AND 3 THEN 'Moderate (2–3)'
        WHEN Avg_Customer_Rating BETWEEN 3 AND 4 THEN 'Good (3–4)'
        ELSE 'Excellent (>4)'
    END AS Rating_Group,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue_Per_Customer,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Percentage
FROM customer_360
GROUP BY 
    CASE 
        WHEN Avg_Customer_Rating < 2 THEN 'Low (<2)'
        WHEN Avg_Customer_Rating BETWEEN 2 AND 3 THEN 'Moderate (2–3)'
        WHEN Avg_Customer_Rating BETWEEN 3 AND 4 THEN 'Good (3–4)'
        ELSE 'Excellent (>4)'
    END
ORDER BY Rating_Group;


---Channel Usage

SELECT 
    Channels_Used,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(AVG(Monetary), 2) AS Avg_Revenue
FROM customer_360
GROUP BY Channels_Used
ORDER BY Total_Revenue DESC;

----Profit vs Discount Correlation

----This means that for every ₹1 of discount given, the company earns ₹5.51 in profit.

SELECT 
    ROUND(AVG(Total_Discount), 2) AS Avg_Discount,
    ROUND(AVG(Total_Profit), 2) AS Avg_Profit,
    ROUND(AVG(Total_Profit) / NULLIF(AVG(Total_Discount), 0), 2) AS Profit_to_Discount_Ratio 
FROM customer_360;


---Year-wise Revenue, Profit, Discount & Revenue Share


SELECT 
    YEAR(First_Transaction_Date) AS Transaction_Year,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    COUNT(Customer_ID) AS Num_New_Customers,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Discount), 2) AS Total_Discount,
    ROUND(SUM(Total_Profit) * 100.0 / NULLIF(SUM(Monetary), 0), 2) AS Profit_Margin_Percentage,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Share_Percentage
FROM customer_360
GROUP BY YEAR(First_Transaction_Date)
ORDER BY Transaction_Year;

--- year wise 

SELECT 
    YEAR(Last_Transaction_Date) AS Year,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Profit) * 100.0 / SUM(Monetary), 2) AS Profit_Margin_Percentage,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Share_Percentage,
    ROUND(SUM(Total_Profit) * 100.0 / SUM(SUM(Total_Profit)) OVER (), 2) AS Profit_Share_Percentage,
    COUNT(Customer_ID) AS Total_Customers
FROM Customer_360
GROUP BY YEAR(Last_Transaction_Date)
ORDER BY Year;

---Month-wise Revenue, Profit & Share

SELECT 
    DATENAME(MONTH, Last_Transaction_Date) AS Month_Name,
    MONTH(Last_Transaction_Date) AS Month_Number,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Share_Percentage,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit
FROM customer_360
GROUP BY DATENAME(MONTH, Last_Transaction_Date), MONTH(Last_Transaction_Date)
ORDER BY Month_Number;


---Year + Month Segmentation

SELECT 
    YEAR(Last_Transaction_Date) AS Year,
    DATENAME(MONTH, Last_Transaction_Date) AS Month_Name,
    MONTH(Last_Transaction_Date) AS Month_Number,
    ROUND(SUM(Monetary), 2) AS Total_Revenue,
    ROUND(SUM(Monetary) * 100.0 / SUM(SUM(Monetary)) OVER (), 2) AS Revenue_Share_Percentage,
    COUNT(Customer_ID) AS Num_Customers,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit
FROM customer_360
GROUP BY YEAR(Last_Transaction_Date), DATENAME(MONTH, Last_Transaction_Date), MONTH(Last_Transaction_Date)
ORDER BY Total_Profit DESC;

--top-performing states or customer segments 

SELECT
    customer_state,
    COUNT(*) AS Total_Customers,
    ROUND(AVG(Monetary),2) AS Avg_Monetary,
    ROUND(STDEV(Monetary),2) AS Std_Monetary,
    ROUND(AVG(Total_Profit),2) AS Avg_Profit,
    ROUND(STDEV(Total_Profit),2) AS Std_Profit,
    ROUND(AVG(Frequency),2) AS Avg_Frequency
FROM Customer_360
GROUP BY customer_state
ORDER BY Total_Customers DESC;



--============== Order_360 =================--


--- Revenue & Profit by Channel

SELECT
    channel AS Channel_Name,
    COUNT(Order_ID) AS Num_Orders,
    ROUND(SUM(Total_Amount), 2) AS Total_Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Revenue_Per_Order,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Amount) * 100.0 / SUM(SUM(Total_Amount)) OVER (), 2) AS Revenue_Percentage
FROM order_360
GROUP BY channel
ORDER BY Total_Revenue DESC;

---Revenue & Profit by Store / Seller City

SELECT
    Seller_City,
    COUNT(Order_ID) AS Num_Orders,
    ROUND(SUM(Total_Amount), 2) AS Total_Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Revenue_Per_Order,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Amount) * 100.0 / SUM(SUM(Total_Amount)) OVER (), 2) AS Revenue_Percentage
FROM order_360
GROUP BY Seller_City
ORDER BY Total_Revenue DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY; -- Top 10 cities


--- Discount vs No Discount Orders

SELECT
    CASE 
        WHEN Total_Discount > 0 THEN 'Used Discount'
        ELSE 'No Discount'
    END AS Discount_Behavior,
    COUNT(Order_ID) AS Num_Orders,
    ROUND(SUM(Total_Amount), 2) AS Total_Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Revenue_Per_Order,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Amount) * 100.0 / SUM(SUM(Total_Amount)) OVER (), 2) AS Revenue_Percentage
FROM order_360
GROUP BY 
    CASE 
        WHEN Total_Discount > 0 THEN 'Used Discount'
        ELSE 'No Discount'
    END
ORDER BY Total_Revenue DESC;


---- Loss Making vs High Profit Orders


SELECT
    CASE 
        WHEN Flag_Loss_Making = 1 THEN 'Loss Making'
        WHEN Flag_High_Profit = 1 THEN 'High Profit'
        ELSE 'Normal Profit'
    END AS Profit_Flag,
    COUNT(Order_ID) AS Num_Orders,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Amount), 2) AS Total_Revenue,
    ROUND(SUM(Total_Amount) * 100.0 / SUM(SUM(Total_Amount)) OVER (), 2) AS Revenue_Percentage
FROM order_360
GROUP BY 
    CASE 
        WHEN Flag_Loss_Making = 1 THEN 'Loss Making'
        WHEN Flag_High_Profit = 1 THEN 'High Profit'
        ELSE 'Normal Profit'
    END
ORDER BY Total_Profit DESC;


---- Weekend vs Weekday Orders


SELECT
    CASE Weekend_Transaction_Flag
        WHEN 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS Transaction_Type,
    COUNT(Order_ID) AS Num_Orders,
    ROUND(SUM(Total_Amount), 2) AS Total_Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Revenue_Per_Order,
    ROUND(SUM(Total_Amount) * 100.0 / SUM(SUM(Total_Amount)) OVER (), 2) AS Revenue_Percentage
FROM order_360
GROUP BY CASE Weekend_Transaction_Flag
             WHEN 1 THEN 'Weekend'
             ELSE 'Weekday'
         END
ORDER BY Total_Revenue DESC;


--- Hourly Distribution of Orders


SELECT
    Hour_Flag AS Order_Hour_Bucket,
    COUNT(Order_ID) AS Num_Orders,
    ROUND(SUM(Total_Amount), 2) AS Total_Revenue,
    ROUND(AVG(Total_Amount), 2) AS Avg_Revenue_Per_Order,
    ROUND(SUM(Total_Amount) * 100.0 / SUM(SUM(Total_Amount)) OVER (), 2) AS Revenue_Percentage
FROM order_360
GROUP BY Hour_Flag
ORDER BY Order_Hour_Bucket;


--=========== Store_360 =========--

--Total Revenue and Profit per Store

SELECT
    StoreID,
    Store_City,
    Store_State,
    ROUND(Total_Sales, 2) AS Total_Revenue,
    ROUND(Total_Profit, 2) AS Total_Profit,
    ROUND(Total_Sales * 100.0 / SUM(Total_Sales) OVER (), 2) AS Revenue_Percentage
FROM store_360
ORDER BY Total_Revenue DESC;


--- Discount Behavior


SELECT
    StoreID,
    Store_City,
    Store_State,
    ROUND(Total_Discount, 2) AS Total_Discount,
    ROUND(Total_Discount * 100.0 / Total_Sales, 2) AS Discount_Percentage_of_Sales
FROM store_360
ORDER BY Discount_Percentage_of_Sales DESC;


--- Transaction timing

SELECT
    StoreID,
    Store_City,
    Store_State,
    Weekend_Transactions,
    Weekday_Transactions,
    Morning_Transactions,
    Afternoon_Transactions,
    Evening_Transactions,
    Night_Transactions
FROM store_360
ORDER BY Weekend_Transactions DESC;

---- Product vareity


SELECT
    StoreID,
    Store_City,
    Store_State,
    Distinct_Categories AS Distinct_Product_Categories
FROM store_360
ORDER BY Distinct_Categories DESC;

---- Revenue by states


SELECT
    Store_State,
    COUNT(StoreID) AS Num_Stores,
    ROUND(SUM(Total_Sales), 2) AS Total_Revenue,
    ROUND(SUM(Total_Profit), 2) AS Total_Profit,
    ROUND(SUM(Total_Sales) * 100.0 / SUM(SUM(Total_Sales)) OVER (), 2) AS Revenue_Percentage
FROM store_360
GROUP BY Store_State
ORDER BY Total_Revenue DESC;

--- total number of products

select count(distinct product_id) as  products
from Orders_Cleaned

-- average discount

SELECT 
    ROUND(100.0 * SUM(Total_Discount) / SUM(Monetary), 2) AS Avg_Discount_Percent
FROM Customer_360;


