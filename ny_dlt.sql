-- Databricks notebook source
CREATE STREAMING LIVE TABLE customers_bronze
AS SELECT *,current_timestamp() as ingestion_date FROM cloud_files("dbfs:/mnt/radababricks/databrickstraining/dltsample/customers/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

CREATE STREAMING LIVE TABLE orders_bronze
AS SELECT * FROM cloud_files("dbfs:/mnt/radababricks/databrickstraining/dltsample/orders/", "csv",map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

Create streaming table orders_silver
as
select order_id,customer_id,order_date,product,quantity,amount from STREAM(LIVE.orders_bronze)

-- COMMAND ----------

create streaming table customer_silver
(
  CONSTRAINT valid_customer_id EXPECT(customer_id IS NOT NULL) ON VIOLATION DROP ROW
)
as
select customer_id,customer_name,email,phone,city,state from STREAM(LIVE.customers_bronze)

-- COMMAND ----------

CREATE STREAMING TABLE customer_order_silver 
as
select o.order_id,c.customer_id,c.customer_name,c.email,c.city
FROM STREAM(LIVE.customer_silver) c
JOIN LIVE.orders_silver o
ON c.customer_id=o.customer_id

-- COMMAND ----------

create live table customer_count
as
select city,count(city) as count from 
LIVE.customer_order_silver 
group by city
