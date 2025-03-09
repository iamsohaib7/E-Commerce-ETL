# **Olist E-Commerce ETL & Data Warehousing Project**  

## **Introduction**  
E-commerce businesses generate vast amounts of transactional and customer data daily, making it challenging to extract insights for business growth and operational efficiency. The **Olist E-Commerce Dataset** contains real-world data on orders, customers, products, payments, and sellers.  

This project focuses on building a **scalable ETL pipeline** to extract, transform, and load data into a **Snowflake data warehouse**. The goal is to enable efficient analytical queries and generate actionable insights through a **BI dashboard**.  

## **Project Workflow**  
![Project Workflow](./dataflow-architecture/Architecture%20Daigram.png)  

1. **Data Extraction** – Loaded raw data from the Olist dataset into a **PostgreSQL** database.  
2. **Data Storage in S3** – Moved extracted data to **Amazon S3** as an intermediate storage layer.  
3. **Data Transformation** – Used **Pandas** to clean and transform the data.  
4. **Data Loading to Snowflake** – Designed a **data warehouse schema** and stored structured data in **Snowflake**.  
5. **Analytical Queries & Dashboarding** – Executed analytical queries and visualized insights in **Power BI/Tableau**.  

## **Database Schema**  
![Database Schema](./Schemas/Ecomerce%20Data%20Models.png)  

## **Data Warehouse Schema**  
![Data Warehouse Schema](./Schemas/Ecommece%20Dim%20Modeling.png)  

## **Tech Stack Used**  
- **Python** – Data extraction, transformation, and loading  
- **PostgreSQL** – Initial data storage  
- **Amazon S3** – Intermediate storage  
- **Snowflake** – Data warehouse for analytical queries  
- **Pandas/PyArrow** – Data transformation  
- **Power BI/Tableau** – Dashboard creation  

## **Key Analytical Questions**  
1. What are the **top-selling products and categories** over different time periods?  
2. How do **customer ratings impact repeat purchases**?  
3. What is the **average delivery time**, and how does it vary by location?  
4. Which **sellers generate the highest revenue**?  
5. Which **payment methods are most commonly used**?  
6. What is the **monthly revenue trend**, and how does seasonality affect sales?  

