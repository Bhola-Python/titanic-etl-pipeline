PySpark ETL Project — Cleaning the Titanic Dataset & Loading into MySQL

This project is a simple, beginner-friendly ETL pipeline built using PySpark.
It takes the raw Titanic dataset, cleans and transforms it, and finally loads the cleaned data into a MySQL database using a JDBC connector.

I made this project to learn how real-world data engineering works:

Extract → Transform → Load.

🔍 What the Project Does (Simple Explanation)
1. Extract

Reads the Titanic CSV file using PySpark.

2. Transform

Cleans and prepares the data using PySpark:
Removes the Cabin column (too many missing values)
Calculates the average Age for each gender
Fills missing Age values using these averages
Fills missing Embarked values using "S" (most common)
Checks for remaining null values after cleaning

3. Load

Connects PySpark to MySQL using the MySQL JDBC driver
Loads the cleaned dataset into a table called titanic_cleaned
Stores it inside the titanicdb database

🛠️ Technologies Used

Python
PySpark (Spark SQL & DataFrame API)
MySQL
MySQL JDBC Connector

SparkSession with JDBC

📁 Project Files
ETL.py          → Main ETL code
Titanic_Dataset.csv  → Raw dataset
README.md       → Project documentation
mysql-connector-j-9.5.0.jar → JDBC driver

⚙️ How to Run the Project
1. Install PySpark
pip install pyspark

2. Install MySQL

Create a database:
CREATE DATABASE titanicdb;


Create a user (optional):

CREATE USER 'rororo'@'localhost' IDENTIFIED BY 'yourpassword';
GRANT ALL PRIVILEGES ON titanicdb.* TO 'rororo'@'localhost';
FLUSH PRIVILEGES;

3. Download JDBC Driver

Download MySQL Connector/J ZIP → extract → move the .jar file to:

4. Run the Script
python ETL.py

📊 Verifying the Loaded Data

In MySQL:

USE titanicdb;
SHOW TABLES;
SELECT * FROM titanic_cleaned LIMIT 10;


If you see rows → your ETL pipeline worked!

⭐ Why This Project Matters

This project shows:

How PySpark is used to clean and transform real data

How missing values are handled in industry

How Spark connects to databases with JDBC

How an end-to-end ETL pipeline works

This is the exact foundation used in Data Engineering jobs.

Even though it’s a small dataset, the flow is the same as real-world pipelines.

🚀 Future Improvements

You can extend this project by:

Adding API data (instead of CSV)

Adding Airflow scheduling

Writing cleaned data to S3, Snowflake, or BigQuery

Creating dashboards using Power BI or Tableau

Adding logging and validation layers

👤 Author

Bhola Rajesh Vishwakarma
Learning Data Engineering with PySpark
Open to internships and junior roles 😊
