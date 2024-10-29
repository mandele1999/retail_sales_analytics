# Retail Sales Data Engineering Project  

## Project Overview

This project is focused on developing a comprehensive data engineering workflow for a retail sales dataset, from data ingestion to analysis and visualization. We use a structured ETL (Extract, Transform, Load) pipeline to clean, transform, and aggregate the data to uncover insights into customer purchasing behavior, product popularity, and overall sales trends.

## Objectives

1. **Data Exploration and Cleaning**: Load the dataset, inspect and clean it by handling missing values, correcting data types, and ensuring data integrity.
2. **Data Transformation**: Apply feature engineering techniques to enhance the dataset with additional insights, such as purchase patterns by day of the week, seasonality, and consistency checks.
3. **Data Storage and Management**: (Optional) Store the cleaned and processed data in an SQL database to simulate a real-world data warehousing solution.
4. **Data Aggregation and Analysis**: Analyze key performance indicators (KPIs) such as total sales, customer segmentation, and product popularity across various dimensions.
5. **Data Visualization**: Generate insightful visualizations to communicate trends and findings effectively.
6. **ETL Automation and Monitoring**: Set up automation to handle the ETL process on a scheduled basis and monitor the workflow to detect issues in real time.

## Dataset

The dataset used in this project contains the following features:

* **Transaction ID**: Unique identifier for each transaction
* **Date**: Transaction date
* **Customer ID**: Unique identifier for each customer
* **Gender**: The gender of the customer
* **Product Category**: Category of the purchased product
* **Quantity**: Number of items purchased in the transaction
* **Price per Unit**: Price of each item
* **Total Amount**: Total amount spent in each transaction

## Project Structure

* **data**/: Directory for raw and cleaned datasets.
* **notebooks**/: Jupyter notebooks documenting each step of the ETL process, from data exploration to visualization.
* **scripts**/: Python scripts automating the ETL pipeline and data validation.
* **visualizations**/: Generated charts and graphs from the analysis.
* **README.md**: Project documentation and objectives.

## Getting Started

1. Clone this repo and install the required packages:
   git clone <https://github.com/mandele1999/retail_sales_analytics.git>
   cd retail-sales-data-engineering
   pip install -r requirements.txt
2. Download the dataset from Kaggle or use the data file provided in the data/ directory.
3. Open the notebooks/ directory and follow the steps in each notebook to run the ETL process and data visualizations.

## Usage

* **Data Cleaning**: Run notebooks/01_data_cleaning.ipynb to load and clean the raw dataset.
* **Data Transformation**: Use notebooks/02_data_transformation.ipynb to apply feature engineering and data validation.
* **Data Analysis & Visualization**: Execute notebooks/03_data_analysis.ipynb for aggregated reports and visualizations.

## Technologies

* **Python**: Data analysis and pipeline automation
* **Pandas**: Data manipulation and cleaning
* **Matplotlib / Seaborn**: Data visualization
* **SQL**: Optional data storage and querying

## Future Work

* Integrate data warehousing solutions for large-scale data management.
* Set up an Airflow pipeline for ETL automation and error handling.
* Implement machine learning models for predictive analytics on customer lifetime value and sales forecasting.
