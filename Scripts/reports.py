import pandas as pd
import os
import time  # Importing time for tracking duration
from Scripts.logger_setup import logger  # Importing the configured logger
import matplotlib.pyplot as plt 
from datetime import datetime
import time

# Global variables for monitoring
metrics = {
    'load_time': [],
    'clean_time': [],
    'transform_time': [],
    'dates': [],  # List to store the dates for tracking ETL run times
    'load_success': 0,
    'clean_success': 0,
    'transform_success': 0
}

# Function to load data
def load_data(filepath):
    start_time = time.time()  # Start timing
    try:
        directory = os.path.dirname(filepath)
        if directory:  # Only try to create directory if it exists in the path
            os.makedirs(directory, exist_ok=True)
            
        _, file_extension = os.path.splitext(filepath)
        if file_extension == '.csv':
            df = pd.read_csv(filepath)
        elif file_extension == '.pkl':
            df = pd.read_pickle(filepath)
        else:
            logger.warning(f"Unsupported file format: {file_extension}")
            return None
        
        if df is not None:
            logger.info("Data loaded successfully.")
            metrics['load_success'] += 1
        else:
            logger.warning("Warning: Loaded data is None.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None
    finally:
        metrics['load_time'].append(time.time() - start_time)  # Log load time

# Function for cleaning the data
def clean_data(df):
    start_time = time.time()  # Start timing
    try:
        # Handle missing values
        df.dropna(inplace=True)

        # Ensure correct data types
        # (Same cleaning code as before)
        df['Transaction ID'] = df['Transaction ID'].astype(int)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Customer ID'] = df['Customer ID'].astype(str)
        df['Gender'] = df['Gender'].astype('category')
        df['Product Category'] = df['Product Category'].astype('category')
        df['Quantity'] = df['Quantity'].astype(int)
        df['Price per Unit'] = df['Price per Unit'].astype(float)
        df['Total Amount'] = df['Total Amount'].astype(float)

        # Value constraints
        df = df[(df['Quantity'] > 0) & (df['Price per Unit'] > 0)]

        logger.info("Data cleaned successfully.")
        metrics['clean_success'] += 1
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
    finally:
        metrics['clean_time'].append(time.time() - start_time)  # Log clean time
        return df   
     
# Function to validate cleaned data
def validate_cleaned_data(df):
    """
    Validates the cleaned data to ensure it meets necessary conditions.

    Args:
    - df (DataFrame): The cleaned DataFrame.

    Returns:
    - bool: True if validation passes, False otherwise.
    """
    try:
        # Check data types
        assert df['Transaction ID'].dtype == int, "Transaction Id should be integer"
        assert pd.api.types.is_datetime64_any_dtype(df['Date']), "Date should be datetime"
        assert df['Quantity'].min() > 0, "Quantity should be positive"
        assert df['Price per Unit'].min() > 0, "Price per Unit should be positive"
        
        logger.info("Cleaned data validation passed.")
        return True
    except AssertionError as e:
        logger.error(f"Validation failed: {e}")
        return False

# Function to save cleaned data
def save_cleaned_data(df, cleaned_filepath):
    """
    Save the cleaned data to a specified file path.

    Args:
    - df (DataFrame): The DataFrame to save.
    - cleaned_filepath (str): Path to save the cleaned data.
    """
    try:
        df.to_pickle(cleaned_filepath)
        logger.info("Cleaned data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving cleaned data: {e}")    

# Function for transforming the data
def transform_data(df):
    start_time = time.time()  # Start timing
    if df is None:
        logger.error("Error: DataFrame is None. Transformation skipped.")
        return None
    try:
        # (Transformation code as before)
        # Create a Day of Week column
        df['Day of Week'] = df['Date'].dt.day_name()
        # Example transformation: Add a 'Season' column based on 'Date'
        df['Month'] = df['Date'].dt.month

        def get_season(month):
            if month in [12, 1, 2]:
                return 'Winter'
            elif month in [3, 4, 5]:
                return 'Spring'
            elif month in [6, 7, 8]:
                return 'Summer'
            else:
                return 'Fall'

        df['Season'] = df['Month'].apply(get_season)

        # Add a derived column, Revenue
        df['Revenue'] = df['Quantity'] * df['Price per Unit']        

        logger.info("Data transformed successfully.")
        metrics['transform_success'] += 1
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
    finally:
        metrics['transform_time'].append(time.time() - start_time)  # Log transform time
        return df

# Function to validate transformed data
def validate_transformed_data(df):
    """
    Validates the transformed data to ensure transformations were successful.

    Args:
    - df (DataFrame): The transformed DataFrame.

    Returns:
    - bool: True if validation passes, False otherwise.
    """
    try:
        # Check that 'Revenue' and 'Season' columns are present
        assert 'Revenue' in df.columns, "Revenue column is missing"
        assert 'Season' in df.columns, "Season column is missing"
        
        # Check that 'Revenue' values are positive
        assert df['Revenue'].min() >= 0, "Revenue should be non-negative"
        
        logger.info("Transformed data validation passed.")
        return True
    except AssertionError as e:
        logger.error(f"Validation failed: {e}")
        return False

# Function to save transformed data
def save_transformed_data(df, transformed_filepath):
    """
    Save the transformed data to a specified file path.

    Args:
    - df (DataFrame): The DataFrame to save.
    - transformed_filepath (str): Path to save the transformed data.
    """
    try:
        df.to_pickle(transformed_filepath)
        logger.info("Transformed data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving transformed data: {e}")


# Function to print monitoring results
def print_monitoring_results():
    logger.info("Monitoring Results:")
    logger.info(f"Load Times: {metrics['load_time']}")
    logger.info(f"Clean Times: {metrics['clean_time']}")
    logger.info(f"Transform Times: {metrics['transform_time']}")
    logger.info(f"Load Successes: {metrics['load_success']}")
    logger.info(f"Clean Successes: {metrics['clean_success']}")
    logger.info(f"Transform Successes: {metrics['transform_success']}")


# Functions to Plot visualizations
def plot_success_rates(metrics):
    categories = ['Load Success', 'Clean Success', 'Transform Success']
    success_counts = [metrics['load_success'], metrics['clean_success'], metrics['transform_success']]
    
    plt.bar(categories, success_counts, color=['blue', 'orange', 'green'])
    plt.title('Success Rates of ETL Pipeline')
    plt.ylabel('Number of Successful Processes')
    
    success_rates_path = 'reports/success_rates.png'
    # Create the 'reports' directory if it doesn't exist
    os.makedirs(os.path.dirname(success_rates_path), exist_ok=True)

    plt.savefig(success_rates_path) # Save the plot
    plt.show()  # Debugging: Verify plot display
    plt.close() # Close the plot to free memory
    return success_rates_path

def plot_trends(metrics):
    plt.plot(metrics['dates'], metrics['load_time'], label='Load Time', marker='o')
    plt.plot(metrics['dates'], metrics['clean_time'], label='Clean Time', marker='o')
    plt.plot(metrics['dates'], metrics['transform_time'], label='Transform Time', marker='o')

    plt.title('ETL Process Time Trends')
    plt.xlabel('Date')
    plt.ylabel('Time (seconds)')
    plt.legend()

    trends_path = 'reports/trends.png'
    plt.savefig(trends_path)
    plt.show()  # Debugging: Verify plot display
    plt.close()
    return trends_path

# Generating HMTL report
def generate_report(report_filepath, success_rates_path, trends_path):
    os.makedirs(os.path.dirname(report_filepath), exist_ok=True)
    with open(report_filepath, 'w') as f:
        f.write("<html><body><h1>Automated ETL Pipeline Report</h1>\n")
        f.write("<h2>Success Rates:</h2>\n")
        f.write(f"<img src='{success_rates_path}' alt='Success Rates'>\n")
        f.write("<h2>Trends Over Time:</h2>\n")
        f.write(f"<img src='{trends_path}' alt='Trends'>\n")
        f.write("</body></html>")



def main(input_filepath, cleaned_filepath, transformed_filepath, report_filepath):
    # Load the initial data
    start_time = time.time()  # Start timer for loading
    df = load_data(input_filepath)
    if df is not None:
        load_time = time.time() - start_time  # Calculate load time
        metrics['load_time'].append(load_time)  # Append load time
        metrics['dates'].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Capture load date

        # Clean the data
        start_time = time.time()  # Start timer for cleaning
        cleaned_df = clean_data(df)
        
        # Validate cleaned data
        if validate_cleaned_data(cleaned_df):
            # Save cleaned data
            save_cleaned_data(cleaned_df, cleaned_filepath)
            clean_time = time.time() - start_time  # Calculate clean time
            metrics['clean_time'].append(clean_time)  # Append clean time
            metrics['dates'].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Capture clean date
            
            # Load cleaned data for transformation
            cleaned_df = load_data(cleaned_filepath)
            if cleaned_df is not None:
                # Transform the data
                start_time = time.time()  # Start timer for transformation
                transformed_df = transform_data(cleaned_df)
                
                # Validate transformed data
                if validate_transformed_data(transformed_df):
                    # Save the transformed data
                    save_transformed_data(transformed_df, transformed_filepath)
                    transform_time = time.time() - start_time  # Calculate transform time
                    metrics['transform_time'].append(transform_time)  # Append transform time
                    metrics['dates'].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Capture transform date

    # Print monitoring results
    print_monitoring_results()

    # Plot and generate report
    success_rates_path = plot_success_rates(metrics)
    trends_path = plot_trends(metrics)
    generate_report(report_filepath, success_rates_path, trends_path)
    logger.info("ETL process and report generation completed.")


if __name__ == "__main__":
    # Define file paths
    input_filepath = 'data/retail_sales_dataset.csv'
    cleaned_filepath = 'cleaned_data.pkl'
    transformed_filepath = 'transform_data.pkl'
    report_filepath = 'etl_report.html'
    
    # Run the data pipeline
    main(input_filepath, cleaned_filepath, transformed_filepath, report_filepath)
