import pandas as pd
import os
import time
from logger_setup import logger
import matplotlib.pyplot as plt
from datetime import datetime

# Global variables for monitoring
metrics = {
    'load_time': [],
    'clean_time': [],
    'transform_time': [],
    'dates': [],
    'load_success': 0,
    'clean_success': 0,
    'transform_success': 0
}

# Function to load data
def load_data(filepath):
    start_time = time.time()
    try:
        directory = os.path.dirname(filepath)
        if directory:
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
        metrics['load_time'].append(time.time() - start_time)

# Function for cleaning the data
def clean_data(df):
    start_time = time.time()
    try:
        df.dropna(inplace=True)
        df['Transaction ID'] = df['Transaction ID'].astype(int)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Customer ID'] = df['Customer ID'].astype(str)
        df['Gender'] = df['Gender'].astype('category')
        df['Product Category'] = df['Product Category'].astype('category')
        df['Quantity'] = df['Quantity'].astype(int)
        df['Price per Unit'] = df['Price per Unit'].astype(float)
        df['Total Amount'] = df['Total Amount'].astype(float)
        df = df[(df['Quantity'] > 0) & (df['Price per Unit'] > 0)]
        logger.info("Data cleaned successfully.")
        metrics['clean_success'] += 1
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
    finally:
        metrics['clean_time'].append(time.time() - start_time)
        return df

# Function to validate cleaned data
def validate_cleaned_data(df):
    try:
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
    try:
        df.to_pickle(cleaned_filepath)
        logger.info("Cleaned data saved successfully.")
    except Exception as e:
        logger.error(f"Error saving cleaned data: {e}")

# Function for transforming the data
def transform_data(df):
    start_time = time.time()
    if df is None:
        logger.error("Error: DataFrame is None. Transformation skipped.")
        return None
    try:
        df['Day of Week'] = df['Date'].dt.day_name()
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
        df['Revenue'] = df['Quantity'] * df['Price per Unit']
        logger.info("Data transformed successfully.")
        metrics['transform_success'] += 1
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
    finally:
        metrics['transform_time'].append(time.time() - start_time)
        return df

# Function to validate transformed data
def validate_transformed_data(df):
    try:
        assert 'Revenue' in df.columns, "Revenue column is missing"
        assert 'Season' in df.columns, "Season column is missing"
        assert df['Revenue'].min() >= 0, "Revenue should be non-negative"
        logger.info("Transformed data validation passed.")
        return True
    except AssertionError as e:
        logger.error(f"Validation failed: {e}")
        return False

# Function to save transformed data
def save_transformed_data(df, transformed_filepath):
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

# Functions to plot visualizations
def plot_success_rates(metrics):
    categories = ['Load Success', 'Clean Success', 'Transform Success']
    success_counts = [metrics['load_success'], metrics['clean_success'], metrics['transform_success']]
    
    plt.bar(categories, success_counts, color=['blue', 'orange', 'green'])
    plt.title('Success Rates of ETL Pipeline')
    plt.ylabel('Number of Successful Processes')

    success_rates_path = 'reports/success_rates.png'
    os.makedirs(os.path.dirname(success_rates_path), exist_ok=True)

    try:
        plt.savefig(success_rates_path)
        plt.show()
    except Exception as e:
        logger.error(f"Error displaying/saving success rates plot: {e}")
    finally:
        plt.close()
    
    return success_rates_path

def plot_trends(metrics):
    # Check if metrics lists are empty
    if not metrics['dates']:
        logger.error("The 'dates' list is empty, cannot plot trends.")
        return None

    # Check for consistency in lengths
    metrics_lengths = {
        'load_time': len(metrics['load_time']),
        'clean_time': len(metrics['clean_time']),
        'transform_time': len(metrics['transform_time']),
    }

    for metric, length in metrics_lengths.items():
        if length != len(metrics['dates']):
            logger.error(f"Mismatch in lengths: {metric} has {length} entries but dates has {len(metrics['dates'])}.")
            return None

    # Proceed with plotting
    plt.plot(metrics['dates'], metrics['load_time'], label='Load Time', marker='o')
    plt.plot(metrics['dates'], metrics['clean_time'], label='Clean Time', marker='o')
    plt.plot(metrics['dates'], metrics['transform_time'], label='Transform Time', marker='o')

    plt.title('ETL Process Time Trends')
    plt.xlabel('Date')
    plt.ylabel('Time (seconds)')
    plt.legend()

    trends_path = 'reports/trends.png'
    os.makedirs(os.path.dirname(trends_path), exist_ok=True)

    try:
        plt.savefig(trends_path)
        plt.show()
    except Exception as e:
        logger.error(f"Error displaying/saving trends plot: {e}")
    finally:
        plt.close()

    return trends_path


# Generating HTML report
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
    start_time = time.time()
    df = load_data(input_filepath)
    if df is not None:
        metrics['load_time'].append(time.time() - start_time)
        metrics['dates'].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Date after load success

        cleaned_df = clean_data(df)
        
        if validate_cleaned_data(cleaned_df):
            save_cleaned_data(cleaned_df, cleaned_filepath)
            metrics['clean_time'].append(time.time() - start_time)  # Append clean time
            metrics['dates'].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Date after clean success

            cleaned_df = load_data(cleaned_filepath)
            if cleaned_df is not None:
                transformed_df = transform_data(cleaned_df)
                
                if validate_transformed_data(transformed_df):
                    save_transformed_data(transformed_df, transformed_filepath)
                    metrics['transform_time'].append(time.time() - start_time)  # Append transform time
                    metrics['dates'].append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # Date after transform success
                else:
                    logger.error("Transformed data validation failed.")
                    metrics['transform_time'].append(None)  # Append None for failed transform
                    metrics['dates'].append(None)  # Append None for failed transform
            else:
                logger.error("Failed to load cleaned data.")
                metrics['transform_time'].append(None)  # Append None for failed load
                metrics['dates'].append(None)  # Append None for failed load
        else:
            logger.error("Cleaned data validation failed.")
            metrics['clean_time'].append(None)  # Append None for failed clean
            metrics['dates'].append(None)  # Append None for failed clean
    else:
        logger.error("Failed to load initial data.")
        metrics['load_time'].append(None)  # Append None for failed load
        metrics['dates'].append(None)  # Append None for failed load

    print_monitoring_results()

    success_rates_path = plot_success_rates(metrics)
    trends_path = plot_trends(metrics)
    generate_report(report_filepath, success_rates_path, trends_path)
    logger.info("ETL process and report generation completed.")


if __name__ == "__main__":
    input_filepath = 'data/retail_sales_dataset.csv'
    cleaned_filepath = 'cleaned_data.pkl'
    transformed_filepath = 'transform_data.pkl'
    report_filepath = 'reports/etl_report.html'
    
    main(input_filepath, cleaned_filepath, transformed_filepath, report_filepath)