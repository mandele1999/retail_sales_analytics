import pandas as pd
import os
import time  # Importing time for tracking duration
from logger_setup import logger  # Importing the configured logger

# Global variables for monitoring
metrics = {
    'load_time': [],
    'clean_time': [],
    'transform_time': [],
    'load_success': 0,
    'clean_success': 0,
    'transform_success': 0
}

# Function to load data
def load_data(filepath):
    start_time = time.time()  # Start timing
    try:
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

        logger.info("Data cleaned successfully.")
        metrics['clean_success'] += 1
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
    finally:
        metrics['clean_time'].append(time.time() - start_time)  # Log clean time
        return df

# Function for transforming the data
def transform_data(df):
    start_time = time.time()  # Start timing
    if df is None:
        logger.error("Error: DataFrame is None. Transformation skipped.")
        return None
    try:
        # (Transformation code as before)

        logger.info("Data transformed successfully.")
        metrics['transform_success'] += 1
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
    finally:
        metrics['transform_time'].append(time.time() - start_time)  # Log transform time
        return df

# Function to print monitoring results
def print_monitoring_results():
    logger.info("Monitoring Results:")
    logger.info(f"Load Times: {metrics['load_time']}")
    logger.info(f"Clean Times: {metrics['clean_time']}")
    logger.info(f"Transform Times: {metrics['transform_time']}")
    logger.info(f"Load Successes: {metrics['load_success']}")
    logger.info(f"Clean Successes: {metrics['clean_success']}")
    logger.info(f"Transform Successes: {metrics['transform_success']}")

# Main function to run the data pipeline
def main(input_filepath, cleaned_filepath, transformed_filepath):
    # Load the initial data
    df = load_data(input_filepath)
    if df is not None:
        # Clean the data
        cleaned_df = clean_data(df)
        
        # Validate cleaned data
        if validate_cleaned_data(cleaned_df):
            # Save cleaned data
            save_cleaned_data(cleaned_df, cleaned_filepath)
            
            # Load cleaned data for transformation
            cleaned_df = load_data(cleaned_filepath)
            if cleaned_df is not None:
                # Transform the data
                transformed_df = transform_data(cleaned_df)
                
                # Validate transformed data
                if validate_transformed_data(transformed_df):
                    # Save the transformed data
                    save_transformed_data(transformed_df, transformed_filepath)

    # Print monitoring results
    print_monitoring_results()

if __name__ == "__main__":
    # Define file paths
    input_filepath = 'data/retail_sales_dataset.csv'
    cleaned_filepath = 'cleaned_data.pkl'
    transformed_filepath = 'transform_data.pkl'
    
    # Run the data pipeline
    main(input_filepath, cleaned_filepath, transformed_filepath)
