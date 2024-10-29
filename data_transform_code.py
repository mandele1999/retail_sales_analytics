import pandas as pd

# Function to load data
def load_data(filepath):
    """
    Load data from a specified file path.

    Args:
    - filepath (str): Path to the data file.

    Returns:
    - DataFrame: Loaded data as a pandas DataFrame.
    """
    try:
        df = pd.read_csv(filepath)
        if df is not None:
            print("Data loaded successfully.")
        else:
            print("Warning: Loaded data is None.")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Function for cleaning the data
def clean_data(df):
    """
    Clean the data by handling missing values and ensuring correct data types.

    Args:
    - df (DataFrame): The DataFrame to clean.

    Returns:
    - DataFrame: Cleaned DataFrame.
    """
    # Handle missing values
    df.dropna(inplace=True)

    # Ensure correct data types
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
    
    print("Data cleaned successfully.")
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
        
        print("Cleaned data validation passed.")
        return True
    except AssertionError as e:
        print(f"Validation failed: {e}")
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
        print("Cleaned data saved successfully.")
    except Exception as e:
        print(f"Error saving cleaned data: {e}")

# Function for transforming the data
def transform_data(df):
    """
    Perform data transformation, including feature engineering.

    Args:
    - df (DataFrame): The DataFrame to transform.

    Returns:
    - DataFrame: Transformed DataFrame.
    """

    if df is None:
        print("Error: DataFrame is None. Transformation skipped.")
        return None    
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
    
    print("Data transformed successfully.")
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
        
        print("Transformed data validation passed.")
        return True
    except AssertionError as e:
        print(f"Validation failed: {e}")
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
        df.to_pickle('transform_data.pkl')
        print("Transformed data saved successfully.")
    except Exception as e:
        print(f"Error saving transformed data: {e}")

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
            
            # Transform the data
            transformed_df = transform_data(cleaned_df)
            
            # Validate transformed data
            if validate_transformed_data(transformed_df):
                # Save the transformed data
                save_transformed_data(transformed_df, transformed_filepath)

if __name__ == "__main__":
    # Define file paths
    input_filepath = 'data/retail_sales_dataset.csv'
    cleaned_filepath = 'cleaned_data.pkl'
    transformed_filepath = 'transform_data.pkl'
    
    # Run the data pipeline
    main(input_filepath, cleaned_filepath, transformed_filepath)
