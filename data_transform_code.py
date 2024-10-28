import pandas as pd

# Function to load data
def load_data(filepath):
    try:
        df = pd.read_csv(filepath)
        print('Data Loaded Successfully.')
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

# Function for cleaning data
def clean_data(df):
    # Handling missing data
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

    print("Data cleaned successfully.")
    return df

# Function for transforming the data
def transform_data(df):
    """
    Perform data transformation such as feature engineering.

    Args:
    - df (DataFrame): The DataFrame to transform.

    Returns:
    - DataFrame: Transformed DataFrame.
    """
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

    # Additional transformations can be added here as needed
    print("Data transformed successfully.")
    return df

# Function to save the transformed data
def save_transformed_data(df, output_filepath):
    """
    Save the transformed data to a specified file path.

    Args:
    - df (DataFrame): The DataFrame to save.
    - output_filepath (str): Path to save the transformed data.
    """
    try:
        df.to_pickle(output_filepath)
        print("Transformed data saved successfully.")
    except Exception as e:
        print(f"Error saving transformed data: {e}")

# Main function to run the transformation automation
def main(input_filepath, output_filepath):
    # Load the data
    df = load_data(input_filepath)
    if df is not None:
        # Clean the data
        cleaned_df = clean_data(df)
        # Transform the data
        transformed_df = transform_data(cleaned_df)
        # Save the transformed data
        save_transformed_data(transformed_df, output_filepath)

if __name__ == "__main__":
    # Define input and output file paths
    input_filepath = 'data/retail_sales_dataset.csv'  # Change this to your actual input file path
    output_filepath = 'transform_data.pkl'  # Change this to your desired output path

    # Run the automation
    main(input_filepath, output_filepath)