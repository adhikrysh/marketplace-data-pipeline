import pandas as pd
import numpy as np

def cleaning_customers(input_file="customers.csv", output_file="clean_customers.csv"):
    """
    Cleans a customers CSV file by:
    - Stripping whitespace from all column names and cell values
    - Replacing empty or whitespace-only cells with NaN
    - Saving the cleaned dataframe to a new CSV

    Parameters:
        input_file (str): Path to the input CSV
        output_file (str): Path to save the cleaned CSV
    """
    # Read all columns as strings to handle whitespace properly
    df = pd.read_csv(input_file, dtype=str)
    df = df.rename(columns={None: "Unknown"})

    # Strip whitespace from column names
    df.columns = df.columns.str.strip()

    # Strip whitespace from all string values
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Replace empty strings or whitespace-only cells with NaN
    df = df.replace(r'^\s*$', np.nan, regex=True)
    
    print(df)
    # Save cleaned data
    #df.to_csv(output_file, index=False)
    return df

def main(): 
    cleaning_customers()
if __name__ == "__main__":
    main()