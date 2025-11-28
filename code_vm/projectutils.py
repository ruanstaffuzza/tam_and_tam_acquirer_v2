import pandas as pd
DATA_DIR = 'data'
import os

def accumulate_and_apply(df, func, id_column, min_rows_per_file):
    """
    Accumulates rows of a DataFrame based on a unique identifier and applies a function 
    to the accumulated DataFrame whenever a minimum row threshold is reached.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    func (function): The function to apply to the accumulated DataFrame.
    id_column (str): The name of the column with unique identifiers.
    min_rows_per_file (int): The minimum number of rows required before applying the function.
    """
    # Initialize the current file index and the DataFrame to accumulate rows
    current_file_index = 0
    accumulated_df = pd.DataFrame()
    
    # Loop through each unique value in the identifier column
    for unique_id in df[id_column].unique():
        # Subset the DataFrame for the current unique identifier
        subset_df = df[df[id_column] == unique_id]

        # Check if the accumulated DataFrame and the subset exceed the minimum row threshold
        if len(accumulated_df) + len(subset_df) > min_rows_per_file:
            # Apply the function to the accumulated DataFrame
            func(accumulated_df, current_file_index)
            # Increment the file index and reset the accumulated DataFrame
            current_file_index += 1
            accumulated_df = subset_df
        else:
            # Concatenate the subset DataFrame to the accumulated DataFrame
            accumulated_df = pd.concat([accumulated_df, subset_df], ignore_index=True)
    
    # Apply the function to any remaining rows in the accumulated DataFrame
    if not accumulated_df.empty:
        func(accumulated_df, current_file_index)
        
        
def read_data(filename, file_format='parquet'):
    import os.path
    filename_format = os.path.splitext(filename)[1][1:]
    file_format = file_format.replace('.', '')

    if not filename_format in ['', 'sql']:
        file_format=filename_format
    elif filename_format=='sql':
        filename = filename[:-4]
        filename = fr"{filename}.{file_format}"
    else:
        filename = fr"{filename}.{file_format}"

    filename = os.path.join(DATA_DIR, filename)
    return eval(f'pd.read_{file_format}(filename)')

