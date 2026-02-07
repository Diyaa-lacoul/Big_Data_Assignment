import pandas as pd
import numpy as np
import random

def reduce_dataset(data, sample_size=1000):
    """
    Reduces the dataset by sampling a specified number of rows.
    
    Parameters:
    data (pd.DataFrame): The input dataset.
    sample_size (int): The number of rows to sample.
    
    Returns:
    pd.DataFrame: The reduced dataset.
    """
    # Sample a specified number of rows
    sampled = data.sample(n=sample_size, random_state=42)
    
    # Save reduced dataset as a DataFrame variable for further use
    return sampled

if __name__ == "__main__":
    reduced_df = reduce_dataset()
    if reduced_df is not None:
        print("Reduced dataset DataFrame is available as 'reduced_df'.")