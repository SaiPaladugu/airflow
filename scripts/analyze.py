import os
import pandas as pd

def analyze_text_length(df: pd.DataFrame, text_column: str):
    """
    Analyze the text length of a specific column in the dataset.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        text_column (str): The name of the text column to analyze.

    Returns:
        dict: Dictionary containing min, max, and average text lengths.
    """
    # Calculate text lengths
    df['text_length'] = df[text_column].apply(lambda x: len(str(x)) if isinstance(x, str) else 0)

    min_length = df['text_length'].min()
    max_length = df['text_length'].max()
    avg_length = df['text_length'].mean()

    print(f"Min text length: {min_length}")
    print(f"Max text length: {max_length}")
    print(f"Average text length: {avg_length}")

    return {'min_length': min_length, 'max_length': max_length, 'avg_length': avg_length}

def create_text_length_buckets(df: pd.DataFrame, text_column: str, bucket_ranges: list):
    """
    Create buckets for text length based on specified ranges.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        text_column (str): The name of the text column to analyze.
        bucket_ranges (list): List of bucket ranges, e.g., [(0, 10), (10, 20)].

    Returns:
        pd.DataFrame: DataFrame with bucket counts.
    """
    df['text_length'] = df[text_column].apply(lambda x: len(str(x)) if isinstance(x, str) else 0)

    bucket_counts = {}
    for lower, upper in bucket_ranges:
        count = df[(df['text_length'] >= lower) & (df['text_length'] < upper)].shape[0]
        bucket_counts[f"{lower}-{upper}"] = count
        print(f"Text length {lower}-{upper}: {count} rows")

    return bucket_counts

def analyze_data(input_csv: str, text_column: str):
    """
    Perform various analysis on the dataset.

    Args:
        input_csv (str): Path to the input CSV file.
        text_column (str): The text column to analyze.
    """
    # Check if the input CSV file exists
    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} does not exist. Please check the file path.")
        return

    # Load the dataset
    df = pd.read_csv(input_csv)

    # Analyze text length
    analyze_text_length(df, text_column)

    # Create buckets based on text length
    bucket_ranges = [(0, 10), (10, 20), (20, 30), (30, 40), (40, 50)]
    create_text_length_buckets(df, text_column, bucket_ranges)

# Main execution
if __name__ == "__main__":
    input_csv_path = '../data/reddit_dataset_2011_clean.csv'
    analyze_data(input_csv_path, 'title')
