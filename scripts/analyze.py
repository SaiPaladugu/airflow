import os
import dask.dataframe as dd
import re
from dask.diagnostics import ProgressBar

print('analyze 12345')

def analyze_text_length(df, text_column):
    """
    Analyze the text length of a specific column in the dataset.

    Args:
        df (Dask DataFrame): The DataFrame containing the data.
        text_column (str): The name of the text column to analyze.

    Returns:
        dict: Dictionary containing min, max, and average text lengths.
    """
    # Calculate text lengths
    df['text_length'] = df[text_column].str.len()

    # Compute metrics
    with ProgressBar():
        min_length = df['text_length'].min().compute()
        max_length = df['text_length'].max().compute()
        avg_length = df['text_length'].mean().compute()

    print(f"\nText Length Analysis for column '{text_column}':")
    print(f"- Min text length: {min_length}")
    print(f"- Max text length: {max_length}")
    print(f"- Average text length: {avg_length:.2f}")

    return {'min_length': min_length, 'max_length': max_length, 'avg_length': avg_length}

def create_text_length_buckets(df, text_column, bucket_ranges):
    """
    Create buckets for text length based on specified ranges.

    Args:
        df (Dask DataFrame): The DataFrame containing the data.
        text_column (str): The name of the text column to analyze.
        bucket_ranges (list): List of bucket ranges, e.g., [(0, 50), (50, 100)].

    Returns:
        dict: Dictionary with bucket counts.
    """
    df['text_length'] = df[text_column].str.len()

    bucket_counts = {}
    for lower, upper in bucket_ranges:
        count = df[(df['text_length'] >= lower) & (df['text_length'] < upper)].shape[0].compute()
        bucket_counts[f"{lower}-{upper}"] = count
        print(f"Text length {lower}-{upper}: {count} rows")

    return bucket_counts

def most_common_words(df, text_column, num_words=10):
    """
    Find the most common words in a text column.

    Args:
        df (Dask DataFrame): The DataFrame containing the data.
        text_column (str): The name of the text column to analyze.
        num_words (int): Number of top words to return.

    Returns:
        pd.Series: Series containing the top words and their counts.
    """
    # Tokenize text and explode into individual words
    df['words'] = df[text_column].str.lower().str.findall(r'\b\w+\b')
    words = df['words'].explode()

    # Compute word counts
    with ProgressBar():
        word_counts = words.value_counts().compute()
    top_words = word_counts.head(num_words)

    print(f"\nTop {num_words} most common words in '{text_column}':")
    for word, count in top_words.items():
        print(f"- {word}: {count} occurrences")

    return top_words

def analyze_subreddit_distribution(df):
    """
    Analyze the distribution of posts across subreddits.

    Args:
        df (Dask DataFrame): The DataFrame containing the data.

    Returns:
        pd.Series: Series containing subreddit counts.
    """
    with ProgressBar():
        subreddit_counts = df['subreddit'].value_counts().compute()

    print("\nSubreddit Distribution (Top 10):")
    print(subreddit_counts.head(10))

    return subreddit_counts

def analyze_average_word_length(df, text_column):
    """
    Analyze the average word length in the text column.

    Args:
        df (Dask DataFrame): The DataFrame containing the data.
        text_column (str): The text column to analyze.

    Returns:
        float: Average word length.
    """
    # Fill NaN values in the text column with empty strings
    df[text_column] = df[text_column].fillna('')

    # Tokenize text into words
    df['words'] = df[text_column].str.findall(r'\b\w+\b')

    # Compute word lengths, handling potential non-list entries
    df['word_lengths'] = df['words'].apply(
        lambda x: [len(word) for word in x] if isinstance(x, list) else [],
        meta=('word_lengths', 'object')
    )

    # Calculate average word length per row
    df['avg_word_length'] = df['word_lengths'].apply(
        lambda x: sum(x) / len(x) if x else 0,
        meta=('avg_word_length', 'float64')
    )

    # Compute the overall average word length
    with ProgressBar():
        average_word_length = df['avg_word_length'].mean().compute()

    print(f"\nAverage word length in '{text_column}': {average_word_length:.2f} characters")

    return average_word_length

def analyze_data(input_csv, text_column):
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

    # Load the dataset using Dask
    df = dd.read_csv(
        input_csv,
        on_bad_lines='skip',
        dtype={'title': 'object', 'body': 'object', 'subreddit': 'object'},
        engine='python',
        sep=',',
        quoting=0,
        encoding='utf-8',
        assume_missing=True,  # Treat all unspecified fields as nullable
    )

    # Analyze text length
    analyze_text_length(df, text_column)

    # Create buckets based on text length
    bucket_ranges = [(0, 50), (50, 100), (100, 200), (200, 500), (500, 1000)]
    create_text_length_buckets(df, text_column, bucket_ranges)

    # Analyze most common words
    most_common_words(df, text_column, num_words=10)

    # Analyze subreddit distribution
    analyze_subreddit_distribution(df)

    # Analyze average word length
    analyze_average_word_length(df, text_column)

# Main execution
if __name__ == "__main__":
    input_csv_path = '/opt/airflow/data/reddit_dataset_2011.csv'
    analyze_data(input_csv_path, 'title')
