import os
import pandas as pd
import re

print('preprocess 12345')

# Utility function to check if a file exists
def file_exists(file_path: str):
    if not os.path.exists(file_path):
        print(f"Error: {file_path} does not exist. Please check the file path.")
        return False
    return True

# Remove rows with null values
def remove_null_rows(df: pd.DataFrame):
    null_rows_before = df.isnull().sum().sum()  # Total null values
    df.dropna(inplace=True)
    null_rows_after = df.isnull().sum().sum()
    print(f"{null_rows_before - null_rows_after} null rows removed")
    return df

# Remove rows with illegal (non-ASCII) characters
def remove_illegal_chars(df: pd.DataFrame):
    def clean_text(text):
        return re.sub(r'[^\x00-\x7F]+', '', text) if isinstance(text, str) else text

    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        df[col] = df[col].apply(clean_text)
    print(f"Illegal characters removed from columns: {', '.join(text_columns)}")
    return df

# Remove emojis from text columns
def remove_emojis(df: pd.DataFrame):
    def clean_emoji(text):
        emoji_pattern = re.compile(
            "["
            "\U0001F600-\U0001F64F"  # emoticons
            "\U0001F300-\U0001F5FF"  # symbols & pictographs
            "\U0001F680-\U0001F6FF"  # transport & map symbols
            "\U0001F1E0-\U0001F1FF"  # flags (iOS)
            "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r'', text) if isinstance(text, str) else text

    text_columns = df.select_dtypes(include=['object']).columns
    for col in text_columns:
        df[col] = df[col].apply(clean_emoji)
    print(f"Emojis removed from columns: {', '.join(text_columns)}")
    return df

# Strip leading and trailing whitespace from all string columns
def strip_whitespace(df: pd.DataFrame):
    text_columns = df.select_dtypes(include=['object']).columns
    df[text_columns] = df[text_columns].apply(lambda x: x.str.strip())
    print(f"Whitespace removed from columns: {', '.join(text_columns)}")
    return df

# Drop duplicate rows
def remove_duplicates(df: pd.DataFrame):
    duplicates_before = df.duplicated().sum()
    df.drop_duplicates(inplace=True)
    duplicates_after = df.duplicated().sum()
    print(f"{duplicates_before - duplicates_after} duplicate rows removed")
    return df

# Convert numeric-like strings to numeric types
def convert_numeric(df: pd.DataFrame):
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = pd.to_numeric(df[col], errors='ignore')
    print(f"Converted string columns to numeric where applicable.")
    return df

# Main preprocessing function that applies all cleaning steps and updates the same CSV
def preprocess_data(input_csv: str):
    # Check if the input CSV file exists
    if not file_exists(input_csv):
        return

    # Load the dataset
    df = pd.read_csv(input_csv)
    print(f"Initial dataset shape: {df.shape}")

    # Apply cleaning functions
    df = remove_null_rows(df)
    df = remove_illegal_chars(df)
    df = remove_emojis(df)
    df = strip_whitespace(df)
    df = remove_duplicates(df)
    df = convert_numeric(df)

    # Final shape of the cleaned dataset
    print(f"Final dataset shape: {df.shape}")
    
    # Save the cleaned dataset back to the original CSV
    df.to_csv(input_csv, index=False)
    print(f"Cleaned dataset updated in {input_csv}")

# Main execution
if __name__ == "__main__":
    input_csv_path = '/opt/airflow/data/reddit_dataset_2011.csv'

    preprocess_data(input_csv_path)
