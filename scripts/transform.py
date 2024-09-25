import os
import pandas as pd
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords

# Download required NLTK resources
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('stopwords')

def tokenize_text(df: pd.DataFrame, text_column: str):
    df[f'{text_column}_tokens'] = df[text_column].apply(lambda x: word_tokenize(str(x)) if isinstance(x, str) else [])
    print(f"Tokenization complete for column: {text_column}")
    return df

def remove_stopwords(df: pd.DataFrame, text_column: str):
    stop_words = set(stopwords.words('english'))
    df[f'{text_column}_clean_tokens'] = df[f'{text_column}_tokens'].apply(lambda tokens: [word for word in tokens if word.lower() not in stop_words])
    print(f"Stopwords removed from column: {text_column}")
    return df

def transform_data(input_csv: str, output_csv: str, text_column: str):
    if not os.path.exists(input_csv):
        print(f"Error: {input_csv} does not exist.")
        return

    df = pd.read_csv(input_csv)
    df = tokenize_text(df, text_column)
    df = remove_stopwords(df, text_column)
    df.to_csv(output_csv, index=False)
    print(f"Transformed dataset saved to {output_csv}")

if __name__ == "__main__":
    input_csv_path = '../data/reddit_dataset_2011_clean.csv'
    output_csv_path = '../data/reddit_dataset_2011_transformed.csv'
    transform_data(input_csv_path, output_csv_path, 'title')
