import os
import pandas as pd
import json
import gzip
from huggingface_hub import hf_hub_download

print('ingest 12345')

# Chosen Dataset & File
repo_id = "sentence-transformers/reddit-title-body"
filename = "reddit_title_text_2011.jsonl.gz"

# Absolute paths for the data directory
compressed_file_path = '/opt/airflow/data/reddit_title_text_2011.jsonl.gz'
extracted_file_path = '/opt/airflow/data/reddit_title_text_2011.jsonl'
output_csv_path = '/opt/airflow/data/reddit_dataset_2011.csv'
sample_csv_path = '/opt/airflow/data/sample.csv'


def download_file_from_hub(repo_id: str, filename: str, output_path: str):
    """
    Download a file from the Hugging Face Hub using hf_hub_download and save it locally.

    Args:
        repo_id (str): The repository ID from the Hugging Face Hub.
        filename (str): The file to download.
        output_path (str): The path to save the downloaded file locally.
    """
    print(f"Downloading {filename} from {repo_id}...")
    
    # Download the file from Hugging Face Hub
    file_path = hf_hub_download(repo_id=repo_id, filename=filename, repo_type="dataset")
    
    # Copy the downloaded file to the output path
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(file_path, 'rb') as src, open(output_path, 'wb') as dst:
        dst.write(src.read())
    
    print(f"File downloaded and saved to {output_path}")
    return output_path

def extract_gzip(input_path: str, output_path: str):
    """
    Extract a .gz compressed file.

    Args:
        input_path (str): Path to the compressed file.
        output_path (str): Path to save the extracted file.
    """
    print(f"Extracting {input_path}...")
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            f_out.write(f_in.read())
    print(f"File extracted to {output_path}")

def ingest_data(jsonl_path: str, output_csv_path: str):
    """
    Ingest a .jsonl file, convert it into a DataFrame, and save it locally as a CSV file.

    Args:
        jsonl_path (str): The path to the JSONL file to process.
        output_csv_path (str): The path to save the resulting CSV file.
    """
    print(f"Reading and converting {jsonl_path} to DataFrame...")
    data = []

    # Read the JSONL file line by line
    with open(jsonl_path, 'r') as file:
        for line in file:
            data.append(json.loads(line))

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(data)

    # Save DataFrame to CSV
    df.to_csv(output_csv_path, index=False)
    print(f"Data saved to {output_csv_path}")

def clean_up(files: list):
    """
    Remove specified files.

    Args:
        files (list): A list of file paths to remove.
    """
    for file in files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Removed file: {file}")

def create_sample_csv(original_csv_path: str, sample_csv_path: str, num_lines: int = 100):
    """
    Create a sample CSV with the first 'num_lines' lines from the original CSV.

    Args:
        original_csv_path (str): The path to the original CSV file.
        sample_csv_path (str): The path to save the sample CSV file.
        num_lines (int): The number of lines to extract for the sample.
    """
    print(f"Creating a sample CSV with the first {num_lines} lines from {original_csv_path}...")
    
    # Read the first 'num_lines' lines from the original CSV
    df = pd.read_csv(original_csv_path, nrows=num_lines)
    
    # Save the sample to a new CSV
    df.to_csv(sample_csv_path, index=False)
    
    print(f"Sample CSV saved to {sample_csv_path}")

# Main execution
if __name__ == "__main__":
    # Step 1: Download the file from the Hugging Face Hub
    download_file_from_hub(repo_id, filename, compressed_file_path)

    # Step 2: Extract the .jsonl.gz file
    extract_gzip(compressed_file_path, extracted_file_path)

    # Step 3: Ingest and process the .jsonl file
    ingest_data(extracted_file_path, output_csv_path)

    # Step 4: Clean up temporary files (.gz and .jsonl)
    clean_up([compressed_file_path, extracted_file_path])

    # Step 5: Create a sample CSV with the first 100 lines
    create_sample_csv(output_csv_path, sample_csv_path)
    
    if os.path.exists(output_csv_path):
        print(f"File {output_csv_path} exists after saving.")
    else:
        print(f"File {output_csv_path} does NOT exist after saving.")

