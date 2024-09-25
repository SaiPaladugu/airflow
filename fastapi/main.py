from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import os

app = FastAPI()

# Get the output directory from environment variables
output_dir = os.getenv("DATA_DIR", "./data")
os.makedirs(output_dir, exist_ok=True)

def tokenize_text(df: pd.DataFrame, text_column: str):
    df[f'{text_column}_tokens'] = df[text_column].apply(lambda x: str(x).split() if isinstance(x, str) else [])
    return df

def remove_stopwords(df: pd.DataFrame, text_column: str):
    stop_words = set([
        'a', 'an', 'the', 'in', 'on', 'at', 'is', 'it', 'this', 'that',  
        'he', 'she', 'they', 'we', 'you', 'and', 'or', 'but'
    ])
    df[f'{text_column}_clean_tokens'] = df[f'{text_column}_tokens'].apply(lambda tokens: [word for word in tokens if word.lower() not in stop_words])
    return df

def transform_data(df: pd.DataFrame, text_column: str):
    df = tokenize_text(df, text_column)
    df = remove_stopwords(df, text_column)
    return df

# @app.post("/upload/")
# async def upload_file(file: UploadFile = File(...), text_column: str = "title"):
#     df = pd.read_csv(file.file)
#     df_transformed = transform_data(df, text_column)
#     output_csv_path = os.path.join(output_dir, f"transformed_{file.filename}")
#     df_transformed.to_csv(output_csv_path, index=False)
#     return {"message": f"File processed successfully. Download the transformed file at /download/{file.filename}"}

@app.get("/download/{filename}")
async def download_file(filename: str):
    file_path = os.path.join(output_dir, f"transformed_{filename}")
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=f"transformed_{filename}", media_type="text/csv")
    return {"error": "File not found."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
