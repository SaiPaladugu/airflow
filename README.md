run instructions:

first is the ingest dockerfile
docker build -t ingest .
docker run -v $(pwd)/data:/app/data ingest

