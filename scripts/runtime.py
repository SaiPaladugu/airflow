import time
import psutil
import os

def profile_resource_usage(func):
    """
    Decorator to profile CPU, memory, and disk usage for a function.

    Args:
        func (callable): The function to profile.

    Returns:
        callable: A wrapped function that profiles resource usage.
    """
    def wrapper(*args, **kwargs):
        # Get system usage before execution
        cpu_start = psutil.cpu_percent(interval=None)
        memory_start = psutil.virtual_memory().used / (1024 ** 3)  # Convert bytes to GB
        disk_start = psutil.disk_usage('/').used / (1024 ** 3)     # Convert bytes to GB

        # Start the timer
        start_time = time.time()

        # Execute the function
        result = func(*args, **kwargs)

        # End the timer
        end_time = time.time()

        # Get system usage after execution
        cpu_end = psutil.cpu_percent(interval=None)
        memory_end = psutil.virtual_memory().used / (1024 ** 3)
        disk_end = psutil.disk_usage('/').used / (1024 ** 3)

        print(f"Execution time: {end_time - start_time:.2f} seconds")
        print(f"CPU usage: {cpu_end - cpu_start:.2f}%")
        print(f"Memory used: {memory_end - memory_start:.2f} GB")
        print(f"Disk space used: {disk_end - disk_start:.2f} GB")

        return result

    return wrapper

@profile_resource_usage
def run_analysis(input_csv: str):
    from analyze import analyze_data
    analyze_data(input_csv, 'title')

@profile_resource_usage
def run_transformation(input_csv: str, output_csv: str):
    from transform import transform_data
    transform_data(input_csv, output_csv, 'title')

# Main execution
if __name__ == "__main__":
    input_csv_path = '../data/reddit_dataset_2011_clean.csv'
    output_csv_path = '../data/reddit_dataset_2011_transformed.csv'

    # Profile analysis
    run_analysis(input_csv_path)

    # Profile transformation
    run_transformation(input_csv_path, output_csv_path)
