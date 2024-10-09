import subprocess
import sys
import time
import psutil
import threading

def monitor_process(proc, interval, stats):
    """
    Monitor the CPU and RAM usage of a subprocess.
    """
    process = psutil.Process(proc.pid)
    while proc.poll() is None:
        try:
            cpu = process.cpu_percent(interval=interval)
            mem = process.memory_info().rss / (1024 * 1024)  # Memory in MB
            stats['cpu'].append(cpu)
            stats['mem'].append(mem)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            break

def run_script(script, args):
    """
    Run a script and monitor its resource usage.
    """
    print(f"\nStarting script: {script} {' '.join(args)}")
    stats = {'cpu': [], 'mem': []}
    start_time = time.time()

    # Start the subprocess
    proc = subprocess.Popen([sys.executable, script] + args)

    # Start the monitoring thread
    monitor_thread = threading.Thread(target=monitor_process, args=(proc, 1, stats))
    monitor_thread.start()

    # Wait for the subprocess to finish
    proc.wait()
    monitor_thread.join()
    end_time = time.time()

    # Calculate statistics
    total_time = end_time - start_time
    avg_cpu = sum(stats['cpu']) / len(stats['cpu']) if stats['cpu'] else 0
    max_mem = max(stats['mem']) if stats['mem'] else 0

    print(f"Finished script: {script}")
    print(f"Execution time: {total_time:.2f} seconds")
    print(f"Average CPU usage: {avg_cpu:.2f}%")
    print(f"Maximum RAM usage: {max_mem:.2f} MB")

    return {'script': script, 'time': total_time, 'avg_cpu': avg_cpu, 'max_mem': max_mem}

def main():
    if len(sys.argv) < 2:
        print("Usage: python runtime.py <script1> [script1_args] -- <script2> [script2_args] -- ...")
        sys.exit(1)

    # Parse the scripts and their arguments
    scripts_and_args = []
    current_script = []
    for arg in sys.argv[1:]:
        if arg == '--':
            if current_script:
                scripts_and_args.append(current_script)
                current_script = []
        else:
            current_script.append(arg)
    if current_script:
        scripts_and_args.append(current_script)

    total_stats = []
    total_start_time = time.time()

    # Run each script sequentially
    for script_args in scripts_and_args:
        script = script_args[0]
        args = script_args[1:]
        stats = run_script(script, args)
        total_stats.append(stats)

    total_end_time = time.time()
    overall_time = total_end_time - total_start_time

    # Summary of all scripts
    print("\n===== Summary =====")
    for stat in total_stats:
        print(f"Script: {stat['script']}")
        print(f"  Execution time: {stat['time']:.2f} seconds")
        print(f"  Average CPU usage: {stat['avg_cpu']:.2f}%")
        print(f"  Maximum RAM usage: {stat['max_mem']:.2f} MB")
    print(f"\nTotal execution time for all scripts: {overall_time:.2f} seconds")

if __name__ == "__main__":
    main()
