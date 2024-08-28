import argparse
import subprocess
import os

def ensure_directory_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created directory: {path}")

def submit_pbs_job(script_path, total_devices, device_index, log_dir):
    job_name = f"download_device_{device_index}"
    log_file = os.path.join(log_dir, f"{job_name}.log")
    
    try:
        result = subprocess.run([
            'qsub',
            '-N', job_name,
            '-o', log_file,
            '-e', log_file,
            '-v', f'total_device={total_devices},device_index={device_index}',
            script_path
        ], check=True, capture_output=True, text=True)
        
        job_id = result.stdout.strip()
        print(f"Submitted job {job_name} with ID: {job_id}")
        print(f"Log will be written to: {log_file}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to submit job {job_name}. Error: {e}")

def main():
    parser = argparse.ArgumentParser(description="Schedule PBS jobs for multi-device download")
    parser.add_argument("total_devices", type=int, help="Total number of devices to use")
    parser.add_argument("--log_dir", default="/home/project/13003826/geyu/youtube_local_raw/log", help="Directory for job logs")
    args = parser.parse_args()

    script_path = "./local/fetch_with_json_wrap.sh"
    ensure_directory_exists(args.log_dir)

    for device_index in range(args.total_devices):
        submit_pbs_job(script_path, args.total_devices, device_index, args.log_dir)

if __name__ == "__main__":
    main()