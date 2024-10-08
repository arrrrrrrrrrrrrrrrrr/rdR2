import os
import subprocess
import time
import json
import threading
import requests  # Import requests library
from fuzzywuzzy import fuzz
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Global flag to control the background logger thread
listing_in_progress = False

# Log function
def log(message, torrent_name=None, is_error=False):
    timestamp = datetime.now().strftime("%Y-%m-%D %H:%M:%S")
    if torrent_name:
        message = f"[{torrent_name}] {message}"
    if is_error:
        message = f"\033[31m{message}\033[0m"  # Bold red for errors
    print(f"{timestamp} - {message}")

    # Write to a log file
    with open('process_log.txt', 'a') as log_file:
        log_file.write(f"{timestamp} - {message}\n")

# Load settings from the JSON config file
def load_settings():
    try:
        with open('settings.json', 'r') as config_file:
            settings = json.load(config_file)

        required_keys = ['REAL_DEBRID_API_KEY', 'MOUNTED_PATH', 'ZURGINFOS_DIR']
        for key in required_keys:
            if key not in settings:
                raise KeyError(f"Missing required setting: {key}")

        return settings

    except FileNotFoundError:
        log("Error: settings.json not found", is_error=True)
        raise
    except KeyError as e:
        log(f"Error: {str(e)}", is_error=True)
        raise
    except json.JSONDecodeError as e:
        log(f"Error decoding JSON from settings.json: {str(e)}", is_error=True)
        raise

# Periodically log the number of files listed so far
def periodic_log(file_list):
    while listing_in_progress:
        log(f"Files listed so far: {len(file_list)}")
        time.sleep(10)  # Log every 10 seconds

# List files with rclone recursively and show progress
def list_rclone_files(remote_path):
    log(f"Listing files with rclone from: {remote_path}")
    file_list = []

    global listing_in_progress
    listing_in_progress = True

    # Start the background thread for periodic logging
    logger_thread = threading.Thread(target=periodic_log, args=(file_list,))
    logger_thread.start()

    try:
        # Use subprocess to run the rclone command and list files progressively
        process = subprocess.Popen(
            ['rclone', 'lsf', remote_path, '--fast-list', '--recursive'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        for line in process.stdout:
            file_list.append(line.strip())

        process.stdout.close()
        return_code = process.wait()

        if return_code != 0:
            error_output = process.stderr.read()
            log(f"Error listing files: {error_output}", is_error=True)
        else:
            log(f"Fetched {len(file_list)} files/folders from rclone.")

    except Exception as e:
        log(f"Error executing rclone command: {str(e)}", is_error=True)

    finally:
        listing_in_progress = False
        logger_thread.join()  # Ensure thread ends before continuing

    return file_list

# Parse .zurginfo files
def parse_zurginfo_files(zurginfo_dir):
    log(f"Parsing .zurginfo files from directory: {zurginfo_dir}")
    torrents = []
    zurginfo_files = os.listdir(zurginfo_dir)

    for zurginfo_file in zurginfo_files:
        if zurginfo_file.endswith('.zurginfo'):
            zurginfo_path = os.path.join(zurginfo_dir, zurginfo_file)
            with open(zurginfo_path, 'r') as file:
                data = json.load(file)
                torrents.append({
                    'hash': data.get('hash'),
                    'status': 0,
                    'torname': data.get('filename')
                })
    log(f"Parsed {len(torrents)} torrents from .zurginfo files.")
    return torrents

# Fuzzy matching with parallel processing
def match_in_parallel(file_list, torrent_name, match_threshold=85):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fuzz.ratio, torrent_name.lower(), file_name.lower()) for file_name in file_list]
        for future in futures:
            match_ratio = future.result()
            if match_ratio >= match_threshold:
                return True  # Match found
    return False

# Add torrent to Real-Debrid
def add_torrent_to_rd(api_key, magnet_hash, torrent_name, timeout):
    log(f"Adding torrent '{torrent_name}' with hash '{magnet_hash}' to Real-Debrid.")
    headers = {'Authorization': f'Bearer {api_key}'}
    data = {'magnet': f'magnet:?xt=urn:btih:{magnet_hash}'}
    url = 'https://api.real-debrid.com/rest/1.0/torrents/addMagnet'

    try:
        response = requests.post(url, headers=headers, data=data, timeout=timeout)
        if response.status_code == 201:
            torrent_info = response.json()
            torrent_id = torrent_info['id']
            log(f"Successfully added torrent '{torrent_name}' to Real-Debrid with ID: {torrent_id}")
            return torrent_id
        else:
            log(f"Error adding torrent '{torrent_name}': {response.text}", is_error=True)
            return None
    except requests.Timeout:
        log(f"Timeout while adding torrent '{torrent_name}' to Real-Debrid.", torrent_name, is_error=True)
        return None

# Main torrent processing function
def process_torrents(api_key, mounted_path, zurginfo_dir, timeout, match_threshold, api_delay):
    log("Starting torrent processing workflow.")

    # Step 1: List all files/folders using rclone recursively
    file_list = list_rclone_files(mounted_path)

    # Step 2: Parse all .zurginfo files
    torrents = parse_zurginfo_files(zurginfo_dir)

    # Step 3: Match each torrent against the files/folders
    for torrent in torrents:
        torrent_name = torrent['torname']
        magnet_hash = torrent['hash']

        # Match in parallel
        if match_in_parallel(file_list, torrent_name, match_threshold):
            log(f"Torrent '{torrent_name}' already exists in mounted path. Skipping.")
            continue

        # Add to Real-Debrid if no match found
        log(f"No match found for '{torrent_name}'. Adding to Real-Debrid.")
        torrent_id = add_torrent_to_rd(api_key, magnet_hash, torrent_name, timeout)

        if torrent_id:
            log(f"Successfully added and processed torrent '{torrent_name}'.")
        else:
            log(f"Failed to process torrent '{torrent_name}'.", is_error=True)

        log(f"Waiting for {api_delay} seconds before the next Real-Debrid API call.")
        time.sleep(api_delay)

if __name__ == '__main__':
    try:
        settings = load_settings()

        api_key = settings.get('REAL_DEBRID_API_KEY')
        mounted_path = settings.get('MOUNTED_PATH')
        zurginfo_dir = settings.get('ZURGINFOS_DIR')
        execution_cycle = settings.get('EXECUTION_CYCLE', 86400)  # Default to 24 hours
        timeout = settings.get('REAL_DEBRID_TIMEOUT', 30)  # Default to 30 seconds
        match_threshold = settings.get('MATCH_THRESHOLD', 85)  # Default fuzzy match threshold
        api_delay = settings.get('REAL_DEBRID_API_DELAY', 10)  # Delay between API calls

        process_torrents(api_key, mounted_path, zurginfo_dir, timeout, match_threshold, api_delay)

    except (KeyError, FileNotFoundError, json.JSONDecodeError) as e:
        log(f"Critical error occurred: {str(e)}", is_error=True)
