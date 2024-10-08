import os
import subprocess
import time
import json
import threading
import requests
import sqlite3
from fuzzywuzzy import fuzz
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import bencodepy
import hashlib

# Global flags for progress reporting
parsing_in_progress = False

# Log function
def log(message, torrent_name=None, is_error=False):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        # Simulating some logic that could raise an error
        print("Attempting to load settings...")
        # Uncomment the next line to simulate an error
        # raise FileNotFoundError("Simulating FileNotFoundError for testing.")
        with open('settings.json', 'r') as f:
            settings = json.load(f)
        return settings

    except FileNotFoundError as e:
        print(f"Error: {e}")
        # Return default settings or an empty dictionary
        return {}

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return {}

    except Exception as e:
        print(f"Some other error occurred: {e}")
        return {}

    finally:
        print("This will always run, even if an error occurred.")

# Initialize the SQLite database and create a table for torrents
def initialize_database(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS torrents (
            hash TEXT PRIMARY KEY,
            torname TEXT,
            status INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    return conn

# Insert or update torrent data into the database
def insert_or_update_torrent(conn, torrent):
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM torrents WHERE hash = ?', (torrent['hash'],))
    exists = cursor.fetchone()
    if not exists:
        cursor.execute('''
            INSERT INTO torrents (hash, torname, status)
            VALUES (?, ?, ?)
        ''', (torrent['hash'], torrent['torname'], torrent['status']))
        conn.commit()
        return True  # New torrent added
    else:
        cursor.execute('''
            UPDATE torrents SET torname = ?, status = ?
            WHERE hash = ?
        ''', (torrent['torname'], torrent['status'], torrent['hash']))
        conn.commit()
        return False  # Torrent already existed

# Update the status of a torrent in the database
def update_torrent_status(conn, torrent_hash, status):
    cursor = conn.cursor()
    cursor.execute('UPDATE torrents SET status = ? WHERE hash = ?', (status, torrent_hash))
    conn.commit()

# Query all torrents from the database
def fetch_all_torrents(conn):
    cursor = conn.cursor()
    cursor.execute('SELECT hash, torname, status FROM torrents')
    return cursor.fetchall()

# Periodically log the number of files parsed so far
def periodic_parse_log(parsed_files):
    while parsing_in_progress:
        log(f"Files parsed so far: {len(parsed_files)}")
        time.sleep(30)  # Log every 30 seconds

# Parse .zurginfo and .zurgtorrent files recursively and insert into the database
def parse_torrent_files_recursively(zurginfo_dir, conn):
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM torrents')
    before_count = cursor.fetchone()[0]
    log(f"Number of torrents in the database before parsing: {before_count}")

    parsed_files = []
    new_torrents_count = 0

    global parsing_in_progress
    parsing_in_progress = True

    # Start background thread for periodic progress logging
    logger_thread = threading.Thread(target=periodic_parse_log, args=(parsed_files,))
    logger_thread.start()

    try:
        # Start parsing files in the directory
        for root, dirs, files in os.walk(zurginfo_dir):  # Recursively walk through subdirectories
            for file_name in files:
                file_path = os.path.join(root, file_name)

                try:
                    # Parse .zurginfo files
                    if file_name.endswith('.zurginfo'):
                        log(f"Detected .zurginfo file: {file_name}")
                        with open(file_path, 'r') as file:
                            data = json.load(file)
                            torrent = {
                                'hash': data.get('hash'),
                                'status': 0,
                                'torname': data.get('filename')
                            }
                            is_new_torrent = insert_or_update_torrent(conn, torrent)
                            if is_new_torrent:
                                new_torrents_count += 1
                            log(f"Parsed and added .zurginfo file: {file_name}")
                            parsed_files.append(file_name)

                    # Parse .zurgtorrent files
                    elif file_name.endswith('.zurgtorrent'):
                        log(f"Detected .zurgtorrent file: {file_name}")
                        with open(file_path, 'rb') as file:
                            try:
                                data = bencodepy.decode(file.read())
                                # Compute the infohash
                                if b'info' in data:
                                    info = data[b'info']
                                    infohash = hashlib.sha1(bencodepy.encode(info)).hexdigest().upper()
                                    # Get the torrent name
                                    if b'name' in info:
                                        torname = info[b'name']
                                        if isinstance(torname, bytes):
                                            torname = torname.decode('utf-8', 'ignore')
                                        else:
                                            torname = str(torname)
                                        torrent = {
                                            'hash': infohash,
                                            'status': 0,
                                            'torname': torname
                                        }
                                        is_new_torrent = insert_or_update_torrent(conn, torrent)
                                        if is_new_torrent:
                                            new_torrents_count += 1
                                        log(f"Parsed and added .zurgtorrent file: {file_name}")
                                        parsed_files.append(file_name)
                                    else:
                                        log(f"Missing 'name' in 'info' dictionary in .zurgtorrent file: {file_name}", is_error=True)
                                else:
                                    log(f"Missing 'info' in .zurgtorrent file: {file_name}", is_error=True)
                            except Exception as e:
                                log(f"Error decoding .zurgtorrent file: {file_name}. Error: {str(e)}", is_error=True)
                except Exception as e:
                    log(f"An error occurred while parsing file '{file_name}': {str(e)}", is_error=True)
    except Exception as e:
        log(f"An error occurred while parsing torrents: {str(e)}", is_error=True)
    finally:
        parsing_in_progress = False
        logger_thread.join()  # Ensure the logging thread ends before continuing

    cursor.execute('SELECT COUNT(*) FROM torrents')
    after_count = cursor.fetchone()[0]
    log(f"Finished parsing torrents. Total parsed files: {len(parsed_files)}")
    log(f"Number of torrents in the database after parsing: {after_count}")
    log(f"Number of new torrents added: {new_torrents_count}")

# List files with rclone recursively and show progress
def list_rclone_files(remote_path):
    log(f"Listing files with rclone from: {remote_path}")
    file_list = []

    try:
        process = subprocess.Popen(
            ['rclone', 'lsf', remote_path, '--fast-list', '--recursive'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        for line in process.stdout:
            file_list.append(line.strip())

        return_code = process.wait()

        if return_code != 0:
            error_output = process.stderr.read()
            log(f"Error listing files: {error_output}", is_error=True)
        else:
            log(f"Fetched {len(file_list)} files/folders from rclone.")

    except Exception as e:
        log(f"Error executing rclone command: {str(e)}", is_error=True)

    return file_list

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
def process_torrents(api_key, mounted_path, zurginfo_dir, db_file, timeout, match_threshold, api_delay):
    log("Starting torrent processing workflow.")

    # Step 1: Initialize the database
    conn = initialize_database(db_file)

    # Step 2: Parse all .zurginfo and .zurgtorrent files recursively and store in the DB
    parse_torrent_files_recursively(zurginfo_dir, conn)

    # Step 3: List all files/folders using rclone recursively
    file_list = list_rclone_files(mounted_path)

    # Step 4: Match each torrent in the database against the files/folders
    torrents = fetch_all_torrents(conn)
    for torrent_hash, torrent_name, status in torrents:
        if status == 1:  # Skip already processed torrents
            continue

        if match_in_parallel(file_list, torrent_name, match_threshold):
            log(f"Torrent '{torrent_name}' already exists in mounted path. Skipping.")
            update_torrent_status(conn, torrent_hash, 1)  # Mark as processed
            continue

        log(f"No match found for '{torrent_name}'. Adding to Real-Debrid.")
        torrent_id = add_torrent_to_rd(api_key, torrent_hash, torrent_name, timeout)

        if torrent_id:
            log(f"Successfully added and processed torrent '{torrent_name}'.")
            update_torrent_status(conn, torrent_hash, 1)  # Mark as processed
        else:
            log(f"Failed to process torrent '{torrent_name}'.", is_error=True)

        log(f"Waiting for {api_delay} seconds before the next Real-Debrid API call.")
        time.sleep(api_delay)

    conn.close()

if __name__ == '__main__':
    try:
        settings = load_settings()

        if not settings:
            raise ValueError("Settings could not be loaded.")

        api_key = settings.get('REAL_DEBRID_API_KEY')
        mounted_path = settings.get('MOUNTED_PATH')
        zurginfo_dir = settings.get('ZURGINFOS_DIR')
        db_file = settings.get('DB_FILE')
        execution_cycle = settings.get('EXECUTION_CYCLE', 86400)  # Default to 24 hours
        timeout = settings.get('REAL_DEBRID_TIMEOUT', 30)  # Default to 30 seconds
        match_threshold = settings.get('MATCH_THRESHOLD', 85)  # Default fuzzy match threshold
        api_delay = settings.get('REAL_DEBRID_API_DELAY', 10)  # Delay between API calls

        process_torrents(api_key, mounted_path, zurginfo_dir, db_file, timeout, match_threshold, api_delay)

    except (KeyError, FileNotFoundError, json.JSONDecodeError, AttributeError, ValueError) as e:
        log(f"Critical error occurred: {str(e)}", is_error=True)
