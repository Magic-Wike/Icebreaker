"""utility functions"""
import os
import datetime
import pandas as pd
import pyinputplus as pyip
from tqdm import tqdm



def clear_screen():
    """attempts to clear user screen. accounts for linux and windows OS"""
    try:
        os.system('clear')
        return
    except:
        pass
    try:
        os.system('cls')
        return
    except:
        pass

def print_response(response):
    print(f'{response.status_code}: {response.reason}')

def utc_to_iso(utc_time):
    """converts UTC timecode to time in iso format"""
    return datetime.datetime.utcfromtimestamp(utc_time/1000).isoformat()


def compare_csvs():
    """compares two CSVs to see differences. want to see how different Phantombuster CSVs are 
    (how many new leads -- is this a waste of processing power?)"""
    filepath1 = pyip.inputFilepath(prompt="Enter the first (original) CSV filepath\n")
    filepath2 = pyip.inputFilepath(prompt="Enter the second CSV filepath\n")

    df1 = pd.read_csv(filepath1)
    df2 = pd.read_csv(filepath2)

    csv1_len = df1.size
    csv2_len = df2.size

    csv1_column = df1["name"].tolist()
    print(f'First CSV has {csv1_len} entries')
    csv2_column = df2["name"].tolist()
    print(f'Second CSV has {csv2_len} entries')

    num_duplicates = 0
    duplicates = []
    for c1 in csv1_column:
        for c2 in csv2_column:
            if c1 == c2:
                num_duplicates += 1
                duplicates.append(c1)
    print(f'{num_duplicates} duplicate entries found!')
    num_good_leads = csv2_len-num_duplicates
    print(f'\nThere are {num_good_leads} new leads in CSV2')
    print_dupes = pyip.inputYesNo(prompt="Would you like to print the duplicate entires?\n$")
    if print_dupes == 'yes':
        print(duplicates)
    else:
        print('Done!')
    return num_duplicates

def generate_backup_csv(results, TAG):
    """creates backup CSVs for Hunter results. Does not specify location."""
    upload_data = []
    for result in tqdm(results):
        if result.account:
            d = result.dict(exclude={'account'})
            d['account_name'] = result.account.name
            d['address'] = result.account.address
            d['category'] = result.account.category
            d['owner_email'] = result.account.owner.email
            d['owner_first_name'] = result.account.owner.first_name
            d['lead_tag'] = TAG
            upload_data.append(d)
    return upload_data
    
# TIMESTAMP FUNCTIONS

# Determine the appropriate file path based on the operating system
DEFAULT_KEYWORD = 'default'

if os.name == 'nt':  # Windows
    import winreg
    key = winreg.CreateKey(winreg.HKEY_CURRENT_USER, 'Software\\MyApp')
    file_path = None  # not used on Windows
else:  # Linux
    file_path = '/var/tmp/timestamp.txt'

def get_timestamp():
    """Returns the current timestamp in ISO 8601 format"""
    return datetime.datetime.now().isoformat()

def store_timestamp(keyword=DEFAULT_KEYWORD):
    """Stores the given timestamp using an appropriate mechanism for the current OS"""
    timestamp = get_timestamp()
    timestamps = load_timestamp()
    timestamps[keyword] = timestamp
    if os.name == 'nt':  # Windows
        winreg.SetValueEx(key, 'Timestamp', 0, winreg.REG_SZ, str(timestamps))
    elif file_path is not None:
        with open(file_path, 'w') as f:
            for key, value in timestamps.items():
                f.write(f"{key}:{value}\n")
    else:
        raise NotImplementedError(f"Unsupported OS: {os.name}")

def load_timestamp():
    """Loads the stored timestamps using an appropriate mechanism for the current OS"""
    if os.name == 'nt':  # Windows
        try:
            value, _ = winreg.QueryValueEx(key, 'Timestamp')
            return ast.literal_eval(value)
        except OSError:
            return {}
    elif file_path is not None:
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
                timestamps = {}
                for line in lines:
                    keyword, timestamp = line.strip().split(':',maxsplit=1)
                    timestamps[keyword] = timestamp
                return timestamps
        except FileNotFoundError:
            return {}
    else:
        raise NotImplementedError(f"Unsupported OS: {os.name}")
    
def clear_timestamps():
    """Clears the stored keywords and timestamps using an appropriate mechanism for the current OS"""
    global timestamps
    timestamps = {}
    if os.name == 'nt':  # Windows
        try:
            winreg.DeleteKeyEx(winreg.HKEY_CURRENT_USER, 'Software\\MyApp')
        except OSError:
            pass
    elif file_path is not None:
        try:
            os.remove(file_path)
        except FileNotFoundError:
            pass
    else:
        raise NotImplementedError(f"Unsupported OS: {os.name}")

# Example usage
# store_timestamp("test")
# loaded_timestamp = load_timestamp()

# # # print("Stored timestamp:", timestamp)
# print("Loaded timestamp:", loaded_timestamp)



