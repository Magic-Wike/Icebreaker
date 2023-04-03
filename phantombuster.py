"""
Contains all API calls to Phantombuster for creating, editing and accessing Phantoms

Phantombuster Terminology Note: User = account/user/ugp, Agent = phantoms/crawls/searches, Container = individual scrapes w/i Phantom
"""

# IMPORTS:
from pydantic import BaseModel
from tqdm import tqdm
import pandas as pd
import pyinputplus as pyip
import datetime
import json
import requests
import utils
import config
from google_api import get_csv_for_phantombuster

# Getting API Key from config.py
API = config.PHANTOMBUSTER_API

class Phantom(BaseModel):
    """Creating Phantom class to store all Phantoms and related data on account"""
    phantom_id: int
    name: str
    is_running: bool
    script: str or None
    argument: object or None
    last_updated: datetime.datetime or None
    launch_type = str or None
    containers = []

    def fetch_containers(self):
    
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-all" 
        headers = {
            "accept": "application/json",
            "X-Phantombuster-Key": API,
            }
        payload = {"agentId":self.phantom_id,"mode":"finalized"} 
        try: 
            response = requests.get(url, headers=headers, json=payload, timeout=30)
            utils.print_response(response)
            data = response.json()
            containers = data['containers']
            print(len(containers))
            print(containers)
            for c in containers:
                container_id = c['id']
                status = c['status']
                end_time = utils.utc_to_iso(c['endedAt'])
        except Exception as e:
            print(e)
class PhantomBusterAccount(BaseModel):
    """Creating Account class for UGP Phantombuster account info.. Mostly for checking usage"""
    email: str
    time_left: int
    phantoms = []
    open_slots: int

    def __str__(self):
        return f'\nPhantombuster Account: {self.email}\nUsage time remaining: {self.time_left}'
    def print_all_phantoms(self): # Returns printed list of Phantoms + info about each
        for p in self.phantoms:
            print(f'\n{p.name} ({p.phantom_id})\nRunning?: {p.is_running}\n' )
    def get_phantom_id(self, keyword=None):
        # returns Phantom id -- if optional keyword arg is supplied, will search that keyword.
        # otherwise, will return most recent phantom id
        found = False
        if keyword:
            for p in self.phantoms:
                # print(p.name.lower())
                if keyword in p.name.lower().split(" "):
                    phantom_id = p.phantom_id
                    found = True
                    return phantom_id
        else:
            try: 
                phantom_id = self.phantoms[0].phantom_id
            except KeyError:
                return 'Phantom not found!'
        if found:
            return phantom_id 
        else:
            return 'Phantom not found!'



"""
--------------------------------------------------------------------------------------------------------
Various functions to test Phantombuster API. Many are redundant, but first need to see what each returns
to decide the best way to implement into script
--------------------------------------------------------------------------------------------------------
"""

def get_user_info():
    """Basic GET request to get account info. Primarily will be used for checking Usage time left."""
    user_url = "https://api.phantombuster.com/api/v1/user" # All Phantombuster API calls are done directly through URL
    headers = {
        "accept": "application/json",
        "X-Phantombuster-Key-1": API
    }
    request = requests.get(user_url, headers=headers, timeout=30)
    data = request.json()['data']
    # print(data)
    account = PhantomBusterAccount(email=data['email'], time_left=data['timeLeft'], open_slots=15-len(data['agents']))
    # Instantiating Phantom class members for each Agent (phantom) in Phantombuster account
    phantom_ids_and_status = []
    for agent in data['agents']:
        phantom_ids_and_status.append((agent['id'], agent['runningContainers'] > 0))
    for phantom in phantom_ids_and_status:
        phantom_data = fetch_phantom(phantom[0])
        account.phantoms.append(Phantom(
            phantom_id=phantom_data['id'],
            is_running=phantom[1],
            script=phantom_data['script'],
            name=phantom_data['name'],
            argument=phantom_data['argument'],
            last_updated=phantom_data['updatedAt'],
            launch_type=phantom_data['launchType'],
        ))
    return account

def fetch_phantom(phantom_id):
    """fetches all information about a specific phantom. Currently independent of Phantom/Account classes."""
    url = f"https://api.phantombuster.com/api/v2/agents/fetch?id={phantom_id}"
    headers = {
        "accept": "application/json",
        "X-Phantombuster-Key": API,
        }
    response = requests.get(url, headers=headers, timeout=30).json()
    # print(response)
    data = {
        "id":response['id'],
        "name":response['name'],
        "script":response['script'],
        "argument":response['argument'],
        "updatedAt":datetime.datetime.fromtimestamp(response['updatedAt']/1000),
        "launchType":response["launchType"]
    }
    return data

def upload_search_urls(csv_data, phantom_to_update=None):
    """uploads new list of google maps search URLs to phantombuster"""
    # Determining which Phantom to update with new CSV url
    print('\nGathering Phantombuster account info...')
    pb = get_user_info()
    csv_url = csv_data[0]
    csv_name = csv_data[1]
    if phantom_to_update: # accepts optional keyword argument to specify a particular phantom...
        phantom_to_update_id = pb.get_phantom_id(phantom_to_update)
    else: # ...otherwise, updates the "oldest" phantom
        oldest_date = datetime.datetime.max
        phantom_to_update_id = None
        for phantom in pb.phantoms:
            if phantom.last_updated < oldest_date:
                oldest_date = phantom.last_updated
                phantom_to_update_id = phantom.phantom_id
                phantom_to_update = phantom.name
    print(f'\nUploading csv to {phantom_to_update} phantom...')
    url = "https://api.phantombuster.com/api/v2/agents/save"
    headers = {
        "accept": "application/json",
        "X-Phantombuster-Key": API,
        }
    payload={"id":str(phantom_to_update_id), "argument":{
        "spreadsheetUrl":csv_url,
        "csvName":csv_name,
        "numberOfResultsPerSearch":200,
        "columnName": "url"
        }}
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        print(f'{response.status_code}: {response.reason}')
        if response.status_code == 200:
            print('\nCSV uploaded succesfully!')
        else:
            print(f'\nError! {response.status_code}: {response.reason}')
    except Exception as e:
        print(e)
        pass
    return (phantom_to_update, phantom_to_update_id)

def launch_phantom(phantom_id):
    """launches phantom via phantom id"""
    url = "https://api.phantombuster.com/api/v2/agents/launch"
    headers = {
        "accept": "application/json",
        "X-Phantombuster-Key": API,
        }
    try:
        response = requests.post(url, json={"id": str(phantom_id)}, headers=headers)
        utils.print_response(response)
        if response.status_code == 200:
            return "Success"
    except Exception as e:
        print(e)
        return None

def fetch_phantom_output(TAG, phantom_id, fetch_all=False):
    """Fetches output of most recent Phantom. Seems (is) redundant to fetch_phantom, but keeping for now."""
    url = f"https://api.phantombuster.com/api/v2/agents/fetch-output?id={phantom_id}" 
    headers = {
        "accept": "application/json",
        "X-Phantombuster-Key": API,
        }
    payload = {"id": str(phantom_id)}
    stored_timestamps = utils.load_timestamp()
    if not fetch_all:
        try: # timestamp is stored in registry 
            stored_timestamp = stored_timestamps[TAG] 
        except KeyError:
            stored_timestamp = None
        if stored_timestamp:
            payload["since"] = str(stored_timestamp)
    response = requests.get(url, headers=headers, json=payload, timeout=30)
    utils.print_response(response)
    data = response.json()
    print(response.text)
    # output_dict = {}
    # for item in data['output']:
    #     output_dict[item['id']] = item['data']
    # print(type(output_dict))
    container_id = data['containerId']
    status = data['status']
    output = data['output']
    outputPos = data['outputPos']
    print(f'{status} {outputPos}')
    split_output = output.strip().split(b'\xe2\x9c\x85'.decode('utf-8'))
    print(output)
    def parse_output(result):
        print(result)
        if "Scraped" in result:
            clean_result = result.strip("Scraped ").split()
            print(clean_result)

    # parse_output(split_output[50])

    # df = pd.DataFrame(response) # pylint: disable=all
    # df.to_json('testing_dump/fantom-output-test.json')
    # print(response.keys())



def fetch_results_csv(phantom_id, depth=1):
    """Fetches results object(s) from specified phantom and returns CSV urls for each 'container' (results from scrape)
       Optionally, takes depth as an argument to return multiple results files/urls"""
    
    def get_containers(phantom_id, depth):
        """helper function to return container IDs from phantom which...contain...the results files in CSV and JSON
           API call returns *all* containers, but depth specifies how many we get back"""
        # API setup + GET request
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-all?agentId={phantom_id}"
        headers = {"accept": "application/json","X-Phantombuster-Key-1": API}
        response = requests.get(url, headers=headers, timeout=30).json() 
        # Testing..
        # df = pd.DataFrame(response) # pylint: disable=all
        # df.to_json('testing_dump/fantom-allcontainer-test.json')
        container_data = response['containers']
        containers = []
        i = 0
        while i < depth: 
            containers.append(container_data[i]['id'])
            i+=1
        return containers
    if phantom_id == 'Phantom not found!':
        raise Exception('Error, no phantom id (Phantom not found)')
    containers = get_containers(phantom_id, depth)
    result_csvs = []
    print('\nGetting URLs...')
    response_array = []
    for i in tqdm(containers):
        container_id = i
        # another API call to return the results object for each container id
        url = f"https://api.phantombuster.com/api/v2/containers/fetch-result-object?id={container_id}"
        headers = {
            "accept": "application/json",
            "X-Phantombuster-Key": API,
            }
        response = requests.get(url, headers=headers, timeout=30).text
        # print(response)
        # most recent conatiner may not contain csv file. if found, splits string into array to isolate url
        if 'csv' in response: 
            response_array += response.split('"')
    # finds url in split response array
    if response_array:
        for i in response_array:
            if ".csv" in i:
                csv_url = i.replace('\\', '') # two slashes because it reads one as interrupt, just removing extraneous char from the end
                result_csvs.append(csv_url)
                print("\nSuccess!")
    else:
        if depth >= 6:
            # prevents endless recursion, stops after 6 iterations (arbitrary choice)...if no results found after 6, something is wrong
            print('\nError! No csv results found in 6 most recent containers. Terminating search...')
            return None
        else:
            # recursive call for if no .csv is found in results object...increments depth and tries again
            print('\nNo results csv found. Checking next container...')
            result_csvs += fetch_results_csv(phantom_id, depth=depth+1)
    # returns array of URL(s) of CSV(s) w/ scrape results
    return result_csvs

# Testing...

ACCOUNT = get_user_info()
# ACCOUNT.print_all_phantoms()

# camps_phantom_id = ACCOUNT.get_phantom_id('camps')
det_id = ACCOUNT.get_phantom_id('det')
# print(fetch_phantom(camps_phantom_id))
# camps_csv = fetch_results_csv(camps_phantom_id)
# print(camps_csv)

# url = "https://drive.google.com/file/d/1ne2E9KxZXtZk4bLkBpmG9Ay6yGo60C3i/view?usp=sharing"

# upload_search_urls(url, "dental")
# print(fetch_phantom(camps_phantom_id))
# fetch_phantom_output('det', det_id)
# fetch_containers(det_id)
# launch_phantom(dental_id)
