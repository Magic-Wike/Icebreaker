"""Main file to execute the Cold Out Program"""

import os
# from tkinter import *
# from tkinter import ttk
import pandas as pd
import pyinputplus as pyip
from tqdm import tqdm
import phantombuster as pb
from hunter_leads import *
from parse_pb import parse_results
from hunter_domain_search import bulk_domain_search
from get_urls import get_list_of_search_urls
from google_api import get_csv_for_phantombuster
import admins


class Session:
    """class object to collect information about the scraping sessions.
       planning to use this to collect all user inputs up front and pass them as args later
       vs. collecting them as they come up, which is how it works currently"""
    def __init__(self, name=None):
        if not name:
            self.get_session_name()
        else:
            self.name = name
        self.tag = self.name
        self.search_terms = []
        self.min_population = float('inf')
        self.max_population = float('-inf')
        self.min_density = float('inf')
        self.max_density = float('-inf')
        self.excluded_store_codes = []
        self.existing_customer_file = self.set_customer_filepath()

    def get_session_name(self):
        while True:
            response = pyip.inputStr("\nEnter the name/category for this session. (One word. Will be used to for tagging and naming)\n")
            response.lower().strip()
            split_response = response.split(" ")
            if len(split_response) > 1:
                print('\nError: Input too long. Must be only one word.')
                continue
            else:
                self.name = response
                self.tag = response
                break
        

    def set_customer_filepath(self):
        """gets user input for current customer csv filepath and updates class property.
           hopefully to be replaced by automated method of getting UGP customer data"""
        response = pyip.inputFilepath(prompt='Paste the filepath of the existing customer CSV: \n')
        self.existing_customer_file = response
        return self.existing_customer_file

def get_start_point():
    """get user input on which point to start execution"""
    CHOICES = ["Start new scrape", "Continue existing scrape", "Continue from backup CSV", "List Phantoms"]
    START_POINT = pyip.inputMenu(choices=CHOICES, prompt="\nWelcome to Cold Out 2.0! Please select an option to get started...\n", numbered=True)
    return START_POINT

"""
--------------------------------------------------------------------------------------------------
Program start...
--------------------------------------------------------------------------------------------------
"""

# Initial state. Get user input on where to start execution
START_POINT = get_start_point()

if START_POINT == "List Phantoms":
    print('\nGetting phantom info...')
    phantombuster = pb.get_user_info()
    phantombuster.print_all_phantoms()
    to_menu = pyip.inputYesNo("\nBack to menu? (Y/N):\n$")
    if to_menu:
        get_start_point()
    else:
        quit()

# Nw sessions is created, global variables declaration
SESSION = Session()
TAG = SESSION.tag

# Starting from scratch. Will generate search url csv and upload csv to Phantombuster
if START_POINT == "Start new scrape":
    SEARCH_URLS = get_list_of_search_urls() # executes get_urls.py
    SEARCH_URLS_DATA = get_csv_for_phantombuster(SEARCH_URLS, TAG) # creates public csv on Drive and returns tuple with (URL, file name)
    prompt = '\nWould you like to specify which Phantom to use? (If no, oldest Phantom will be used)\n'
    specify_phantom = pyip.inputYesNo(prompt)
    if specify_phantom:
        while True:
            phantom_to_update = pyip.inputStr("\nEnter name of Phantom to update (one word)")
            if len(phantom_to_update.split()) > 1:
                print("\nError! Input can only be one word.")
                continue
            else:
                break
    else:
        phantom_to_update = None
    upserted_phantom = pb.upload_search_urls(SEARCH_URLS_DATA, phantom_to_update) # returns tuple of phantom name and id
    # check if user is ready to launch phantom, if so, execute
    ready_to_launch = pyip.inputYesNo(f"\nWould you like to launch {upserted_phantom[0]} Phantom?\n$")
    if ready_to_launch:
        launch_result = pb.launch_phantom(int(upserted_phantom[1]))
        if launch_result == "Success":
            print(f'\nPhantom succesfully launched! Approximate Phantom exection time is 5 hours. Use the tag {TAG} to access this Phantom later.')
            quit()
        else:
            pass # come back and touch up once launch function is written
    else:
        print(f'\nExiting! Use the tag "{TAG}" to access this Phantom later.')
        quit()

# Picking up from existing search. Assumes Phantom has already been run -- gathers results, filters them, and uploads to Hunter
if START_POINT == "Continue existing scrape":
    existing_customer_filepath = SESSION.existing_customer_file # original code to be (hopefully replaced with UGP API)
    ACCOUNT = pb.get_user_info()
    PHANTOM_ID = ACCOUNT.get_phantom_id(TAG)

    # Testing..
    # dental_phantom_id = ACCOUNT.get_phantom_id("dental")
    # recent_phantom_id = ACCOUNT.get_phantom_id()
    # DENTAL_ID_LITERAL = 2921674486435367 # for testing
    # RESULT_CSV = ["https://cache1.phantombooster.com/CKNJu5bCStA/Or8YMaIpASxZZY0aylBEvQ/cary_restaurants.csv"]


    print('\nFetching results from Phantombuster...')
    RESULT_CSV = pb.fetch_results_csv(PHANTOM_ID)

    print('\nParsing Phantombuster results...')
    PB_RESULTS = []
    # parse_results returns single dataframe or array of dataframes currently
    # returned DF(s) should include formatted/filtered PB results
    if len(RESULT_CSV) > 1:
        for url in RESULT_CSV: 
            PB_RESULTS.append(parse_results(url, existing_customer_filepath))
        PB_RESULTS = pd.concat(PB_RESULTS, ignore_index=True)
    else:
        PB_RESULTS = parse_results(RESULT_CSV[0], existing_customer_filepath)
        # PB_RESULTS.to_csv('testing_dump/pb-format-test.csv')

    # Assign leads to Admnis, initiate Hunter Domain Search
    print('\nRetrieving admins...')
    ADMINS = admins.get_admins() # includes prompt to exclude store codes, likely will need to move this prompt

    print('\nCreating lead accounts and assigning owners...') 
    ALL_ACCOUNTS = admins.get_accounts(PB_RESULTS,ADMINS) # assigns admins to each lead. returns array of Accounts objects
    acceptable_categories = admins.get_acceptable_categories(ALL_ACCOUNTS)
    GOOD_ACCOUNTS = [x for x in ALL_ACCOUNTS if x.category in acceptable_categories]
    # Testing...
    account_df = pd.DataFrame(GOOD_ACCOUNTS)
    account_df.to_csv('testing_dump/pb-good-accounts-test.csv')

    initiate_domain_search = pyip.inputYesNo(prompt=f"\n{len(GOOD_ACCOUNTS)} good domains found. Execute Hunter Domain search? (This process will take several minutes depending on length of list)\n$")
    if initiate_domain_search == "yes":
        print('\nExecuting Hunter.io Domain Search...')
        HUNTER_RESULTS = bulk_domain_search(GOOD_ACCOUNTS, TAG)
        # Generate backup CSV...remove after testing
        upload_data = utils.generate_backup_csv(HUNTER_RESULTS, TAG)
        udf = pd.DataFrame(upload_data)
        udf.to_csv(f'backup_csvs/unfiltered_lead_backup_{TAG}.csv',index=False)
    else:
        print('\nOperation cancelled! Exiting...')
        quit()

    # Filter Hunter search results, create Lead Lists and Leads
    print('\nFiltering leads...')
    generic_filter = filter_generics(HUNTER_RESULTS) # filters out bad generic web domains
    keep_leads = verification_filter(generic_filter) # filters based on deliverability + confidence score 

    # creates physical CSV of leads in case of interrupt/failure
    print('\nCreating backup CSV file...') 
    upload_data = utils.generate_backup_csv(keep_leads, TAG)
    print('\nSaving backup CSV...')
    udf = pd.DataFrame(upload_data)
    udf.to_csv(f'backup_csvs/lead_backup_{TAG}.csv',index=False)

    # prompts user and uploads filtered leads to hunter
    initiate_lead_upload = pyip.inputYesNo(prompt=f"\n{len(keep_leads)} leads prepared for upload. Continue with upload? (This process will take several minutes depending on length of list)\n$")
    if initiate_lead_upload == "yes":
        print('\nCreating Hunter Lead Lists...')
        lead_list_ids = bulk_lead_list_create(TAG, ADMINS)
        print('\nCreating Leads in Hunter...') 
        bulk_lead_create(TAG, keep_leads, ADMINS)
        utils.store_timestamp(TAG) # stores timestamp in registry with {{TAG:timestamp}. used to get incremental results from phantombuster 
    else:
        print('\nOperation cancelled! Exiting...')
        quit()


## Step 5.5: create email templates?
## Step six: create and execute Hunter Campaign - no API for this currently.
