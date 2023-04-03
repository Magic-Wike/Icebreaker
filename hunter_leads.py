"""lead list creation and management using Hunter API"""

# imports

# pyhunter is wrapper for Hunter that supports most hunter methods including domain search
# and lead list controls. Does not support Campaigns so I will still need to write that myself
import pyinputplus as pyip
from pyhunter import PyHunter
from pydantic import BaseModel
from tqdm import tqdm
import pandas as pd
import datetime
import utils
from hunter_domain_search import bulk_domain_search, HunterResult, CSV_PATH, results_from_csv # for testing
from admins import Account # for testing
from admins import Admin # for testing
import admins
import requests
import config


hunter = PyHunter(config.HUNTER_API)
API = config.HUNTER_API

class LeadList(BaseModel):
    list_id: int
    name: str
    num_leads: int

class Lead(BaseModel):
    pass

def filter_generics(HunterResults):
    """filters Hunter leads to remove "bad generics" and returns updated list of results"""
    leads_to_keep = []
    bad_generics = ["info", "hello", "team", "help", "contact"]
    good_verify = ['valid', 'accept_all']
    for domain in tqdm(list(set([x.account.domain for x in HunterResults if x.account]))):
        domain_leads = [x for x in HunterResults if x.account and x.account.domain == domain]
        for lead in domain_leads:
            # Updated 3/27/2023...removed most 'good generics' and now filtering out all that do not have a first_name
            slug = lead.email.split('@')[0]
            if slug not in bad_generics and lead.verification_status in good_verify and lead.first_name:
                lead.good = True
            else:
                lead.good = False

        good_leads = [x for x in domain_leads if x.good]
        if good_leads:
            leads_to_keep.append(good_leads[0])
        else:
            leads_to_keep.append(domain_leads[0])
    return leads_to_keep

def verification_filter(HunterResults):
    """Loops through all Hunter Domain Search results and looks for unverified emails
       if email needs verification, runs verify_email function and updates results.
       will return newly fitlered list of HunterResult objects"""
    compare_date = datetime.date.today() - datetime.timedelta(days=182) # date object representing today minus 6 months
    print('\nFiltering and veryifying email addresses...')
    count = 0
    for result in tqdm(HunterResults):
        # during Domain Search, if Hunter has already verified lead, it will have verification info, but may not be up to date
        # first check: if no verification exists (None) or is older than 6 months, run Verifier
        if result.verification_date:
            # converts string time to date object in same format
            verify_date = datetime.datetime.strptime(result.verification_date, '%Y-%m-%d').date()
        else:
            verify_date = None
        if not result.verification_status or verify_date <= compare_date: 
            if result.verification_status == "invalid": # assuming anything already invalid will stay invalid. Skip. 
                result.good = False
                continue
            new_verification = verify_email(result.email) # verify_email function runs Hunter Email Verifier
            count+=1
            print(f'\nVerifying {result.email}...')
            if new_verification: # checks if verify_email returns a result
                verify_status, deliverability, confidence = new_verification[0], new_verification[1], new_verification[2]
            else: # if it does not, skip this email
                continue
            result.verification_status = verify_status
            result.confidence = confidence
            result.verification_date = datetime.date.today().strftime('%Y-%m-%d') #updates verify date to string representing today
            if deliverability == "deliverable": # Email Verifier actually tests deliverability, which supercedes status+confidence
                result.good = True
            if deliverability == "risky": # if risky, check confidence score and verify a first name exists
                if confidence >= 80 and result.first_name:
                    result.good = True
                else:
                    result.good = False
            if deliverability == "undeliverable":
                result.good = False
        elif result.verification_status == 'valid' and result.first_name: # if verified within 6 months and valid, lead is good
            result.good = True
        elif result.verification_status == 'accept_all': # if server is catchall, remove
            # if result.confidence >= 75 and result.first_name:
            #     result.good = True
            # else:
            result.good = False
        else:
            result.good = False # in all other cases (unknown status, low confidence, etc) remove leads
    print(f'\nSuccess! {count} emails verified.')
    leads_to_keep = [x for x in HunterResults if x.good]
    print(f'\nFound {len(leads_to_keep)} verifiable leads of {len(HunterResults)}')
    # Testing... 
    removed_leads = [x for x in HunterResults if not x.good] 
    removed_df = pd.DataFrame.from_records([x.to_dict() for x in removed_leads])
    removed_df.to_csv(f'testing_dump/removed-leads.csv', index=False)
    assert(len(leads_to_keep)+len(removed_leads) == len(HunterResults))
    # End Testing
    return leads_to_keep

def verify_email(email):
    """verfies email address deliverability via Hunter Email Verifier"""
    URL = "https://api.hunter.io/v2/email-verifier"
    PARAMS = {"email":email, "api_key":API}
    try:
        response = requests.get(URL, PARAMS, timeout=30)
        print(f'{response.status_code}: {response.reason}')
        data = response.json()
        if response.status_code != 200:
            pass
        else:
            verify_status = data['data']['status']
            confidence = data['data']['score']
            result = data['data']['result']
            return (verify_status, result, confidence) # returns tuple with gathered data
        
    except Exception as e:
        print(e)
    
    

# BROKEN AF
def search_leads(lead_list_tag=None, admin_slug=None, industry=None, uncontacted=False, limit=100):
    offset=0
    PARAMS = {"api_key":API, "limit":limit, "offset":offset}
    lead_lists = []
    if lead_list_tag:
        URL = "https://api.hunter.io/v2/leads_lists/1"
        lead_lists = get_lead_lists(keyword=lead_list_tag) # array of LeadList objects
    else:
        URL = "https://api.hunter.io/v2/leads"
    if industry:
        PARAMS['industry'] = industry
    if uncontacted:
        PARAMS['sending_status'] = "~"
        PARAMS["last_contacted_at"] = "~"
    # if admin_slug:
    #     PARAMS['for_user'] = admin_slug
    try:
        response = requests.get(URL, PARAMS, timeout=60)
        print(response.status_code)
        print(response.text)
        leads = response['data']['leads']
        print(leads)



    except Exception as e:
        print(e)

def get_lead_lists(keyword=None, retrieve_all=False, count=False):
    """creates array of LeadList class objects from Hunter API call
       optionally, takes Keyword argunment to search for specific lists"""
    URL = "https://api.hunter.io/v2/leads_lists"
    offset=0 # offset determines # to skip (like clicking next page). will be incremented if no result found to search next 100
    lead_lists = []
    while True:
        PARAMS = {"limit":100, "api_key":API, "offset":offset}   
        request = requests.get(URL, PARAMS)
        result = request.json()
        print(result)
        data = result['data']['leads_lists']
        num_lead_lists = result["meta"]["total"]
        for i in range(len(data)): # pylint: disable=all
            list_id = data[i]["id"]
            list_name = data[i]["name"]
            if keyword and keyword not in list_name: # skip if search term not found in list name
                continue
            else: # if found, create LeadList object and append to array
                lead_lists.append(LeadList(
                    list_id = list_id,
                    name=list_name,
                    num_leads=data[i]['leads_count']
                ))
        # if no result fount, or if retrieve_all is True increase offset and continue loop
        if retrieve_all or not lead_lists:
            offset += 100
        # STOP CONDITIONS / INF LOOP PROTECTION...
        # stop count = floor division to determine number of loops to run...
        stop_count = (num_lead_lists//100)*100+100 # +100 to ensure loop runs once more before terminating
        if offset == stop_count or len(lead_lists) == num_lead_lists:
            break

    if count: # if cunt is True, returns # of leads in returned lists instead of array of LeadList objects
        lead_count = 0
        for ls in lead_lists:
            lead_count += ls.num_leads
        return lead_count
    else:
        return lead_lists # returns array of LeadList objects

def get_lead_list_ids(tag):
    """returns lead list ids based on tag/scrape name. optionally accepts admin slug as arg to narrow search"""
    offset=0 # offset determines # to skip (like clicking next page). will be incremented if no result found to search next 100
    lead_list_ids = {}
    while True:
        URL = "https://api.hunter.io/v2/leads_lists"
        try: 
            PARAMS = {"limit":100, "api_key":API, "offset":offset}  
            request = requests.get(URL, PARAMS)
            result = request.json()
            lead_lists = result['data']['leads_lists']
            num_lead_lists = result['meta']['total']
            stop_count = (num_lead_lists//100)*100+100 # +100 to ensure loop runs once more before terminating
            for lead_list in lead_lists:
                if tag in lead_list["name"]:
                    lead_id = lead_list["id"]
                    admin_slug = lead_list["name"].split(' ')[-1]
                    lead_list_ids[admin_slug] = lead_id
            if offset < stop_count:
                offset+=100
            else:
                break
        except Exception as e:
            print(e)
        
    if len(lead_list_ids.keys()) > 1: # if more than one result, return dict of names/ids
        return lead_list_ids
    elif lead_list_ids: # if only 1 result is found, return only id instead of dict
        lead_list_id = list(lead_list_ids.values())[0]
        return lead_list_id

def create_leads_list(name):
    """function both creates Hunter Lead list via API request, but also
       returns Lead List id of resulting list"""
    URL = "https://api.hunter.io/v2/leads_lists"
    PARAMS = {"name":name, "api_key":API}
    response = requests.post(URL, PARAMS, timeout=30)
    print(response.reason)
    print(response.status_code)
    if response.status_code == 201:
        response = response.json()
        new_list = LeadList(
            list_id=response['data']['id'],
            name=response['data']['name'],
            num_leads=response['data']['leads_count']
            )
        print(f'\nSuccess! Lead List created.\nName: {new_list.name} / ID: {new_list.list_id}')
        return new_list.list_id
    else:
        raise Exception('\nError! Failed to create lead list (duplicate?)')
    
def delete_leads_list(id):
    url = f'https://api.hunter.io/v2/leads_lists/{id}?api_key={API}'
    # params = {'id':id, "api_key":API}
    response = requests.delete(url)
    if response.status_code == 204 or response.status_code == 202:
        print(f'\nSuccess! Lead List deleted.')
    else:
        print(f'\nError! Failed to delete Lead List...\nReason: ({response.status_code}){response.reason}')
    utils.clear_screen()

def bulk_delete_lead_lists(tag):
    lead_list_ids = get_lead_list_ids(tag)
    if lead_list_ids:
        for l in tqdm(lead_list_ids.values()):
            delete_leads_list(l)
        print(f'\nDeleted {len(lead_list_ids)} lead lists.')
    else:
        print('\nError! No lead lists found')
        
def create_lead(HunterResult, list_id, TAG=None, update=False):
    """creates leads from HunterResult class objescts and POSTs to Hunter.io via API request"""
    if not list_id:
        raise KeyError('\nError! Must include a lead list id.')
    PARAMS = {
        'email':HunterResult.email or None,
        'first_name':HunterResult.first_name or None,
        'last_name':HunterResult.last_name or None,
        'position':HunterResult.position or None,
        'company':HunterResult.organization,
        'company_industry':HunterResult.account.category or None,
        'website':HunterResult.domain,
        'linkedin_url':HunterResult.linkedin or None,
        'phone_number':HunterResult.phone or None,
        'twitter':HunterResult.twitter or None,
        'confidence_score':HunterResult.confidence or None,
        'leads_list_id':list_id,
        'custom_attributes': {
            "owner_first_name": HunterResult.account.owner.first_name,
            "short_name": HunterResult.account.owner.slug,
            "address": HunterResult.account.address or None,
            "admin_location": HunterResult.account.owner.city,
            "lead_specific_tag": TAG or HunterResult.account.category
        },
        'api_key': API
    }

    URL = 'https://api.hunter.io/v2/leads'
    if update: # if Update is true, sends PUT request to update lead
        try: 
            response = requests.put(URL, json=PARAMS, timeout=30)
            print(f'{response.status_code}: {response.reason}')
            if response.status_code not in [200, 201]:
                print(f'\nError! Lead was not updated.\nReason {response.status_code}: {response.reason}')
        except Exception as e:
            print(e)
            pass
    else: # otherwise, sends POST request to create
        try: 
            response = requests.post(URL, json=PARAMS, timeout=30)
            if response.status_code not in [200, 201]:
                print(f'\nError! Lead was not created.\nReason {response.status_code}: {response.reason}')
        except Exception as e:
            print(e)
            pass

def bulk_lead_list_create(TAG, ADMINS=None):
    """create lead lists in Hunter for each admin w/ supplied tag"""
    if not ADMINS:
        ADMINS = admins.get_admins()
    print('\nCreating Hunter Lead Lists...')
    for admin in tqdm(ADMINS):
        lead_list_ids = {}
        try:
            new_list_id = create_leads_list(f'{TAG} - {admin.slug}')
            lead_list_ids[admin.slug] = new_list_id
        except Exception as e:
            print(e)
        # utils.clear_screen()
    return lead_list_ids

def bulk_lead_create(TAG, hunter_results, ADMINS=None, update=False):
    """create leads in Hunter and assigns them to already created leads lists.
       this is the same as in main cold_out script, but writing separately
       to upload csv backup if needed"""
    print('\nGathering data...')
    if not ADMINS:
        ADMINS = admins.get_admins(exclude_RM=True)
    print('\nCreating Leads in Hunter...')
    admin_slugs = set(admin.slug for admin in ADMINS)
    lead_list_ids = get_lead_list_ids(TAG) # returns dict of admin.slug / list id pairs OR single 
    count = len(hunter_results)
    for lead in tqdm(hunter_results):
        admin_slug = lead.account.owner.email.split('@')[0]
        # handling for if only one lead list is found (integer returned vs. dict)
        if type(lead_list_ids) == int:
            lead_list_id = lead_list_ids
        else:
            lead_list_id = lead_list_ids[admin_slug]
        # Testing...
        if admin_slug not in admin_slugs:
            print('REKT')
            continue
        if update:
            create_lead(lead, lead_list_id, update=True)
        else:
            create_lead(lead, lead_list_id)
    print(f'\nSuccess! {count} leads created.')

def reassign_leads_list(current_list_id, new_list_keyword):
    # get all leads from current list
    print("\nGathering leads to move...")
    offset = 0
    lead_ids_to_move = []

    def get_leads_to_move(current_list_id, offset):
        try:
            lead_list_url = f"https://api.hunter.io/v2/leads_lists/{current_list_id}"
            response = requests.get(lead_list_url, {"api_key":API, "limit":100, "offset":offset}, timeout=30)
            # print(f'{response.status_code}: {response.reason}')
            data = response.json()
            leads_count = data["data"]["leads_count"]
            leads = data["data"]["leads"]
        except Exception as e:
            print(e)
        return leads

    # stop_count = (leads_count//100)*100+100 # +100 to make sure loop runs one final time
    while True:
        leads = get_leads_to_move(current_list_id, offset)
        for lead in leads:
            lead_id = lead["id"]
            admin_slug = lead["short_name"]
            lead_ids_to_move.append((lead_id, admin_slug))
        if not len(leads) < 100:
            offset+=100
        else:
            break
    print('\nDone!')

    # move leads to new lists based on admin slug + keyword
    new_lead_list_ids = get_lead_list_ids(new_list_keyword) # dictionary of {admin slug: list_ids}
    print('\nMoving leads to new lists...')
    for lead_data in tqdm(lead_ids_to_move): # lead_ids.. is list of (lead_id, admin_slug) tupless
        admin_slug = lead_data[1]
        lead_id = lead_data[0]
        new_list_id = new_lead_list_ids[admin_slug]
        lead_url = f'https://api.hunter.io/v2/leads/{lead_id}'
        params = {"api_key":API, "leads_list_id":new_list_id}
        try:
            response = requests.put(lead_url, params, timeout=60)
            print(f'{response.status_code}: {response.reason}')
        except Exception as e:
            print(e)
            continue


# new_list_1 = get_lead_list_ids("new list 1")
# reassign_leads_list(new_list_1, "safety")

CCALE_STUDENT_ORGS_ID = 3748662
# reassign_leads(CCALE_STUDENT_ORGS_ID, new_admin_email)

def delete_lead(lead_id):
    """deletes a lead in Hunter based on lead id"""
    URL = f"https://api.hunter.io/v2/leads/{lead_id}"
    try:
        response = requests.delete(URL, {"api_key":API}, timeout=30)
        if response.status_code != 204:
            print(f'\nError! Lead not deleted. Reason: {response.reason}')
        else:
            print(f'\nLead {lead_id} deleted succesfully!')
    except Exception as e:
        print(e)
        pass

def count_leads(monthly=True):
    """counts leads uploaded for Hunter campaigns. optionally searched by tag/keyword. by default counts only from last 30 days"""
    tag_response = pyip.inputStr("\nEnter tags of campaigns to count (separated by commas)...\n")
    tags = [x.strip() for x in tag_response.split(',')]
    lead_list_ids = []
    for t in tags:
        lead_list_ids += (list(get_lead_list_ids(t).values()))
    
    num_leads = 0
    limit = 1000 # of leads to return per query -- 1000 is max
    url = "https://api.hunter.io/v2/leads"
    for list_id in tqdm(lead_list_ids):
        offset = 0
        print("\nCounting leads...")
        while True:
            params = {"api_key": API, "limit":limit, "offset":offset, "leads_list_id":list_id}
            try:
                response = requests.get(url, params, timeout=60)
                data = response.json()
                leads = data['data']['leads']
                count = data['meta']['count']
                total = data['meta']['total']
                # print(f'Current # leads: {len(leads)}\nCount: {count}')
                for lead in leads:
                    thirty_days_ago = datetime.datetime.now() - datetime.timedelta(30)
                    upload_date = datetime.datetime.strptime(lead['created_at'], "%Y-%m-%d %H:%M:%S %Z")
                    if monthly and upload_date <= thirty_days_ago:
                        num_leads += 1
                    elif not monthly:
                        num_leads += 1
                offset+=limit
                stop_count = (count//10)*10+1000
                if offset >= stop_count:
                    break

                    
            except Exception as e:
                print(e)
                continue
    return num_leads

test = count_leads()
print(test)

def upload_from_csv_backup(TAG, PATH):
    """uploads leads to Hunter from backup CSV file (for upload errors)"""
    HUNTER_RESULTS = results_from_csv(PATH)
    # print(HUNTER_RESULTS)
    bulk_lead_create(TAG, HUNTER_RESULTS, update=True)

# TAG = "det"
# path = 'backup_csvs/lead_backup_det.csv'
# upload_from_csv_backup(TAG, path)
# bulk_delete_lead_lists("test")