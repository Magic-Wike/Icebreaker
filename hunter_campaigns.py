"""
--------------------------------------------
Hunter.io Campagign management - primarily for generating reports on campaigns 
-Hunter Campaign API endpoints are very limited. Currently only allow for getting info and cancelling emails to recipients
--------------------------------------------
"""

# IMPORTS

import pandas as pd
from pydantic import BaseModel
from pprint import pprint as pr
import json
import datetime
import requests
import math
import pyinputplus as pyip
import config
from tqdm import tqdm
from utils import print_response

# CLASS DECLARATIONS

class Recipient(BaseModel):
    email: str
    first_name: str | None
    last_name: str | None
    company: str | None
    sending_status: str
    lead_id: int | None = None
    campaign_name: str or None
    quoted = bool | None
    domain_quoted = bool | None
    quoted_value = float | None
    converted = bool | None
    domain_converted = bool | None
    converted_value = float | None

    class Config:
        arbitrary_types_allowed = True

class Campaign(BaseModel):
    hunter_id: int
    name: str
    owner: str
    recipients_count: int
    recipients: list[Recipient] | None
    responses: int | None = 0
    opens: int | None = 0
    clicks: int | None = 0

# GLOBALS

HUNTER_API_KEY = config.HUNTER_API
HUNTER_BASE_URL = 'https://api.hunter.io/v2/'

# FUNCTIONS

def get_campaigns(keyword=None) -> 'list[Campaign]': # added optional keyword arg to get only certain campaigns
    url = HUNTER_BASE_URL + 'campaigns'
    total_campaigns = get_total_campaigns()
    campaigns = []
    for i in range(math.ceil(float(total_campaigns)/100.0)):
        params = {'limit':100,'offset':i*100,'api_key':HUNTER_API_KEY}
        r = requests.get(url,params=params)
        for response in r.json()['data']['campaigns']:
            if response['name'].split(' - ')[-1] == "on hold":
                owner_index = -2
            else:
                owner_index = -1
            if not keyword or keyword and keyword in response['name']: # added logic to check for keyword and only grab those results
                campaigns.append(Campaign(
                    hunter_id=response['id'],
                    name=response['name'],
                    owner=response['name'].split(' - ')[owner_index],
                    recipients_count=response['recipients_count']
                ))

    return campaigns

def get_recipients(campaigns: 'list[Campaign]') -> 'list[Recipient]':
    all_recipients = []
    for campaign in tqdm(campaigns):
        campaign.recipients = []
        url = f'{HUNTER_BASE_URL}campaigns/{campaign.hunter_id}/recipients?api_key={HUNTER_API_KEY}'
        for i in range(math.ceil(float(campaign.recipients_count)/100.0)):
            params = {'limit':100,'offset':i*100}
            r = requests.get(url,params=params)
            for rec in r.json()['data']['recipients']:
                campaign.recipients.append(Recipient(
                    email=rec['email'],
                    first_name=rec['first_name'] if rec['first_name'] else rec['company'],
                    last_name=rec['last_name'] if rec['last_name'] else None,
                    company=rec['company'],
                    sending_status=rec['sending_status'],
                    lead_id=rec['lead_id'] if rec['lead_id'] else 0,
                    campaign_name = campaign.name
                ))
        all_recipients += campaign.recipients
    return all_recipients

def filter_campaigns(campaigns: 'list[Campaign]', categories: 'list[str]') -> 'list[Campaign]':
    included_campaigns = []
    for campaign in tqdm(campaigns):
        if not categories:
            included_campaigns.append(campaign)
        else:
            for category in categories:
                if category.lower() in campaign.name.lower():
                    included_campaigns.append(campaign)
    return included_campaigns

def get_clicks_responses(included_campaigns: 'list[Campaign]') -> 'list[Recipient]':
    clicks_responses = []
    for campaign in tqdm(included_campaigns):
        for respondent in campaign.recipients:
            if respondent.sending_status in ['clicked','replied']:
                clicks_responses.append(respondent)
    return clicks_responses

def rates(x: pd.Series) -> pd.Series:
    if x['recipients_count']:
        x['response_rate'] = '{:.2f}'.format(x['responses'] / x['recipients_count'])
        x['open_rate'] = '{:.2f}'.format(x['opens'] / x['recipients_count'])
        x['click_rate'] = '{:.2f}'.format(x['clicks'] / x['recipients_count'])
    else:
        x['response_rate'] = False
        x['open_rate'] = False
        x['click_rate'] = False
    return x

def strip_dollar(x: str) -> float:
    if ',' in x:
        x = x.replace(',','')
    return float(x.strip('$'))

def get_total_campaigns():
    """gets exact number of campaigns from Hunter"""
    url = HUNTER_BASE_URL + "campaigns"
    offset = 0
    campaign_count = 0
    while True:
        params = {'limit': 100, 'offset':offset, "api_key": HUNTER_API_KEY}
        try:
            response = requests.get(url, params)
            # print(response.text)
            num_campaigns = len(response.json()['data']['campaigns'])
            campaign_count += num_campaigns
            if num_campaigns == 100:
                offset += 100
            else:
                break
        except Exception as e:
            print(e)
    return campaign_count

def generate_campaign_reports():
    adf = pd.read_csv('admin_info.csv')
    admin_slugs = list(adf['slug'])
    total_campaigns = get_total_campaigns()

    initial_prompt = 'Which cagegories would you like to include? These categories are parts of the names of the campaigns.'
    print(initial_prompt)

    to_include = []
    add_more = True
    while add_more:
        # want to accept blank input to return all campaigns
        category_name = pyip.inputStr(blank=True)
        to_include.append(category_name)
        another = pyip.inputYesNo('Add another category? (Y/N) \n')
        if another.upper() == 'NO':
            add_more = False

    recent_customer_filepath = pyip.inputFilepath('Existing customers filepath: \n')
    recent_quoter_filepath = pyip.inputFilepath('Recent quoter filepath: \n')

    qdf = pd.read_csv(recent_quoter_filepath)
    cdf = pd.read_csv(recent_customer_filepath)

    print('\n--Timeframe--')
    print('\nEnter each value as a number (i.e., 10 instead of October)')
    begin_month = pyip.inputInt('Start month: ')
    begin_day = pyip.inputInt('Start day: ')
    begin_year = pyip.inputInt('Start year: ')
    end_month = pyip.inputInt('End month: ')
    end_day = pyip.inputInt('End day: ')
    end_year = pyip.inputInt('End year: ')
    
    print('\nGetting campaigns from Hunter...')
    campaigns = get_campaigns() # returns list of Campaign objects
    print('\nFiltering campaign...')
    included_campaigns = filter_campaigns(campaigns,to_include) # uses tag/keyword to retrieve specific campaigns
    print('\nRetrieving recipients...')
    recipients = get_recipients(included_campaigns) # returns list of Recipient objects
    print(f'Retrieved {len(recipients)} recipients')

    for campaign in included_campaigns:
        campaign.responses = len([x for x in campaign.recipients if x.sending_status == 'replied'])
        campaign.opens = len([x for x in campaign.recipients if x.sending_status == 'opened'])
        campaign.clicks = len([x for x in campaign.recipients if x.sending_status == 'clicked'])

    sum_df = pd.DataFrame([x.dict(exclude={'recipients'}) for x in included_campaigns])
    sum_df = sum_df.apply(rates,axis=1)
    sum_df.to_csv(f'campaign_results/campaign_performance_{datetime.datetime.now().month}-{datetime.datetime.now().year}.csv',index=False)

    all_recipients = []
    for campaign in included_campaigns:
        recipients = campaign.recipients
        for recipient in recipients:
            recipient.campaign_name = campaign.name
            all_recipients.append(recipient)

    recipients_df = pd.DataFrame([x.dict(exclude={'quoted','domain_quoted','quoted_value','converted','converted_value','domain_converted'}) for x in all_recipients])
    recipients_df.to_csv(f'campaign_results/recipients_summary_{datetime.datetime.now().month}-{datetime.datetime.now().year}.csv',index=False)

    begin_date = datetime.date(year=begin_year,month=begin_month,day=begin_day)
    end_date = datetime.date(year=end_year,month=end_month,day=end_day)

    cdf['last_order'] = pd.to_datetime(cdf['last_order'])
    cdf['first_order'] = pd.to_datetime(cdf['first_order'])

    # filters DataFrame to remove 'last_orders' outside of date range. added logic to also remove 'first_orders' before the begin_date to help eliminate existing customers
    cdf.query('last_order >= @begin_date & last_order <= @end_date & first_order >= @begin_date',inplace=True)

    # NEW -- filters quote manager export to include only dates in range
    qdf['Created'] = pd.to_datetime(qdf['Created'])
    qdf.query('Created >= @begin_date & Created <= @end_date',inplace=True) 
    # END NEW STUFF

    qdf.rename(columns={'Email':'email','Value':'quote_group_value'},inplace=True)
    qdf.dropna(subset=['quote_group_value'],inplace=True)
    cdf.rename(columns={'Email':'email'},inplace=True)

    if type(qdf['quote_group_value']) is str:
        qdf['quote_group_value'] = qdf['quote_group_value'].apply(strip_dollar)

    recipients_df['domain'] = recipients_df['email'].apply(lambda x: str(x).split('@')[-1])
    qdf = qdf[~qdf['email'].isin(cdf['email'])] # NEW: added to try to remove existing customers from quoter data frame
    qdf['domain'] = qdf['email'].apply(lambda x: str(x).split('@')[-1])
    cdf['domain'] = cdf['email'].apply(lambda x: str(x).split('@')[-1])

    generic_domains = ['gmail.com','yahoo.com','aol.com','hotmail.com','comcast.net','sbcglobal.net',]
    def check_conversions(row: pd.Series) -> pd.Series:
        row['quoted'] = True if row['email'] in list(qdf['email']) else False
        row['domain_quoted'] = True if row['domain'] in list(qdf['domain']) and row['domain'] not in generic_domains else False
        row['ordered'] = True if row['email'] in list(cdf['email']) else False
        row['domain_ordered'] = True if row['domain'] in list(cdf['domain']) and row['domain'] not in generic_domains else False
        return row

    print('Checking conversions...')
    recipients_df = recipients_df.apply(check_conversions,axis=1)

    def quote_values(row: pd.Series) -> pd.Series:
        if row['domain_quoted'] == True:
            fdf = qdf.loc[qdf['domain'] == row['domain']]
            row['quote_value'] = sum(list(fdf['quote_group_value']))
        return row

    print('Compiling quote values...')
    recipients_df = recipients_df.apply(quote_values,axis=1)

    print('Querying conversions...')
    recipients_df.query('domain_quoted == True or domain_ordered == True', inplace=True)

    print('Saving...')
    recipients_df.to_csv(f'campaign_results/conversion_summary_{datetime.datetime.now().month}-{datetime.datetime.now().year}.csv',index=False)

def clean_recipients(TAG):
    """hardcoded, for one time use (hopefully) to remove problem leads from list"""
    campaigns = get_campaigns(TAG)
    recipients = get_recipients(campaigns)
    campaign_ids = {}
    leads_to_remove = {}
    leads_to_check = []
    
    print('\nGathering recipients...')
    for c in campaigns:
        campaign_ids[c.name] = c.hunter_id
        leads_to_remove[c.hunter_id] = []

    for r in tqdm(recipients):
        slug = r.email.split('@')[0]
        campaign_id = campaign_ids[r.campaign_name]
        if slug in ['info', 'contact', 'help', 'contact', 'sales', 'careers']:
            leads_to_remove[campaign_id].append(r.email)
        elif not r.first_name:
            leads_to_check.append((r.lead_id, campaign_id))
    # print(leads_to_remove)

    def check_catchall(lead_data):
        """queries hunter to determin if lead is catchall server"""
        lead_id = lead_data[0]
        campaign_id = lead_data[1]
        url = HUNTER_BASE_URL + 'leads'
        params = {"api_key": HUNTER_API_KEY, "id": lead_id}
        try:
            response = requests.get(url, params, timeout=30)
            lead = response.json()['data']['leads'][0]
            if lead['verification']['status'] == "accept_all":
                return (lead['email'], campaign_id)
            else:
                return False
        except Exception as e:
            print(e)
            return False

    print('\nChecking for catchall servers...')
    for lead in leads_to_check:
        is_catchall = check_catchall(lead) # returns tuple of (email, campaign_id) if true, false if not     
        if is_catchall:
            email, campaign_id = is_catchall[0], is_catchall[1]
            leads_to_remove[campaign_id].append(email)
    
    def cancel_sending(leads_to_remove):
        print('Cancelling sending to bad emails...')
        for campaign_id, emails in leads_to_remove.items(): 
            url = HUNTER_BASE_URL + f'campaigns/{campaign_id}/recipients?api_key={HUNTER_API_KEY}'
            if len(emails) > 50:
                max_chunk_size = 50
                chunks = [emails[i:i+max_chunk_size] for i in range(0, len(emails), max_chunk_size)]
            else: 
                chunks = [emails]
            for chunk in chunks:
                json_data = json.dumps(chunk)
                try:
                    response = requests.delete(url, json={"emails":chunk}, timeout=60)
                    print_response(response)
                    data = response.json()
                except Exception as e:
                    print(e)
                    continue

    cancel_sending(leads_to_remove)

# clean_recipients("safety")

generate_campaign_reports()