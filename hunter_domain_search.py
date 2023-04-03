"""domain search script for Hunter.io -- will bulk search domains from Phantombuster results
   and return email addresses + info for each domain"""

from pyhunter import PyHunter
from pydantic import BaseModel
import pandas as pd
from tqdm import tqdm
from admins import Account
from datetime import date, datetime, timedelta
import requests
import admins
import config
# Testing...
# from test import SAMPLE_ACCOUNTS_LITERAL, ALL_ACCOUNTS_LITERAL

hunter = PyHunter(config.HUNTER_API)
API = config.HUNTER_API

class HunterResult(BaseModel):
    """main lead object. stores info about lead + admin assigned to it"""
    input_domain: str
    email: str
    domain: str
    organization: str
    confidence: int | None
    email_type: str
    num_sources: int | None
    pattern: str | None
    first_name: str | None
    last_name: str | None
    department: str | None
    position: str | None
    twitter: str | None
    linkedin: str | None
    phone: str | None
    verification_status: str | None
    verification_date: str | date | None
    account: Account | None = None
    good: bool | None

    def to_dict(self):
        """returns dict version of self, for easy dataframing"""
        return {
            'input_domain': self.input_domain,
            'email': self.email,
            'domain': self.domain,
            'organization': self.organization,
            'confidence': self.confidence,
            'email_type': self.email_type,
            'num_sources': self.num_sources,
            'pattern': self.pattern,
            'first_name': self.first_name,
            'last_name': self.last_name,
            'department': self.department,
            'position': self.position,
            'twitter': self.twitter,
            'linkedin': self.linkedin,
            'phone': self.phone,
            'verification_status': self.verification_status,
            'verification_date': self.verification_date,
            'account_name': self.account.name,
            'address': self.account.address,
            'category': self.account.category,
            'owner_email': self.account.owner.email,
            'owner_first_name': self.account.owner.first_name,
            'good': self.good      
        }

# response = hunter.domain_search("undergroundshirts.com", company='Underground Printing')
# emails_df = pd.DataFrame(response['emails'])

def bulk_domain_search(GOOD_ACCOUNTS, TAG):
    """process multiple hunter domain_searches. sends GET request for each query, which returns dict with response/results
       (will likely pass results directly to hunter lead lists, but want to save CSV as well in case of program interrupt)"""
    all_hunter_results = []
    errors = []
    for account in tqdm(GOOD_ACCOUNTS):
        try:
            response = hunter.domain_search(domain=account.domain, company=account.name)
            for email in response['emails']:
                all_hunter_results.append(HunterResult(
                    input_domain=response['domain'],
                    email=email['value'],
                    domain=response['domain'],
                    organization=response['organization'] or account.name,
                    email_type= email['type'],
                    num_sources=len(email['sources']),
                    pattern=response['pattern'],
                    first_name=email['first_name'],
                    last_name=email['last_name'],
                    department=email['department'],
                    position=email['position'],
                    twitter=email['twitter'],
                    linkedin=email['linkedin'],
                    phone=email['phone_number'],
                    confidence=email['confidence'],
                    verification_status=email['verification']['status'],
                    verification_date=email['verification']['date'],
                    account=account,
                    good=True
                ))                
        except Exception as e:
            print(e)
            continue
    print(f'\nSuccess! {len(all_hunter_results)-len(errors)} domains searched!')
    success_df = pd.DataFrame.from_records([r.to_dict() for r in all_hunter_results])
    success_df.to_csv(f'testing_dump/{TAG}-domain-search-backup.csv')
    print(f'\n{len(errors)} searches unsuccesful.')
    # error_df = pd.DataFrame(errors)
    # error_df.to_csv('testing_dump/domain-search-error-report.csv')


    return all_hunter_results # returns array of all HunterResults objects

# result_df = pd.DataFrame(bulk_domain_search(SAMPLE_ACCOUNTS_LITERAL))
# result_df.to_csv('testing_dump/test-hunter-domain-search.csv', index=False, header=True)

CSV_PATH = "upload_to_nutshell/lead_backup_safety.csv" # will be call to phantombuster.py for results object

def results_from_csv(CSV_PATH):
    """creates array of HunterResult objects from CSV
       To be used to restart from CSV backup if program fails/interrupts"""
    print("\nParsing CSV backup...")
    df = pd.read_csv(CSV_PATH, index_col=None)
    df.fillna('', inplace=True) 
    result = []
    # total is for tqdm to manually give total num. takes this from "shape" of dataframe. 
    # since iterrows is 2 dimensional, we specify just one of the dimensions
    for i, row in tqdm(df.iterrows(), total=df.shape[0]): 
        result.append(HunterResult(
            input_domain=row['domain'],
            email=row['email'],
            domain=row['domain'],
            organization=row['organization'],
            email_type=row['email_type'],
            num_sources=row['num_sources'],
            pattern=row['pattern'],
            first_name=row['first_name'],
            last_name=row['last_name'],
            department=row['department'],
            position=row['position'],
            twitter=row['twitter'],
            linkedin=row['linkedin'],
            phone=row['phone'],
            confidence=row['confidence'],
            verification_date=row['verification_date'],
            verification_status=row['verification_status'],
            account=Account(
                name=row['account_name'],
                domain=row['domain'],
                address=row['address'],
                state=None, # need to fix. backup csv doesn't store city/state
                city=None,
                category=row['category'],
                owner=admins.get_admin(row['owner_email']),
                owner_first_name=row['owner_first_name'],
                owner_email=row['owner_email']
            ),
            good=True
        ))
    return result 

# test = results_from_csv(CSV_PATH)
# print(test)

# data = hunter.domain_search("undergroundshirts.com")
# # print(data['emails'][0].keys())
# test_date = data['emails'][0]['verification']['date']
# print(test_date)
# test_new_date = datetime.strptime(test_date,'%Y-%m-%d')
# print(type(test_new_date))
# print(test_new_date)
# compare_date = datetime.now()
# print(compare_date)
# print(test_new_date < compare_date)
