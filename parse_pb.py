import pandas as pd
from pprint import pprint as pr
import numpy as np
import pyinputplus as pyip
import tldextract
import math
from tqdm import tqdm

def format_result(url: str) -> str:
    result = tldextract.extract(url)
    return f'{result.domain}.{result.suffix}'

def clean_name(name: str) -> str:
    """parses and formats business names to cleaner formats"""
    if type(name) == str:
        parenthesis = ' ('
        new_name = name
        if parenthesis in str(name):
            new_name = name.split(parenthesis)[0]
        if ' at' in str(new_name):
            new_name = new_name.split(' at')[0]
        if ' - ' in str(new_name):
            new_name = new_name.split(' - ')[0]
        if ' @' in str(new_name):
            new_name = new_name.split(' @')[0]
        if ', LLC' in str(new_name):
            new_name = new_name.split(', LLC')[0]
        if ' LLC' in str(new_name):
            new_name = new_name.split(' LLC')[0]
        if ' | ' in str(new_name):
            new_name = new_name.split(' | ')[0]
        if ': ' in new_name:
            new_name = new_name.split(': ')[0]
        return new_name
    else:
        return name


def parse_results(CSV_URL, existing_customer_filepath):
    """Parses results CSV from Phantombuster...cleans up bad data, checks for duplicates, etc returns DataFrame of filtered results"""
    ecdf = pd.read_csv(existing_customer_filepath)[['Email']]
    existing_customer_domains = list(ecdf['Email'].apply(lambda x: str(x).split('@')[-1]))
    tqdm.pandas() # registers tqdm to pandas methods for progress bar on operations below..

    print('Reading data...') 
    pbdf = pd.read_csv(CSV_URL)[['title','category','address','website',]]
    pbdf.rename(columns={'title':'name'},inplace=True)
    print('Removing listings without websites...')
    pbdf.dropna(subset=['website'],inplace=True)
    print('Cleaning names...')
    pbdf['name'] = pbdf['name'].progress_apply(clean_name)
    print('Cleaning URLs...')
    pbdf['website'] = pbdf['website'].progress_apply(format_result)
    print('Removing duplicate entries...')
    pre = len(pbdf)
    pbdf.drop_duplicates(subset=['website'],inplace=True)
    post = len(pbdf)
    print(f'Removed {pre-post} duplicates.')
    print('Checking for existing customers...') 
    pbdf['remove'] = pbdf['website'].progress_apply(lambda x: np.nan if x in existing_customer_domains else 'keep')
    print('Removing existing customers...')
    pbdf.dropna(subset=['remove'],inplace=True)
    afterec = len(pbdf)
    print(f'Removed {post-afterec} existing customers.')
    # save_as = pyip.inputStr('Save as: \n')

    chunks = math.ceil(afterec/25000)
    if chunks > 1:
        print(f'Hunter allows for a maximum of 25,000 rows. Your file is being split into {chunks} CSVs.')
        for chunk in tqdm(range(chunks)):
            result = pbdf[chunk*25000:(chunk+1)*25000]
            # chunked..to_csv(f'upload_to_hunter_domain_search/{save_as}_{chunk+1}.csv',index=False) **(original code, creates CSV)**
    else:
        result = pbdf
        # pbdf.to_csv(f'upload_to_hunter_domain_search/{save_as}.csv',index=False)
        # print(f'Saved as {save_as}.csv')
    # print(f'Domains gathered: {len(pbdf)}') # **(original code, creates CSV)**
    print('Done!')
    return result # now returns either array of DataFrames or single DataFrame object


