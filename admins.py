"""
Retrieves UGP Admin info from admin_info.csv and creates a list of Admin class objects. 
This will determine the assignment of leads based on the Admins locations
"""

from pydantic import BaseModel
import pandas as pd
import pyinputplus as pyip
import numpy as np
from pprint import pprint as pr
import usaddress
import random
import math

# class instantiation and type checking via Pydantic
class Admin(BaseModel):
    first_name: str
    last_name: str
    slug: str
    email: str
    city: str or None
    state: str or None
    store_code: str

class Account(BaseModel):
    """an 'account' here represents a lead and it's data + admin assignment"""
    name: str
    domain: str
    address: str | None
    state: str | None
    city: str | None
    category: str
    owner: Admin

# functions...

def get_admins(exclude_RM=False) -> list[Admin]:
    df = pd.read_csv('admin_info.csv')
    if exclude_RM: # skips stores to exlucde prompt if exlcude RM is true
        excluded_store_codes = "'RM', 'XX'"
    else:
        excluded_store_codes = pyip.inputStr(
            prompt='Input the store codes you would like to exclude from this campaign, separated by commas. Note, RM is the store code for Regional Managers. \n',
            blank=True
        )
        print('ok')
    if excluded_store_codes:
        esl_list = excluded_store_codes.split(',') 
        esl_list = [x.strip().upper() for x in esl_list]
        df.query('store_code not in @esl_list',inplace=True)
    
    return [Admin(**row) for i,row in df.iterrows()]

def get_admin(email):
    """searches and returns single admin object based on email"""
    df = pd.read_csv('admin_info.csv')
    row = df.query('email == @email').squeeze().to_dict()
    return Admin(**row)

def get_accounts(data_frame: str, admins: list[Admin]) -> list[Account]:
    accounts = []
    #splits dataframe into chunks for memory purposes
    array_df = np.array_split(data_frame, 1000)
    for chunk in array_df:
        for i, row in chunk.iterrows():
            if row['address']:
                try:
                    #formats addresses into Address objects to allow for geographical lead assignment
                    address = usaddress.tag(str(row['address']))[0]
                    state = address['StateName'] if 'StateName' in address.keys() else None
                    city = address['PlaceName'] if 'PlaceName' in address.keys() else None
                    if city:
                        city_admins = [x for x in admins if x.city == city] # first looks for any admins in current city
                        if city_admins:
                            admin = random.choice(city_admins) # if so assign admin from that city at random
                        elif state: 
                            state_admins = [x for x in admins if x.state == state]
                            if state_admins: # if there are no admins in that city, but are mins in the state, assign
                                admin = random.choice(state_admins)
                            else:
                                admin = random.choice(admins)
                        else: # this seems redundant...but if not city or state admins, randomly assign from all
                            admin = random.choice(admins)
                    elif state:
                        state_admins = [x for x in admins if x.state == state]
                        if state_admins:
                            admin = random.choice(state_admins)
                        else:
                            admin = random.choice(admins)
                    else:
                        admin = random.choice(admins)
                except: # if address parsing fails or no address, assign to random admin
                    admin = random.choice(admins) 
                    city = None
                    state = None
            else:
                admin = random.choice(admins)
                city = None
                state = None
                
            accounts.append(
                Account(
                    name=row['name'],
                    domain=row['website'],
                    address=row['address'],
                    state=state,
                    city=city,
                    category=row['category'],
                    owner=admin
                )
            )
    return accounts


def get_acceptable_categories(all_accounts: list[Account],acceptable_category_cutoff_pct: float = 0.05) -> list[str]:
    all_categories = [x.category for x in all_accounts] # creates list of all categories in csv

    category_dict = {}
    for c in list(set(all_categories)): # creating a Set removes duplicates
        category_dict[c] = all_categories.count(c) # counts # of times category occurs in all_cat array. creates dict entry w/ Category: # occurences

    sorted_category_list = sorted(category_dict.items(), key=lambda x: x[1], reverse=True) 

    acceptable_category_cutoff = math.ceil(sum([value for key,value in category_dict.items()])*acceptable_category_cutoff_pct)

    return [x[0] for x in sorted_category_list[:acceptable_category_cutoff]]
