"""
Script generates Google Maps search URLs for cities within specified States for specified critera.
Currently takes user input for States, population/density requirments, etc to filter states. Can be 
used for targeted search or nationwide
"""
from pydantic import BaseModel
import pandas as pd
import pyinputplus as pyip
from tqdm import tqdm
from google_api import get_csv_for_phantombuster

class City(BaseModel):
    """Class object to store city data"""
    city: str
    state_name: str
    state_id: str
    lat: float
    lng: float
    population: int

class Net:
    "Class object to store net size data (for search parameters)"
    def __init__(self, min_density=400, max_density=29000, min_population=2500, max_population=20000000):
        self.min_density = min_density # density stays constant currently
        self.max_density = max_density
        self.min_population = min_population
        self.max_population = max_population


def set_net_size():
    """gets user input to determine 'net size', which refers to the scope of the search
        rather than making user enter population/density, will instead prepop
        these parameters with predetermiend scopes"""
    # First, give option of wide, medium, narrow or custom "net" size
    choices = ["Wide", "Medium", "Narrow", "Manual"]
    net_size_response = pyip.inputMenu(choices, "\nHow large of a net would you list to cast? (determines number and size of cities to search)\n", numbered=True)
    # if wide, include small, med and large cities
    if net_size_response == "Wide":
        net_size = Net() # Net class obj has "wide" params by default, no args needed
    # if medium, gives option of small/med vs. mid/large sized cities
    if net_size_response == "Medium":
        medium_choices = ["Small and Mid-Size Cities", "Mid-Size and Large Cities"]
        medium_response = pyip.inputMenu(medium_choices, "\nWhat sizes of cities would you like to include?\n", numbered=True)
        if medium_response == "Small and Mid-Size Cities":
            net_size = Net(min_population=2500, max_population=1000000)
        if medium_response == "Mid-Size and Large Cities":
            net_size = Net(min_population=1000000, max_population=20000000)
    # if narrow, choose one size
    if net_size_response == "Narrow":
        narrow_choices = ["Small Cities", "Medium Cities", "Large Cities"]
        narrow_response = pyip.inputMenu(narrow_choices, "\nWhat sizes of cities would you like to search?\n", numbered=True)
        if narrow_response == "Small Cities":
            net_size = Net(min_population=2500, max_population=150000)
        if narrow_response == "Medium Cities":
            net_size = Net(min_population=150000, max_population=1000000)
        if narrow_response == "Large Cities":
            net_size = Net(min_population=1000000, max_population=20000000)
    # if manual, set your own population numbers manually 
    if net_size_response == "Manual":
        min_population = pyip.inputInt('Minimum population of cities to search (do not include comma): \n(NYC Pop: ~18 million, Ann Arbor Pop: 121,536) \n')
        max_population = pyip.inputInt('Maximum population of cities to search (do not include comma):  \n(NYC Pop: ~18 million, Ann Arbor Pop: 121,536) \n')
        net_size = Net(min_population=min_population, max_population=max_population)
    return net_size


def get_list_of_search_urls():
    """Asks user for input to determine Google search parameters. Determines which cities to search
        based on population size / density"""
    df = pd.read_csv('uscities.csv')

    # Asks user to specify a state to search -- don't love this, may change later
    search_states = pyip.inputStr('List the states you would like to search. Separate each one by a comma. Use the 2-letter abbreviation. \n*To search all states, leave blank:*\n',blank=True)
    if search_states:
        states = search_states.split(',')
        states = [x.strip().upper() for x in states]
        df.query('state_id in @states',inplace=True)

    # NEW: simplified paramater getting to net_size (vs. user entering numbers manually). Sets search params based on size of the "net" we want to cast
    net_size = set_net_size() # gets user input to determine search params, and returns Net class object w/ params
    population_lower_limit = net_size.min_population
    population_upper_limit = net_size.max_population
    density_lower_limit = net_size.min_density
    density_upper_limit = net_size.max_density
    # Filters DataFrame based on search params
    if population_lower_limit:
        df.query('population >= @population_lower_limit', inplace=True)
    if population_upper_limit:
        df.query('population <= @population_upper_limit', inplace=True)
    if density_lower_limit:
        df.query('density >= @density_lower_limit', inplace=True)
    if density_upper_limit:
        df.query('density <= @density_upper_limit', inplace=True) 
   
    cities = [City(**row) for i,row in df.iterrows()] # return list of cities from df, filtered by params

    search_terms = input('List the categories/industries you would like to search. Separate each one by a comma.\n')
    if search_terms:
        search_term_list = search_terms.split(',')
        search_term_list = [x.strip() for x in search_term_list]
    else:
        print('No search terms provided')
        exit()

    data = []
    for search_term in search_term_list:
        for city in tqdm(cities):
            st = ' '.join([city.city.lower(),city.state_name.lower(),search_term.lower()])
            formatted_search_term = st.replace(' ','+')
            zoom = 12
            url = f'https://www.google.com/maps/search/{formatted_search_term}/@{city.lat},{city.lng},{zoom}z/'
            data.append(
                {
                    'url':url,
                    'city':city.city,
                    'term':search_term
                    
                }
            )
    print(f'Generated {len(data)} search URLs for {len(cities)} cities in {len(search_term_list)} categories.')
    
    # original code, exported search results to CSV...
    # save_as = input('Save as: ')
    # urlsheet = pd.DataFrame(data)
    # urlsheet.to_csv(f'search_urls/{save_as}.csv',index=False)
    # print(f'Done. Saved as {save_as}.csv')

    return data # returns list of objects with search url + metadata

