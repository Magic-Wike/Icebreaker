"""
----------------------------------------------------------------------------
Scripts for parsing and editing email copy + HTMLifies email copy
Eventually may contain API calls to ChatGPT/other AI service to auto-gen emails
----------------------------------------------------------------------------
"""

def linkify(body):
    """takes email body, searches for commonly linked terms and replaces with HTML links"""
    links = {
        "ugp_main": "https://undergroundshirts.com/",
        "full_catalog":"https://design.undergroundshirts.com/collections?_ga=2.167597180.391404341.1646665127-1580018936.1646422100",
        "design_studio":"https://design.undergroundshirts.com/designer",
        "screenprinting": "https://undergroundshirts.com/pages/screen-printing",
        "embroidery": "https://undergroundshirts.com/pages/embroidered-shirts",
        "dtg": "https://undergroundshirts.com/pages/direct-to-garment",
        "pop_up_shops": "https://undergroundshirts.com/pages/pop-up-online-store",
        "t-shirts": "https://design.undergroundshirts.com/collections/t-shirts-soft",
        "business_apparel": "https://design.undergroundshirts.com/collections/business-apparel",
    }
    body_array = body.split(' ')
    for i, word in enumerate(body_array):
        pass
