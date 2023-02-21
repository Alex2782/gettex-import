import time
import random
from convert import load_isin_dict, get_all_isin_groups, get_isin_group_keys
from utils import *


def get_onvista_data(isin):

    ret = onvista_search(isin, False, False, False)

    instrument = ret['search']['instrument']
    entityType = instrument.get('entityType')
    entitySubType = instrument.get('entitySubType')

    issuer_name = None
    try:
        issuer_name = ret.get('derivative_snapshot').get('derivativesIssuer').get('name')
    except Exception as e:
        print(f"ERROR: {str(e)}")

    market_name = None
    next_data = ret.get('__NEXT_DATA__')
    if next_data:
        
        try:
            market_name = next_data['props']['pageProps']['data']['snapshot']['quote']['market']['name']
        except Exception as e:
            print(f"ERROR: {str(e)}")

    return entityType, entitySubType, issuer_name, market_name, ret

# ==================================================================

init_request()

#isin_dict, isin_dict_idx = load_isin_dict()
#print('len:', len(isin_dict))

isin_grp_dict = get_all_isin_groups()
groups = get_isin_group_keys()

for grp in groups:  
    isin_dict = isin_grp_dict[grp]['isin_dict']
    print(grp, 'len:', len(isin_dict))

    lang_isin = {}
    isin_group = {}

    for isin in isin_dict:
        lang = isin[0:2]

        obj = lang_isin.get(lang)
        if obj: obj['count'] += 1
        else: lang_isin[lang] = {'count': 1}

        if lang == 'DE':
            group = isin[5:7]
            obj = isin_group.get(group)
            
            if obj: 
                obj['count'] += 1
                if len(obj['isin']) < 5: obj['isin'].append(isin) 
            else: 
                isin_group[group] = {'count': 1, 'isin':[isin]}


    for lang in lang_isin:
        print ('lang:', lang, ', count:', lang_isin[lang]['count'])

exit()

for group in isin_group:

    print ('group:', group)
    print ('+++++++++++++++++++++++++++++++')

    if isin_group[group]['count'] > 10:
        for isin in isin_group[group]['isin']:
            time.sleep(random.uniform(1, 3))
 
            entityType, entitySubType, issuer_name, market_name, ret = get_onvista_data(isin)
            print(isin, ' = ', issuer_name, ' -> ', entityType, entitySubType) 

    print ('count:', isin_group[group]['count'], isin_group[group]['isin'])
    print ('-------------------------------')


#entityType, entitySubType, issuer_name, market_name, ret = get_onvista_data('DE000GF6GHC1')
#print (entityType, entitySubType, issuer_name)

# Emittent
# ======================================================================================================
# DE000CZ45VS1 = Commerzbank AG https://www.onvista.de/anleihen/COBA-MTH-S-P36-Anleihe-DE000CZ45VS1 
# DE000CZ40LG8 = Commerzbank AG https://www.onvista.de/anleihen/COBA-MTH-S-P11-16-26-Anleihe-DE000CZ40LG8

# DE000CH0SN33 = Goldman Sachs https://www.onvista.de/derivate/Knock-Outs/171463965-CH0SN3-DE000CH0SN33

# DE000A1EK0H1 = DB ETC https://www.onvista.de/derivate/ETCs/34592563-A1EK0H-DE000A1EK0H1
# DE000A1EWVY8 = (Land Deutschland) https://www.onvista.de/aktien/FORMYCON-AG-Aktie-DE000A1EWVY8

# DE000HW0KX45 = UniCredit https://www.onvista.de/derivate/Knock-Outs/176377767-HW0KX4-DE000HW0KX45
# DE000HW0S9K3 = UniCredit https://www.onvista.de/derivate/Knock-Outs/176727090-HW0S9K-DE000HW0S9K3

#-------------------------------------------------------------------------------------------------------
# HC (58438), HB (45829) = UniCredit
# TT (16724), HG (63147) = HSBC
# GZ (75294), GX (27242), GF (2152), GK (52050), GH (10947) = Goldman Sachs

