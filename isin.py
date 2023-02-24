import time
import random
from convert import load_isin_dict, get_all_isin_groups, get_isin_group_keys
from utils import *
from open_html import *
from validate_isin import *

# ------------------------------------------------------------------------------------
# get_onvista_data
# ------------------------------------------------------------------------------------
def get_onvista_data(isin):

    entityType = None
    entitySubType = None
    issuer_name = None
    market_name = None
    ret = None

    try:
        ret = onvista_search(isin, False, False, False)
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return entityType, entitySubType, issuer_name, market_name, ret

    try:
        instrument = ret['search']['instrument']
        entityType = instrument.get('entityType')
        entitySubType = instrument.get('entitySubType')
    except Exception as e:
        print(f"ERROR: {str(e)}")

    try:
        issuer_name = ret.get('derivative_snapshot').get('derivativesIssuer').get('name')
    except Exception as e:
        print(f"ERROR: {str(e)}")

    next_data = ret.get('__NEXT_DATA__')
    if next_data:
        
        try:
            market_name = next_data['props']['pageProps']['data']['snapshot']['quote']['market']['name']
        except Exception as e:
            print(f"ERROR: {str(e)}")

    return entityType, entitySubType, issuer_name, market_name, ret

# ------------------------------------------------------------------------------------
# check_isin_groups
# ------------------------------------------------------------------------------------
def check_isin_groups(isin_group):

    for group_name in isin_group:

        print ('group_name:', group_name)
        print ('+++++++++++++++++++++++++++++++')

        for grp in isin_group[group_name]:
            if isin_group[group_name][grp]['count'] > 10:

                isin_group[group_name][grp]['types'] = []

                for isin in isin_group[group_name][grp]['isin']:              
                    time.sleep(random.uniform(1, 3))       
                    entityType, entitySubType, issuer_name, market_name, ret = get_onvista_data(isin)
                    print(isin, ' = ', issuer_name, ' -> ', entityType, entitySubType) 
                    item = {'isin': isin, 'issuer_name':issuer_name, 'entityType': entityType, 'entitySubType': entitySubType}
                    isin_group[group_name][grp]['types'].append(item)
                    pass

            print ('grp:', grp, 'count:', isin_group[group_name][grp]['count'], isin_group[group_name][grp]['isin'])
            print ('-------------------------------')

    return isin_group

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


# ------------------------------------------------------------------------------------
# show_isin_stats
# ------------------------------------------------------------------------------------
def show_isin_stats():

    isin_grp_dict = get_all_isin_groups()
    groups = get_isin_group_keys()
    isin_group = {}

    invalid_isin_list = []

    for grp_name in groups:  

        isin_dict = isin_grp_dict[grp_name]['isin_dict']
        print(grp_name, 'len:', len(isin_dict))

        isin_group[grp_name] = {}

        country_isin = {}
        currency_isin = {}

        for isin in isin_dict:

            valid_isin = validate_isin(isin)
            if not valid_isin: invalid_isin_list.append(isin)

            country = isin[0:2]
            currency = isin_dict[isin]['c']

            obj = country_isin.get(country)
            if obj: obj['count'] += 1
            else: country_isin[country] = {'count': 1}

            c_key = country + '-' + currency
            obj = currency_isin.get(c_key)
            if obj: obj['count'] += 1
            else: currency_isin[c_key] = {'count': 1}

            if country == 'DE':
                group = isin[5:7]
                obj = isin_group[grp_name].get(group)
                
                if obj: 
                    obj['count'] += 1
                    list_len = len(obj['isin'])
                    if list_len < 20: obj['isin'].append(isin) 
                    elif random.uniform(0, 20): 
                        rand_idx = random.randint(0, list_len-1)
                        obj['isin'][rand_idx] = isin

                else: 
                    isin_group[grp_name][group] = {'count': 1, 'isin':[isin]}


        for country in country_isin:
            print ('country:', country, ', count:', country_isin[country]['count'])

        for c_key in currency_isin:
            print ('c_key:', c_key, ', count:', currency_isin[c_key]['count'])
        
        print ('-' * 60)

    #list isin groups
    for grp_name in isin_group:

        if grp_name == None: continue
        line_len = 52*2+2+len(grp_name)
        print('-' * line_len)
        print(' ' * 52, grp_name, ' ' * 52)
        print('-' * line_len)

        for grp in isin_group[grp_name]:
            count = isin_group[grp_name][grp]['count']
            isin = isin_group[grp_name][grp]['isin']
            print(f'grp: {grp} count: {count:>15}')

    print ('=' * 60)

    invalid_len = len(invalid_isin_list)
    print ('INVALID ISIN, count: ', invalid_len)
    if invalid_len < 10: print (invalid_isin_list)

    return isin_group


# ------------------------------------------------------------------------------------
# isin_group_html_output
# ------------------------------------------------------------------------------------
def isin_group_html_output(isin_group):

    out_path = '../isin.group.html'

    styles = '<head><style>' \
            'body {background-color: #222222; color: #888888; font-family: "Lucida Console", "Courier New", "DejaVu Sans Mono", monospace;}' \
            'hr {border: 2px solid #444444}' \
            'table, td, th {border:1px solid #333333; border-collapse: collapse;}' \
            'td, th {padding-right:20px; padding:3px}' \
            'a {color: #8888aa}' \
            '</style></head>'

    script = ''

    html = ''

    for group_name in isin_group:

        print('group_name:', group_name)
        print('-' * 60)
        html += f'<h1> {group_name} </h1>'

        for grp in isin_group[group_name]:
            
            data = isin_group[group_name][grp]
            count = data['count']
            isin = data['isin']

            out_detail_stats = f'<h3> {grp}: {count} </h3>'

            #types -> list of [{'isin': isin, 'issuer_name':issuer_name, 'entityType': entityType, 'entitySubType': entitySubType}]
            types = data.get('types')
            types_len = None
            if types is not None: types_len = len(types)

            print(f'grp: {grp}, count: {count:>8}, types-len: {types_len}')

            html += f'<h3>{grp}: {count}</h3>'

            if types is not None:
                html += '<table> <th>isin_type</th> <th>issuer_name</th> <th>entityType</th> <th>entitySubType</th> '
                for isin_type in types:

                    isin = isin_type['isin']
                    issuer_name = isin_type['issuer_name']
                    entityType = isin_type['entityType']
                    entitySubType = isin_type['entitySubType']

                    link = isin
                    if entityType == 'STOCK': link = f'<a href="https://www.onvista.de/aktien/{isin}" target="_blank">{isin}</a>'

                    html += f'<tr> <td>{link}</td> <td>{issuer_name}</td> <td>{entityType}</td> <td>{entitySubType}</td> </tr>'

                html += '</table>'

        html += '<hr>'



    f = open(out_path, 'wt')    
    f.write(styles)
    f.write(html)
    f.write(script)
    f.close()
    open_file (out_path)


# ==================================================================

path = '../isin.stats.pickle.zip'
isin_group = show_isin_stats()

#init_request()
#isin_group = check_isin_groups(isin_group)
#save_as_pickle(path, isin_group)

#isin_group = load_from_pickle(path)
#isin_group_html_output(isin_group)
