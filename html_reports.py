from utils import *
from isin_groups import *
from open_html import *


# ------------------------------------------------------------------------------------
# get_styles
# ------------------------------------------------------------------------------------
def get_styles():

    styles = '<head><style>' \
            'body {background-color: #222222; color: #888888; font-family: "Lucida Console", "Courier New", "DejaVu Sans Mono", monospace;}' \
            'hr {border: 2px solid #444444}' \
            'table, td, th {border:1px solid #333333; border-collapse: collapse;}' \
            'td, th {padding-right:20px; padding:3px}' \
            'a {color: #8888aa}' \
            '</style></head>'
    
    return styles

# ------------------------------------------------------------------------------------
# isin_group_html_output
# ------------------------------------------------------------------------------------
def isin_group_html_output(isin_group):

    out_path = '../isin.group.html'

    styles = get_styles()
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


# ------------------------------------------------------------------------------------
# munc_isin_html_output
# ------------------------------------------------------------------------------------
def munc_isin_html_output(munc_isin_dict):

    out_path = '../munc.isin.html'

    styles = get_styles()
    script = ''
    html = ''

    html += '<table> <th>isin</th> <th>name</th> <th>entityType</th> <th>entitySubType</th> <th>issuer_name</th> <th>market_name</th> '

    for isin in munc_isin_dict:
        d = munc_isin_dict[isin][0]

        entityType = d[0]
        entitySubType = d[1]
        issuer_name = d[2]
        market_name = d[3]
        json = d[4]

        name = ''
        url = ''
        if json is not None:

            try:
                ret = json['search']['facets'][0]['results'][0]
                name = ret['name']
                url = ret['urls']['WEBSITE']
            except Exception as e:
                #print (e)
                pass
        
        isin_link = isin
        if url != '': isin_link = f'<a href="{url}" target="_blank"> {isin} </a>'

        name_out = name
        if entityType == 'STOCK': name_out = f'<span style="color:#aaaa55">{name}</span>'
        name_out += '<button id="copy-button">Copy</button>'

        html += f'<tr> <td>{isin_link}</td> <td>{name_out}</td> <td>{entityType}</td> ' \
                f'<td>{entitySubType}</td> <td>{issuer_name}</td> <td>{market_name}</td> </tr>'

    html += '</table>'

    f = open(out_path, 'wt')    
    f.write(styles)
    f.write(html)
    f.write(script)
    f.close()
    open_file (out_path)
    
#=========================================================================

path = '../munc.isin.pickle.zip'
munc_isin_dict = load_from_pickle(path)
munc_isin_html_output(munc_isin_dict)

