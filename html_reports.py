from utils import *
from isin_groups import *
from open_html import *
from stats import *


# ------------------------------------------------------------------------------------
# get_styles
# ------------------------------------------------------------------------------------
def get_styles():

    styles = '<head><style>' \
            'body {background-color: #222222; color: #888888; font-family: "Lucida Console", "Courier New", "DejaVu Sans Mono", monospace;}' \
            'hr {border: 2px solid #444444}' \
            'table, td, th {border:1px solid #333333; border-collapse: collapse;}' \
            'td, th {padding-right:20px; padding:3px}' \
            'a, caption {color: #8888aa}' \
            '.r {text-align:right}' \
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
# volume_html_output
# ------------------------------------------------------------------------------------
def volume_html_output(sum_volume_stats, volume_day_stats, output_top = 100):

    out_path = '../volume.html'

    isin_grp_dict = get_all_isin_groups()
    groups = get_isin_group_keys()

    styles = get_styles()
    script = ''
    html = ''

    for grp in groups:
        
        if sum_volume_stats.get(grp) is None: continue

        html += f'<h1 style="clear:both">GROUP: {grp}</h1>'
        isin_dict = isin_grp_dict[grp]['isin_dict']

        
        html += '<table style="float:left; margin-right:10px"> <th>isin</th> <th>volume</th> '
        html += f'<caption>TOTAL</caption>'

        for isin, volume in sum_volume_stats[grp][:output_top]:
            formatted_volume = locale.format_string("%20.2f", volume, True, True)
            currency = isin_dict[isin]['c']
            formatted_volume += ' ' + currency

            html += f'<tr> <td>{isin}</td> <td class="r">{formatted_volume}</td> </tr>'

        html += '</table>'

        for date in volume_day_stats:

            html += '<table style="float:left"> <th>isin</th> <th>volume</th> '
            html += f'<caption>DATE: {date}</caption>'

            for isin, volume in volume_day_stats[date][grp][:output_top]:
                formatted_volume = locale.format_string("%20.2f", volume, True, True)
                currency = isin_dict[isin]['c']
                formatted_volume += ' ' + currency

                html += f'<tr> <td>{isin}</td> <td class="r">{formatted_volume}</td> </tr>'

            html += '</table>'

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

if __name__ == '__main__':

    path = '../data/'
    from_date = '2023-01-13'
    number_of_days = 90
    output_top = 100
    selected_group = None  #options: False, None, 'HSBC', 'Goldman_Sachs', 'UniCredit'
    sum_volume_stats, volume_day_stats = analyze_volume(path, from_date, number_of_days, output_top, selected_group)
    volume_html_output(sum_volume_stats, volume_day_stats, output_top)


    #path = '../munc.isin.pickle.zip'
    #munc_isin_dict = load_from_pickle(path)
    #munc_isin_html_output(munc_isin_dict)

