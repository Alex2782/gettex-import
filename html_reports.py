from utils import *
from isin_groups import *
from open_html import *
from stats import *


# ------------------------------------------------------------------------------------
# get_styles
# ------------------------------------------------------------------------------------
def get_styles(title = ''):

    styles = '<head><style>' \
            'body {background-color: #222222; color: #888888; font-family: "Lucida Console", "Courier New", "DejaVu Sans Mono", monospace;}' \
            'hr {border: 2px solid #444444}' \
            'table, td, th {border: 1px solid #333333; border-collapse: collapse;}' \
            'td, th {padding-right: 20px; padding: 3px}' \
            'a, caption {color: #8888aa}' \
            '.fade {color: #555555} '\
            '.r {text-align: right}' \
            '.c {text-align: center}' \
            '.bid {background-color: #0c3003}' \
            '.ask {background-color: #400303}' \
            f'</style> <title>{title}</title> </head>'
    
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
# isin_trades_html_output
# ------------------------------------------------------------------------------------
def isin_trades_html_output(output_data, isin):

    out_path = f'../{isin}.trades.html'

    locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')

    styles = get_styles(isin)
    script = ''
    html = ''

    pretrade = output_data['pretrade']
    extra = output_data['extra']
    posttrade = output_data['posttrade']

    #TODO pretrade

    html += '<table style="float:left">' 
    html += '<tr> <th class="c" colspan="7">file</th> </tr>'
    html += '<tr> <th>counter</th> <th>open</th> <th>high</th> <th>no bid / ask</th> <th>vola_profit</th> <th>bid_long</th> <th>ask_long</th></tr>'
    html += '<tr> <th>       </th> <th>close</th> <th>low</th> <th>no post     </th> <th>           </th> <th>bid_short</th><th>ask_short</th></tr>'

    for e in extra:
        tmp = extra_list_to_dict(e)
        #counter, open,high,low,close, no-pre-bid,no-pre-ask,no-post, vola_profit, bid_long,bid_short,ask_long,ask_short
        file = e[-1]

        #html += str(tmp)
        counter = locale.format_string("%d", tmp["counter"], True, True)
        p_open = locale.format_string("%.3f", tmp["open"], True, True)
        p_high = locale.format_string("%.3f", tmp["high"], True, True)
        p_low = locale.format_string("%.3f", tmp["low"], True, True)
        p_close = locale.format_string("%.3f", tmp["close"], True, True)
        no_pre_bid = locale.format_string("%d", tmp["no-pre-bid"], True, True)
        no_pre_ask = locale.format_string("%d", tmp["no-pre-ask"], True, True)
        no_post = locale.format_string("%d", tmp["no-post"], True, True)
        vola_profit = locale.format_string("%.3f", tmp["vola_profit"], True, True)
        bid_long = locale.format_string("%.3f", tmp["bid_long"], True, True)
        bid_short = locale.format_string("%.3f", tmp["bid_short"], True, True)
        ask_long = locale.format_string("%.3f", tmp["ask_long"], True, True)
        ask_short = locale.format_string("%.3f", tmp["ask_short"], True, True)

        html += f'<tr><td class="fade c" colspan="7">{file}</td></tr>'

        html += f'<tr> <td class="r">{counter}</td> <td class="r">{p_open}</td> <td class="r">{p_high}</td> ' \
                f'<td class="c">{no_pre_bid} / {no_pre_ask}</td> <td class="r">{vola_profit}</td> ' \
                f'<td class="r">{bid_long}</td> <td class="r">{ask_long}</td> </tr>'
        
        html += f'<tr> <td> </td> <td class="r">{p_close}</td> <td class="r">{p_low}</td> '\
                f'<td class="c">{no_post}</td> <td></td> <td class="r">{bid_short}</td><td class="r">{ask_short}</td></tr>'        

    html += '</table>'


    html += '<table style="float:left">' 
    html += '<tr> <th>time</th> <th>price</th> <th>amount</th> <th>trade_type</th></tr>'

    sum_amount = 0
    sum_volume = 0
    sum_bid_volume = 0
    sum_ask_volume = 0

    for post in posttrade:
        
        strtime = timestamp_to_strtime(post[0], post[1])
        tmp = strtime.split('.')
        strtime = f'{tmp[0]}<span class="fade">.{tmp[1]}</span>'

        price = locale.format_string("%.3f", post[2], True, True)
        amount = locale.format_string("%d", post[3], True, True)

        sum_amount += post[3]

        volume = post[2] * post[3]
        sum_volume += volume

        trade_type = 'unknown'
        if post[4] == 1: 
            trade_type = 'ask'
            sum_bid_volume += volume            
        elif post[4] == 2:
            trade_type = 'bid'
            sum_ask_volume += volume

        html += f'<tr class="{trade_type}"> <td>{strtime}</td> <td class="r">{price}</td> <td class="r">{amount}</td> <td class="c">{trade_type}</td></tr>'

    sum_amount = locale.format_string("%d", sum_amount, True, True)

    bid_p = locale.format_string("%.2f", sum_bid_volume / sum_volume * 100, True, True)
    ask_p = locale.format_string("%.2f", sum_ask_volume / sum_volume * 100, True, True)

    sum_bid_volume = locale.format_string("%.2f", sum_bid_volume, True, True)
    sum_ask_volume = locale.format_string("%.2f", sum_ask_volume, True, True)

    out_bid_ask = f'<span class="bid">{sum_bid_volume} <span class="fade">({bid_p}%)</span></span>' \
                  f'<span class="ask">{sum_ask_volume} <span class="fade">({ask_p}%)</span></span>'

    sum_volume = locale.format_string("%.2f", sum_volume, True, True)

    html += f'<tr> <td></td> <td class="r">{sum_volume}</td> <td class="r">{sum_amount}</td> <td class="c">{out_bid_ask}</td></tr>'  
    html += '</table>' 


    f = open(out_path, 'wt')    
    f.write(styles)
    f.write(html)
    f.write(script)
    f.close()
    open_file (out_path)

    pass

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

        if len(volume_day_stats) > 1:
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
    from_date = '2023-03-13'
    number_of_days = 90
    output_top = 10
    selected_group = False  #options: False, None, 'HSBC', 'Goldman_Sachs', 'UniCredit'
    #sum_volume_stats, volume_day_stats = analyze_volume(path, from_date, number_of_days, output_top, selected_group)
    #volume_html_output(sum_volume_stats, volume_day_stats, output_top)

    
    isin = 'DE000HG832C8'
    output_path = f'../{isin}.pickle.zip'

    #output_data = get_isin_trades(path, from_date, number_of_days, isin)
    #save_as_pickle(output_path, output_data)
    output_data = load_from_pickle(output_path)
    isin_trades_html_output(output_data, isin)


    #path = '../munc.isin.pickle.zip'
    #munc_isin_dict = load_from_pickle(path)
    #munc_isin_html_output(munc_isin_dict)

