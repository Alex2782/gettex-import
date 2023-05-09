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
            'table {margin-right: 20px;}' \
            'td, th {padding-right: 20px; padding: 3px}' \
            'a, caption {color: #8888aa}' \
            '.fade {color: #555555} '\
            '.r {text-align: right}' \
            '.c {text-align: center}' \
            '.bid {background-color: #0c3003}' \
            '.ask {background-color: #400303}' \
            '.block {width: 100%}' \
            '.pretrade, .pretrade_mod0 {background-color: #26293b}' \
            '.pretrade_mod1 {background-color: #202335}' \
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
# format_int
# ------------------------------------------------------------------------------------
def format_int(number):
    return locale.format_string("%d", number, True, True)

# ------------------------------------------------------------------------------------
# format_float
# ------------------------------------------------------------------------------------
def format_float(number, f = '%.3f'):
    return locale.format_string(f, number, True, True)

# ------------------------------------------------------------------------------------
# isin_trades_html_output
# ------------------------------------------------------------------------------------
def isin_trades_html_output(output_data, isin):

    out_path = f'../{isin}.trades.html'

    locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')

    styles = get_styles(isin)
    script = ''
    html = ''

    for str_date in output_data:

        html += f'<h1>{str_date}</h1>'


        html += '<table style="float:left">' 
        #HEADER for extra-data
        html += '<tr> <th class="c" colspan="7">file</th> </tr>'
        html += '<tr> <th>counter</th> <th>open</th> <th>high</th> <th>no bid / ask</th> <th>vola_profit</th> <th>bid_long</th> <th>ask_long</th></tr>'
        html += '<tr> <th>       </th> <th>close</th> <th>low</th> <th>no post     </th> <th>           </th> <th>bid_short</th><th>ask_short</th></tr>'
        #HEADER for pretrade
        html += '<tr class="pretrade"> <th class="c" colspan="7">PRETRADE</th> </tr>'
        html += '<tr class="pretrade"> <th>time</th>     <th>open</th> <th>high</th> <th>spread_max</th> <th>volatility long/short </th> <th>bid_size max.</th> <th>ask_size max.</th></tr>'
        html += '<tr class="pretrade"> <th>activity</th> <th>close</th> <th>low</th> <th>spread_min</th> <th>volatility activity<br>long/short/equal </th> <th>bid_size min.</th><th>ask_size min.</th></tr>'


        pretrade = output_data[str_date]['pretrade']
        extra = output_data[str_date]['extra']
        posttrade = output_data[str_date]['posttrade']

        pretrade_idx = 0

        for e in extra:
            tmp = extra_list_to_dict(e)
            #counter, open,high,low,close, no-pre-bid,no-pre-ask,no-post, vola_profit, bid_long,bid_short,ask_long,ask_short
            file = e[-1]
            tmp_file = file.split('.')

            file_date = tmp_file[1]
            file_hh = tmp_file[2]
            file_mm = tmp_file[3]
            file_grp = tmp_file[4]

            file_timestamp = datetime.datetime.strptime(f'{file_date} {file_hh}:{file_mm}', "%Y%m%d %H:%M").timestamp()

            if file_grp == 'pickle': file_grp = None

            timestamp, seconds = strtime_to_timestamp(f'{file_hh}:{file_mm}:00')

            counter = format_int(tmp["counter"])
            p_open = format_float(tmp["open"])
            p_high = format_float(tmp["high"])
            p_low = format_float(tmp["low"])
            p_close = format_float(tmp["close"])
            no_pre_bid = format_int(tmp["no-pre-bid"])
            no_pre_ask = format_int(tmp["no-pre-ask"])
            no_post = format_int(tmp["no-post"])
            vola_profit = format_float(tmp["vola_profit"])
            bid_long = format_float(tmp["bid_long"])
            bid_short = format_float(tmp["bid_short"])
            ask_long = format_float(tmp["ask_long"])
            ask_short = format_float(tmp["ask_short"])

            html += f'<tr><td class="fade c" colspan="7">{file}</td></tr>'

            html += f'<tr> <td class="r">{counter}</td> <td class="r">{p_open}</td> <td class="r">{p_high}</td> ' \
                    f'<td class="c">{no_pre_bid} / {no_pre_ask}</td> <td class="r">{vola_profit}</td> ' \
                    f'<td class="r">{bid_long}</td> <td class="r">{ask_long}</td> </tr>'
            
            html += f'<tr> <td></td> <td class="r">{p_close}</td> <td class="r">{p_low}</td> '\
                    f'<td class="c">{no_post}</td> <td></td> <td class="r">{bid_short}</td><td class="r">{ask_short}</td></tr>'    

            time_from = timestamp - 15 # 15 minutes
            time_to = timestamp

            # PRETRADE output
            tmp_pretrade = pretrade[pretrade_idx:pretrade_idx+15]
            for pre in tmp_pretrade:
                p = pretrade_list_to_dict(pre)
                p_timestamp = p['timestamp']
                if time_from < p_timestamp and p_timestamp >= time_to: break
                pretrade_idx += 1

                out_timestamp = timestamp_to_strtime(p_timestamp)

                ask_size_max = format_int(p['ask_size_max'])
                ask_size_min = format_int(p['ask_size_min'])
                bid_size_max = format_int(p['bid_size_max'])
                bid_size_min = format_int(p['bid_size_min'])

                spread_max = format_float(p["spread_max"])
                spread_min = format_float(p["spread_min"])

                price_open = format_float(p["price_open"])
                price_high = format_float(p["price_high"])
                price_low = format_float(p["price_low"])
                price_close = format_float(p["price_close"])

                activity = format_int(p['activity'])
                volatility_long = format_float(p["volatility_long"])
                volatility_short = format_float(p["volatility_short"])
                volatility_activity_long = format_int(p['volatility_activity_long'])
                volatility_activity_short = format_int(p['volatility_activity_short'])
                volatility_activity_equal = format_int(p['volatility_activity_equal'])

                out_vol = f'{volatility_long} / {volatility_short}'
                out_vol_activity = f'{volatility_activity_long} / {volatility_activity_short} / {volatility_activity_equal}'
                tr_class = f'pretrade_mod{pretrade_idx % 2}'

                html += f'<tr class="{tr_class}"> '\
                        f'<td class="r">{out_timestamp}</td> <td class="r">{price_open}</td> <td class="r">{price_high}</td> '\
                        f'<td class="r">{spread_max}</td> <td class="c">{out_vol}</td>'\
                        f'<td class="r">{bid_size_max}</td><td class="r">{ask_size_max}</td>'\
                        '</tr>' 
                   
                html += f'<tr class="{tr_class}"> '\
                        f'<td class="r">{activity}</td> <td class="r">{price_close}</td> <td class="r">{price_low}</td> '\
                        f'<td class="r">{spread_min}</td> <td class="c">{out_vol_activity}</td>'\
                        f'<td class="r">{bid_size_min}</td><td class="r">{ask_size_min}</td>'\
                        '</tr>'    



        html += '</table>'


        html += '<table style="float:left">' 
        html += '<tr> <th colspan=5>POSTTRADE</th></tr>'
        html += '<tr> <th>time</th> <th>price</th> <th>amount</th> <th>trade_type</th></tr>'

        sum_amount = 0
        sum_bid_amount = 0
        sum_ask_amount = 0

        sum_volume = 0
        sum_bid_volume = 0
        sum_ask_volume = 0

        for post in posttrade:
            
            strtime = timestamp_to_strtime(post[0], post[1])
            tmp = strtime.split('.')
            strtime = f'{tmp[0]}<span class="fade">.{tmp[1]}</span>'

            price = format_float(post[2])
            amount = format_int(post[3])

            sum_amount += post[3]

            volume = post[2] * post[3]
            sum_volume += volume

            trade_type = 'unknown'
            if post[4] == 1: 
                trade_type = 'ask'
                sum_ask_volume += volume
                sum_ask_amount += post[3]

            elif post[4] == 2:
                trade_type = 'bid'
                sum_bid_volume += volume
                sum_bid_amount += post[3]  


            html += f'<tr class="{trade_type}"> <td>{strtime}</td> <td class="r">{price}</td> <td class="r">{amount}</td> <td class="c">{trade_type}</td></tr>'

        sum_amount = format_int(sum_amount)
        sum_bid_amount = format_int(sum_bid_amount)
        sum_ask_amount = format_int(sum_ask_amount)

        bid_p = ask_p = 0
        if sum_volume > 0:
            bid_p = format_float(sum_bid_volume / sum_volume * 100, '%.2f')
            ask_p = format_float(sum_ask_volume / sum_volume * 100, '%.2f')

        sum_bid_volume = format_float(sum_bid_volume, '%.2f')
        sum_ask_volume = format_float(sum_ask_volume, '%.2f')

        sum_volume = format_float(sum_volume, '%04.2f')

        out_volume = '<table>' \
                    f'<tr><td class="bid r"> {sum_bid_volume} </td> <td class="fade"> {bid_p} % </td></tr>' \
                    f'<tr><td class="ask r"> {sum_ask_volume} </td> <td class="fade"> {ask_p} % </td></tr>' \
                    f'<tr><td class="r"> {sum_volume} </td><td></td></tr>' \
                    '</table>'
        
        out_amount = '<table>' \
                    f'<tr><td class="bid r"> {sum_bid_amount} </td></tr>' \
                    f'<tr><td class="ask r"> {sum_ask_amount} </td></tr>' \
                    f'<tr><td class="r"> {sum_amount} </td></tr>' \
                    '</table>'
        
        html += f'<tr> <td></td> <td class="r"></td> <td class="r">{out_amount}</td> <td class="r">{out_volume}</td></tr>'  
        html += '</table>' 

        html += '<hr style="clear:both">'

    #END for str_date




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
            formatted_volume = format_float(volume, '%20.2f')
            currency = isin_dict[isin]['c']
            formatted_volume += ' ' + currency

            html += f'<tr> <td>{isin}</td> <td class="r">{formatted_volume}</td> </tr>'

        html += '</table>'

        if len(volume_day_stats) > 1:
            for date in volume_day_stats:

                html += '<table style="float:left"> <th>isin</th> <th>volume</th> '
                html += f'<caption>DATE: {date}</caption>'

                for isin, volume in volume_day_stats[date][grp][:output_top]:
                    formatted_volume = format_float(volume, '%20.2f')
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
    number_of_days = 30
    output_top = 10
    selected_group = False  #options: False, None, 'HSBC', 'Goldman_Sachs', 'UniCredit'
    #sum_volume_stats, volume_day_stats = analyze_volume(path, from_date, number_of_days, output_top, selected_group)
    #volume_html_output(sum_volume_stats, volume_day_stats, output_top)

    
    #isin = 'US88160R1014' #Tesla  
    #isin = 'DE000HG832C8' #HSBC Knock-Out Produkte Long DAX
    #isin = 'CA82509L1076'
    isin = 'DE000HG976C3' # KO - Put Tesla
    #isin = 'US88160R1014' # Tesla

    output_path = f'../{isin}.pickle.zip'

    from_date = '2023-05-05'
    number_of_days = 1
    output_data = get_isin_trades(path, from_date, number_of_days, isin)
    save_as_pickle(output_path, output_data)
    output_data = load_from_pickle(output_path)
    isin_trades_html_output(output_data, isin)


    #path = '../munc.isin.pickle.zip'
    #munc_isin_dict = load_from_pickle(path)
    #munc_isin_html_output(munc_isin_dict)

