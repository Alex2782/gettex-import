import datetime
from convert import *
import locale


# ------------------------------------------------------------------------------------
# get_isin_trades
# ------------------------------------------------------------------------------------
def get_isin_trades(path, from_date, number_of_days, isin):
    print ("get_isin_trades, path:", path, 'isin:', isin)

    isin_grp_dict = get_all_isin_groups()
    groups = get_isin_group_keys()
    grp = ISIN_GROUPS_IDX.get(isin[0:7])
    print ('GROUP:', grp)
    isin_dict = isin_grp_dict[grp]['isin_dict']
    isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']

    if isin_dict.get(isin) is None: 
        print (f'ERROR: unknown isin {isin}')
        return

    isin_idx = isin_dict[isin]['id']
    start = timeit.default_timer()
    date = datetime.datetime.strptime(from_date, '%Y-%m-%d')

    #output_data = {'pretrade':[], 'extra':[], 'posttrade':[]}
    output_data = {}

    for i in range(number_of_days):

        str_date = f'{date.strftime("%Y-%m-%d")}'
        sub_path = str_date + '/'
        date += datetime.timedelta(days=1)
        tmp_path = path + sub_path

        if not os.path.exists(tmp_path): continue

        output_data[str_date] = {'pretrade':[], 'extra':[], 'posttrade':[]}

        files = list_zip_files(tmp_path, True, True)

        for filepath in tqdm(files, unit=' files', unit_scale=True, desc=str_date):
            
            basename = os.path.basename(filepath)
            tmp = basename.split('.')

            file_date = tmp[1]
            file_hh = tmp[2]
            file_mm = tmp[3]
            file_grp = tmp[4]
            
            if file_grp == 'pickle': file_grp = None
            if grp != file_grp: continue

            trade = load_from_pickle(filepath)
            if isin_idx >= len(trade): continue

            data = trade[isin_idx]
            
            if len(data) > 0:
                pretrade = data[0]
                extra = list(data[1])
                extra.append(basename) #append filename
                output_data[str_date]['pretrade'] += pretrade
                output_data[str_date]['extra'].append(extra)

            if len(data) == 3 and len (data[2]) > 0:
                
                posttrade = data[2]
                output_data[str_date]['posttrade'] += posttrade 



    stop = timeit.default_timer()
    show_runtime('volume read out in', start, stop)

    return output_data

# ------------------------------------------------------------------------------------
# analyze_volume
# ------------------------------------------------------------------------------------
def analyze_volume(path, from_date, number_of_days, output_top = 25, select_group = False):

    print ("analyze_volume, path:", path)

    #locale.setlocale(locale.LC_ALL, '')
    locale.setlocale(locale.LC_ALL, 'de_DE.UTF-8')

    isin_grp_dict = get_all_isin_groups()
    groups = get_isin_group_keys()

    start = timeit.default_timer()

    date = datetime.datetime.strptime(from_date, '%Y-%m-%d')

    sum_isin_volume_grp_stats = {}
    sum_isin_volume_grp_day_stats = {}

    for i in range(number_of_days):

        str_date = f'{date.strftime("%Y-%m-%d")}'
        sub_path = str_date + '/'
        date += datetime.timedelta(days=1)
        tmp_path = path + sub_path

        if not os.path.exists(tmp_path): continue

        files = list_zip_files(tmp_path, True, True)
        isin_volume_grp_stats = {}

        for grp in groups:

            if select_group != False and select_group != grp: continue

            isin_dict = isin_grp_dict[grp]['isin_dict']
            isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']

            print ('GROUP:', grp, 'PATH:', tmp_path)

            if isin_volume_grp_stats.get(grp) is None: isin_volume_grp_stats[grp] = {}
            if sum_isin_volume_grp_stats.get(grp) is None: sum_isin_volume_grp_stats[grp] = {}
            

            for filepath in tqdm(files, unit=' files', unit_scale=True):
                
                basename = os.path.basename(filepath)
                tmp = basename.split('.')

                file_date = tmp[1]
                file_grp = tmp[4]
                
                if file_grp == 'pickle': file_grp = None
                if grp != file_grp: continue

                trade = load_from_pickle(filepath)

                isin_idx = 0
                for data in trade:

                    isin = isin_dict_idx[isin_idx]
                    isin_idx += 1

                    if len(data) == 3 and len (data[2]) > 0:

                        if isin_volume_grp_stats[grp].get(isin) is None: isin_volume_grp_stats[grp][isin] = 0
                        if sum_isin_volume_grp_stats[grp].get(isin) is None: sum_isin_volume_grp_stats[grp][isin] = 0
                        
                        post = data[2]

                        for p in post:
                            isin_volume_grp_stats[grp][isin] += p[2] * p[3]
                            sum_isin_volume_grp_stats[grp][isin] += p[2] * p[3]
                        
            #END for filepath

            # reverse sorting
            isin_volume_grp_stats[grp] = sorted(isin_volume_grp_stats[grp].items(), key=lambda x: x[1], reverse=True)

            for isin, volume in isin_volume_grp_stats[grp][:output_top]:
                formatted_volume = locale.format_string("%20.2f", volume, True, True)
                currency = isin_dict[isin]['c']
                formatted_volume += ' ' + currency

                print (f'{isin}: {formatted_volume}')
            print ('-' * 20)

        sum_isin_volume_grp_day_stats[str_date] = isin_volume_grp_stats

        #END for grp

    # SUM output
    for grp in groups:

        if select_group != False and select_group != grp: continue

        print ('-'*20)
        print ('TOTAL GROUP:', grp)
        print ('-'*20)

        isin_dict = isin_grp_dict[grp]['isin_dict']        
        sum_isin_volume_grp_stats[grp] = sorted(sum_isin_volume_grp_stats[grp].items(), key=lambda x: x[1], reverse=True)

        for isin, volume in sum_isin_volume_grp_stats[grp][:output_top]:
            formatted_volume = locale.format_string("%20.2f", volume, True, True)
            currency = isin_dict[isin]['c']
            formatted_volume += ' ' + currency

            print (f'{isin}: {formatted_volume}')

    stop = timeit.default_timer()
    show_runtime('volume analyzed in', start, stop)

    return sum_isin_volume_grp_stats, sum_isin_volume_grp_day_stats

# ------------------------------------------------------------------------------------
# analyze_activity
# ------------------------------------------------------------------------------------
def analyze_activity(file, group, min_count = 1, max_count = 9999, max_vola = 9999, 
                    min_post_trade = 0, max_post_trade = 9999, min_post_trade_amount = 0, post_trade_type = None):

    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = 0
    isin_list = []

    for isin_data in arr:

        if len(isin_data) == 0: 
            isin_idx += 1
            continue

        try:
            pre_trade = isin_data[0]
            extra = isin_data[1]

            post_trade = []
            if len(isin_data) > 2:
                post_trade = isin_data[2]

            
            len_post_trade = len(post_trade)
            for post in post_trade:
                #amount
                if post[3] < min_post_trade_amount:
                    len_post_trade -= 1
                #type
                if post_trade_type is not None and post[4] != post_trade_type:
                    len_post_trade -= 1

            count = extra[0]
            open = extra[1]
            high = extra[2]
            low = extra[3]
            close = extra[4]

            vola = high - low

            #print (isin_dict_idx[isin_idx], 'vola:', vola, 'max_vola', max_vola)

            #skip min_count / max_count
            if count > max_count or count < min_count: 
                isin_idx += 1
                continue
            elif len_post_trade > max_post_trade or len_post_trade < min_post_trade: 

                isin_idx += 1
                continue
            elif vola < max_vola:
                #print ("APPEND")
                isin_list.append(isin_dict_idx[isin_idx])

        except Exception as e:
            print('ERROR:', e, isin_data, 'isin_idx: ', isin_idx)
            isin_idx += 1
            continue
        
        isin_idx += 1
    #END for loop ---------------------------------------------------------------

    print(isin_list)
    print('=====================')
    print('isin_list len:', len(isin_list))

    return isin_list


# ------------------------------------------------------------------------------------
# show_row_data
# ------------------------------------------------------------------------------------
def show_row_data(file, group, isin, out_path = None):
    #isin dictionary
    global isin_grp_dict
    global arr

    if not 'isin_grp_dict' in globals(): isin_grp_dict = get_all_isin_groups()
    if not 'arr' in globals(): arr = load_from_pickle(file)

    file_out = ''

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = isin_dict[isin]['id']
    currency = isin_dict[isin]['c']

    try:
        pre_trade = arr[isin_idx][0]
        extra = arr[isin_idx][1]

        post_trade = []
        if len(arr[isin_idx]) > 2:
            post_trade = arr[isin_idx][2]

    except Exception as e:
        print('ERROR:', e, arr[isin_idx], 'isin_idx: ', isin_idx)

    # ==================== EXTRA DATA ======================

    print('=' * 80, isin, '=' * 79)
    file_out += '{} {} {}'.format('=' * 80, isin, '=' * 79) + "\n"

    e = extra_list_to_dict(extra)
    oc_diff = round(e['close'] - e['open'], 3)
    oc_diff_p = round((e['close'] - e['open']) / e['close'] * 100, 3)

    hl_diff = round(e['high'] - e['low'], 3)  
    hl_diff_p = round((e['high'] - e['low']) / e['high'] * 100, 3)

    extra_out = f"counter         : {e['counter']:> 9} \n" \
                f"open            : {e['open']:> 9.3f} \n" \
                f"close           : {e['close']:> 9.3f} = {oc_diff:> 9.3f} ({oc_diff_p:.3f} %)\n" \
                f"high            : {e['high']:> 9.3f} \n" \
                f"low             : {e['low']:> 9.3f} = {hl_diff:> 9.3f} ({hl_diff_p:.3f} %)\n" \
                f"no-pre-bid      : {e['no-pre-bid']:> 9} \n" \
                f"no-pre-ask      : {e['no-pre-ask']:> 9} \n" \
                f"no-post         : {e['no-post']:> 9} \n" \
                f"vola_profit     : {e['vola_profit']:> 9.3f} %\n" \
                f"bid long/short  : {e['bid_long']:> 9.3f} / {e['bid_short']:> .3f} \n" \
                f"ask long/short  : {e['ask_long']:> 9.3f} / {e['ask_short']:> .3f} \n" \
    
    print(extra_out)

    # ==================== PRETRADE ======================

    print('=' * 80, ' PRETRADE ', '=' * 80)
    file_out += extra_out + "\n"
    file_out += '{} {} {}'.format('=' * 80, ' PRETRADE ', '=' * 80) + "\n"

    vola_diff_sum = 0
    open_close_diff_sum = 0
    close_open_diff = 0 

    last_close = 0

    # output ascii table
    str_format = "{:>9} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12} | {:>9} | {:>12} | {:>16} | {:>9}"
    out = str_format.format('timestamp', 'bid_size_max', 'ask_size_max', 'spread_max', 'price_open', 
                             'price_high', 'activity', 'vol.long', 'vol.activity', 'vola-diff')
    print (out)
    file_out += out + "\n"
    
    out = str_format.format('         ', 'bid_size_min', 'ask_size_min', 'spread_min', 'price_close', 
                             'price_low', '        ', 'vol.short', 'long/short/equal', '')
    print (out)
    file_out += out + "\n"
    print ('-' * 160)
    file_out += str('-' * 160) + "\n"


    for trade in pre_trade:
        vola_diff = round(trade[12] + trade[13], 3)
        vola_diff_sum += vola_diff
        open_close_diff = round(trade[10] - trade[7], 3)
        open_close_diff_sum += open_close_diff

        if last_close > 0: close_open_diff += trade[7] - last_close

        last_close = trade[10]
        td = pretrade_list_to_dict(trade)

        tm_out = timestamp_to_strtime(td['timestamp'])
        vol_activity = f"{td['volatility_activity_long']:^3} / {td['volatility_activity_short']:^3} / {td['volatility_activity_equal']:^3} "

        out = f"{tm_out:>9} | {td['bid_size_max']:>12} | {td['ask_size_max']:>12} | {td['spread_max']:>12.3f} | {td['price_open']:>12.3f} | " \
                    f"{td['price_high']:>12.3f} | {td['activity']:>9} | {td['volatility_long']:>12.3f} | {vol_activity:^16} | {vola_diff:>9.3f}"
        print (out)
        file_out += out + "\n"

        out = f"{'':>9} | {td['bid_size_min']:>12} | {td['ask_size_min']:>12} | {td['spread_min']:>12.3f} | {td['price_close']:>12.3f} | " \
                    f"{td['price_low']:>12.3f} | {'':>9} | {td['volatility_short']:>12.3f} | {'':^16} | {'':>16}"
        print (out)
        file_out += out + "\n"
    
    print('-' * 160)
    file_out += str('-' * 160) + "\n"

    DIFF = round(vola_diff_sum + close_open_diff, 3)
    out = f"vola_diff_sum       : {round(vola_diff_sum, 3):> 9.3f} \n" \
          f"open_close_diff_sum : {round(open_close_diff_sum, 3):> 9.3f} \n" \
          f"close_open_diff     : {round(close_open_diff, 3):> 9.3f} \n" \
          f"DIFF                : {DIFF:> 9.3f} " \
    
    print(out)   
    file_out += out + "\n"

    if DIFF - oc_diff != 0: 
        out = f"DIFF-ERROR:         : {DIFF-oc_diff:> 9.3f}"
        print(out) 
        file_out += out + "\n"

    # ==================== POSTTRADE ======================

    print('=' * 60, ' POSTTRADE ', '=' * 59)
    file_out += '{} {} {}'.format('=' * 60, ' POSTTRADE ', '=' * 59) + "\n"

    str_format = "{:>15} | {:>9} | {:>9} | {:>9} "
    out = str_format.format('timestamp', 'price', 'amount', 'type')
    print (out)
    file_out += out + "\n"

    print('-' * 160)
    file_out += str('-' * 160) + "\n"

    volume = 0
    bid_volume = 0
    ask_volume = 0

    for trade in post_trade:
        tm = timestamp_to_strtime(trade[0])
        extra_out = f"{tm:5}:{trade[1]:09.6f} | {trade[2]:>9.3f} | {trade[3]:>9} | {trade[4]:>9}"
        volume += trade[2] * trade[3]

        if trade[4] == 1: bid_volume += trade[2] * trade[3]
        elif trade[4] == 2: ask_volume += trade[2] * trade[3]

        print(extra_out)
        file_out += extra_out + "\n"
    
    print('-' * 160)
    file_out += str('-' * 160) + "\n"

    bid_p = 0
    ask_p = 0

    if volume > 0:
        bid_p = bid_volume / volume * 100
        ask_p = ask_volume / volume * 100

    out = f"VOLUME     : {volume:>12.3f} {currency} \n" \
          f"BID VOLUME : {bid_volume:>12.3f} {currency} ({bid_p:.3f} % \n" \
          f"ASK VOLUME : {ask_volume:>12.3f} {currency} ({ask_p:.3f} %) \n"

    print(out)
    file_out += out

    if out_path:
        
        style = ''
        if not os.path.exists(out_path):
            style = '<head><style>body{background-color: #222222; color: #888888}</style></head>'

        f = open(out_path, 'a')    
        if len(style) > 0: f.write(style)

        f.write('<code>{}</code> <hr style="border: 2px solid #5555aa">'.format(file_out.replace('\n', '<br>').replace('  ', '&nbsp;&nbsp;')))
        f.close()


# ------------------------------------------------------------------------------------
# analyze_max_long_max_short
# ------------------------------------------------------------------------------------
def analyze_len_data(file, group, max_open_price = 3):

    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = 0

    len_dict = {'err':0, '0': 0}

    for isin_data in arr:

        if len(isin_data) == 0: 
            isin_idx += 1
            len_dict['0'] += 1
            continue

        try:
            d = isin_data[0]
            extra = isin_data[1]
            key = str(len(d))
            if not len_dict.get(key): len_dict[key] = 0 
            len_dict[str(len(d))] += 1

        except Exception as e:
            print('ERROR:', e, isin_data, 'isin_idx: ', isin_idx)
            isin_idx += 1
            len_dict['err'] += 1
            continue

    print (len_dict)


#================================================================================================

if __name__ == '__main__':

    path = '../data/'
    from_date = '2023-03-13'
    number_of_days = 1
    output_top = 10
    selected_group = None  #options: False, None, 'HSBC', 'Goldman_Sachs', 'UniCredit'
    #sum_volume_stats, volume_day_stats = analyze_volume(path, from_date, number_of_days, output_top, selected_group)


    #TODO check 'DE000HB6HPZ6:         1.497.253,44 EUR'  
    #https://www.onvista.de/derivate/Optionsscheine/223755554-HB6HPZ-DE000HB6HPZ6


    #Daten prüfen: US5339001068, Preis plötzlich bei 212,50 Euro ? was steht im 'pretrade' und 'posttrade' ?
    #20230220,14,45,165.0,212.5,165.0,212.5,0,0,0,4,4,1,20.471,0,-1.0,96.0,0
    #20230220,15,00,212.5,212.5,164.0,164.0,0,0,0,65,65,0,20.941,0,0,0,-97.0

    debug_gz = '../data/2023-05-17/pretrade.20230517.13.45.mund.csv.gz'
    time = None # '08:00'
    #isin = 'DE000HG976C3' # KO - Put Tesla 50x
    isin = 'DE000HG9N8H0' # KO Tesla
    


    #isin = 'US88160R1014' # Tesla
    pretrade_debug(debug_gz, isin, time)

    #TODO Bugfix: price / vola_profit ...
    #../data/2023-02-20/pretrade.20230220.15.00.mund.csv.gz
    #        isin |       timestamp |      bid | bid_size |      ask | ask_size |   spread |    price
    #====================================================================================================
    #US5339001068 | 14:45:11.260829 |    0.000 |        0 |    0.000 |        0 |    0.000 |    0.000
    #US5339001068 | 14:45:11.547997 |    0.000 |        0 |    0.000 |        0 |    0.000 |    0.000
    #US5339001068 | 14:45:26.412211 |  262.000 |      100 |  163.000 |      100 |   99.000 |  212.500
    #US5339001068 | 14:45:31.418708 |  262.000 |      100 |  163.000 |      100 |   99.000 |  212.500
    #US5339001068 | 14:45:31.695911 |  262.000 |      100 |  163.000 |      100 |   99.000 |  212.500
    #US5339001068 | 14:45:32.099788 |  262.000 |      100 |  163.000 |      100 |   99.000 |  212.500
    #US5339001068 | 14:45:40.974181 |    0.000 |        0 |    0.000 |        0 |    0.000 |    0.000
    #US5339001068 | 14:45:51.350435 |    0.000 |        0 |    0.000 |        0 |    0.000 |    0.000
    #US5339001068 | 14:46:11.354488 |    0.000 |        0 |    0.000 |        0 |    0.000 |    0.000    





    #arr = load_from_pickle('../data/2023-03-13/trade.20230313.14.45.HSBC.pickle.zip')
    #isin_grp_dict = get_all_isin_groups()
    #isin_dict = isin_grp_dict['HSBC']['isin_dict']
    #isin_idx = isin_dict['DE000HG832C8']['id']
    #print ('DE000HG832C8:', isin_idx)
    #print (arr[isin_idx][1])



    exit()
    #TODO: genauer überprüfen, Zusammenfassung möglich?

    #path = "../data/2023-02-03/trade.20230203.11.15"
    path = "../data/trade.20230118.14.45"

    group = 'Goldman_Sachs'
    file = path + f'.{group}.pickle.zip'

    #group = None
    #file = path + '.pickle.zip'

    analyze_len_data(file, group)

    exit()
    min_count = 0
    max_count = 100
    max_vola = 2

    #min / max len
    min_post_trade = 0
    max_post_trade = 0

    min_post_trade_amount = 0
    post_trade_type = None   # None = no filter, 0 = unknown, 1 = ask, 2 = bid
    isin_list = analyze_activity(file, group, min_count, max_count, max_vola, min_post_trade, max_post_trade, min_post_trade_amount, post_trade_type)

    if group: out_path = f'../show_row_data.{group}.html' #None
    else: out_path = f'../show_row_data.html' #None
    #out_path = None


    #delete out_path content
    if out_path and os.path.exists(out_path):
        os.remove(out_path)

    isin = 'DE000A2QDNX9'
    #show_row_data (file, group, isin, out_path)

    counter = 0
    for isin in isin_list:
        show_row_data (file, group, isin, out_path)
        counter += 1
        if counter > 100: break
        
    if out_path: open_file (out_path)

    debug_gz = '../data/pretrade.20230201.08.15.mund.csv.gz'
    time = None # '08:00'
    #pretrade_debug(debug_gz, isin, time)