from read import *


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
            print(e, isin_data, 'isin_idx: ', isin_idx)
            isin_idx += 1
            continue
        
        isin_idx += 1
    #END for loop ---------------------------------------------------------------

    print(isin_list)
    print('=====================')
    print('isin_list len:', len(isin_list))



# ------------------------------------------------------------------------------------
# show_row_data
# ------------------------------------------------------------------------------------
def show_row_data(file, group, isin):
    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

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
        print(e, arr[isin_idx], 'isin_idx: ', isin_idx)

    print('=' * 59, isin, '=' * 59)

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
                f"bid long/short  : {e['bid_long']:> 9.3f} % / {e['bid_short']:> .3f} %\n" \
                f"ask long/short  : {e['ask_long']:> 9.3f} % / {e['ask_short']:> .3f} %\n" \
    
    print(extra_out)
    print('=' * 60, ' PRETRADE ', '=' * 60)
    vola_diff_sum = 0
    open_close_diff_sum = 0
    close_open_diff = 0 

    last_close = 0

    # output ascii table
    str_format = "{:>9} | {:>12} | {:>12} | {:>12} | {:>12} | {:>12} | {:>9} | {:>12} | {:>16} | {:>9}"
    print (str_format.format('timestamp', 'bid_size_max', 'ask_size_max', 'spread_max', 'price_open', 
                             'price_high', 'activity', 'vol.long', 'vol.activity', 'vola-diff'))
    
    print (str_format.format('         ', 'bid_size_min', 'ask_size_min', 'spread_min', 'price_close', 
                             'price_low', '        ', 'vol.short', 'long/short/equal', ''))
    print ('-' * 160)

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

        out = f"{'':>9} | {td['bid_size_min']:>12} | {td['ask_size_min']:>12} | {td['spread_min']:>12.3f} | {td['price_close']:>12.3f} | " \
                    f"{td['price_low']:>12.3f} | {'':>9} | {td['volatility_short']:>12.3f} | {'':^16} | {'':>16}"
        print (out)
    
    print('-' * 160)
    print('vola_diff_sum: ', round(vola_diff_sum, 3), 'open_close_diff_sum: ', round(open_close_diff_sum, 3), 'close_open_diff:', round(close_open_diff, 3))
    print('=' * 60, ' POSTTRADE ', '=' * 59)

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
    
    print('-' * 160)

    bid_p = bid_volume / volume * 100
    ask_p = ask_volume / volume * 100
    print(f"VOLUME     : {volume:>12.3f} {currency}")
    print(f"BID VOLUME : {bid_volume:>12.3f} {currency} ({bid_p:.3f} %)")
    print(f"ASK VOLUME : {ask_volume:>12.3f} {currency} ({ask_p:.3f} %)")

# ------------------------------------------------------------------------------------
# analyze_max_long_max_short
# ------------------------------------------------------------------------------------
def analyze_max_long_max_short(file, group, max_open_price = 3):

    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = 0
    max_vola_long = 0
    max_vola_short = 0
    isin_idx_max_vola_long = 0
    isin_idx_max_vola_short = 0

    for isin_data in arr:

        if len(isin_data) == 0: 
            isin_idx += 1
            continue

        try:
            d = isin_data[0]
            extra = isin_data[1]

            #skip price > 3 EURO 
            trade_dict = pretrade_list_to_dict(d[0])
            if trade_dict['price_open'] > max_open_price: 
                isin_idx += 1
                continue

        except Exception as e:
            print(e, isin_data, 'isin_idx: ', isin_idx)
            isin_idx += 1
            continue


        #if isin_idx != 37135:
        #    isin_idx += 1
        #    continue

        #print('isin:', isin_dict_idx[isin_idx], isin_idx)    
        #print('extra:', extra)


        sum_long = 0
        sum_short = 0
        sum_vola_long = 0
        sum_vola_short = 0
        sum_vola_open_close_long = 0
        sum_vola_open_close_short = 0

        prev_trade = False
        for trade in d:

            #print(timestamp_to_strtime(trade[0]), trade)
            sum_vola_long += trade[12]
            sum_vola_short += trade[13]

            open = trade[7]
            close = trade[10]

            if prev_trade :
                vola =  round(open - prev_trade[10], 3)
                if vola > 0: sum_vola_open_close_long += vola
                else: sum_vola_open_close_short += vola

            prev_trade = trade
        
        #print('----------------------------------------------')
        sum_vola_long = round(sum_vola_long, 3)
        sum_vola_short = round(sum_vola_short, 3)
        sum_vola_open_close_long = round(sum_vola_open_close_long, 3)
        sum_vola_open_close_short = round(sum_vola_open_close_short, 3)

        #print('sum_vola long / short           :', sum_vola_long,  sum_vola_short)
        #print('sum_vola_open_close long / short:', sum_vola_open_close_long, sum_vola_open_close_short)

        sum_long = round(sum_vola_open_close_long + sum_vola_long, 3)
        #print('sum_long:', sum_long)

        sum_short = round(sum_vola_open_close_short + sum_vola_short, 3)
        #print('sum_short:', sum_short)

        sum = round(sum_vola_open_close_long + sum_vola_long + sum_vola_open_close_short + sum_vola_short, 3)
        #print('sum:', sum)

        if sum_long > max_vola_long:
            #print ('sum_long > max_vola_long', sum_long, max_vola_long, ', isin_idx:', isin_idx)
            max_vola_long = sum_long
            isin_idx_max_vola_long = isin_idx

        if sum_short < max_vola_short:
            #print ('sum_short < max_vola_short', sum_short, max_vola_short, ', isin_idx:', isin_idx)
            max_vola_short = sum_short
            isin_idx_max_vola_short = isin_idx

        isin_idx += 1

        #if isin_idx > 10: break
        

    print('-----------------------------------------------------------------------')
    print ('MAX_LONG:', max_vola_long)
    print('isin:', isin_dict_idx[isin_idx_max_vola_long], 'isin_idx:', isin_idx_max_vola_long)    
    print('extra:', arr[isin_idx_max_vola_long][1])
    print ('=======================')
    d = arr[isin_idx_max_vola_long][0]

    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)

    print('------------------------')

    print ('MAX_SHORT:', max_vola_short)
    print('isin:', isin_dict_idx[isin_idx_max_vola_short], 'isin_idx:', isin_idx_max_vola_short)    
    print('extra:', arr[isin_idx_max_vola_short][1])
    print ('=======================')
    d = arr[isin_idx_max_vola_short][0]

    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)

    print('------------------------')

#================================================================================================


#TODO: genauer überprüfen, Zusammenfassung möglich?
group = 'Goldman_Sachs'
file = f'../data.{group}.pickle.zip'

group = None
file = f'../data.pickle.zip'

#analyze_max_long_max_short(file, group, 2)

min_count = 100
max_count = 999
max_vola = 999

#min / max len
min_post_trade = 1
max_post_trade = 9999

min_post_trade_amount = 1
post_trade_type = None   # None = no filter, 0 = unknown, 1 = bid, 2 = ask
#analyze_activity(file, group, min_count, max_count, max_vola, min_post_trade, max_post_trade, min_post_trade_amount, post_trade_type)


# TEST Data
# 'US29355A1079', 'US15130G7097', 'US55354G1004', 'DE000A2QDNX9', 'IT0003132476',
isin = 'DE000A2QDNX9'
show_row_data(file, group, isin)


debug_gz = '../data/pretrade.20230201.08.15.mund.csv.gz'
time = None # '08:00'
#pretrade_debug(debug_gz, isin, time)


exit()
# Maximalen Wert aus der Spalte 'bid_size' ermitteln
max_bid_size = np.amax(x['bid_size'])
print("Maximaler Wert in bid_size:", max_bid_size)

# Maximalen Wert aus der Spalte 'ask_size' ermitteln
max_ask_size = np.amax(x['ask_size'])
print("Maximaler Wert in ask_size:", max_ask_size)


# Maximalen Wert aus der Spalte 'bid' ermitteln
max_bid_size = np.amax(x['bid'])
print("Maximaler Wert in bid:", max_bid_size)

# Maximalen Wert aus der Spalte 'ask' ermitteln
max_ask_size = np.amax(x['ask'])
print("Maximaler Wert in ask:", max_ask_size)


max_bid_index = np.argmax(x['bid'])
isin_idx = x[max_bid_index]['isin_idx']
print("Maximaler Wert in bid:", x[max_bid_index]['bid'], 'isin_idx:', isin_idx)
print("ISIN:", isin_dict_idx.get(isin_idx))

max_ask_index = np.argmax(x['ask'])
isin_idx = x[max_ask_index]['isin_idx']
print("Maximaler Wert in ask:", x[max_ask_index]['ask'], 'isin_idx:', isin_idx)
print("ISIN:", isin_dict_idx.get(isin_idx))

indices = np.where(x['isin_idx'] == 44)
print('data isin_idx = 44:', x[indices])