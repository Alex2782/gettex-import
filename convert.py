import datetime
from utils import *
from isin_groups import *
from open_html import *

# ------------------------------------------------------------------------------------
# pretrade_debug
# ------------------------------------------------------------------------------------
def pretrade_debug(path, debug_isin = None, debug_time = None, output_content = False, 
                   debug_bid_size = None, debug_ask_size = None, debug_min_price = None, debug_max_price = None):

    print('pretrade_debug:', path, debug_isin, debug_time)

    total = None

    output = []
    isin_counter = {}
    file_out = f'<h2>{path}</h2>'

    with gzip.open(path, 'rt') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):
            
            #empty isin
            if line[0:1] == ',':
                tmp = line.split(',')
                line_isin = ''
                if debug_time is not None: line_time = tmp[1][0:len(debug_time)]
            else:
                line_isin = line[0:12]
                if debug_time is not None: line_time = line[13:13+len(debug_time)]

            if debug_isin is not None and line_isin != debug_isin: continue
            if debug_time is not None and line_time != debug_time: continue

            if output_content:
                output.append (line)
                if isin_counter.get(line_isin) is None: isin_counter[line_isin] = 0 
                isin_counter[line_isin] += 1

            else:
                tmp = line.split(',')
                isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade(tmp)

                if debug_bid_size is not None and debug_bid_size != bid_size: continue
                if debug_ask_size is not None and debug_ask_size != ask_size: continue
                if debug_min_price is not None and debug_min_price > price: continue
                if debug_max_price is not None and debug_max_price < price: continue

                out = f"{isin:>15} | {tmp[1]:>15} | {bid:>8.3f} | {bid_size:>8} | {ask:>8.3f} | {ask_size:>8} | {spread:>8.3f} | {price:>8.3f}"
                output.append (out)

        # output ascii table
        str_format = "{:>15} | {:>15} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8}"
        if not output_content:
            out = str_format.format('isin', 'timestamp', 'bid', 'bid_size', 'ask', 'ask_size', 'spread', 'price')
            print (out)
            file_out += out  + "\n"

        print ("=" * 100)
        file_out += str('=' * 100) + "\n"

        for out in output:
            print(out)
            file_out += out + "\n"

        print ("=" * 100)
        file_out += str('=' * 100) + "\n"

        if len(isin_counter) > 0:
            for isin in isin_counter:
                out = f'{isin}: {isin_counter[isin]}'
                print (out)
                file_out += out + "\n"


    out_path = '../pretrade_debug.html'
    style = '<head><style>body{background-color: #222222; color: #888888}</style></head>'

    f = open(out_path, 'wt')    
    f.write(style)
    f.write('<code>{}</code>'.format(file_out.replace('\n', '<br>').replace('  ', '&nbsp;&nbsp;')))
    f.close()
    open_file (out_path)

# ------------------------------------------------------------------------------------
# posttrade_debug
# ------------------------------------------------------------------------------------
def posttrade_debug(path, debug_isin, debug_time):

    print('TODO')

# ------------------------------------------------------------------------------------
# cast_data_posttrade
# ------------------------------------------------------------------------------------
def cast_data_posttrade(arr):
    isin = arr[0]
    tm, seconds = strtime_to_timestamp(arr[1])
    currency = arr[2]

    price = float(arr[3])
    amount = int(float(arr[4]))

    #try:
    #    amount = int(arr[4], base=10)
    #except Exception as e:
    #    print('ERROR:', e, arr)

    return isin, tm, seconds, currency, price, amount


# ------------------------------------------------------------------------------------
# cast_data_pretrade
# ------------------------------------------------------------------------------------
def cast_data_pretrade(arr):
    isin = arr[0]
    tm, seconds = strtime_to_timestamp(arr[1])
    currency = arr[2]

    # cast to int and float, idx = 0 (timestamp) is already int
    ask = float(arr[3]) #ask
    ask_size = int(float(arr[4])) #ask_size
    bid = float(arr[5]) #bid
    bid_size = int(float(arr[6])) #bid_size

    if bid > 0 and ask > 0:
        #spread = round(ask - bid, 3)
        #price = round(bid + spread / 2, 3)
        #speed up, without round
        spread = bid - ask
        price = ask + spread / 2
    else: # 'bid' or 'ask' can be 0 values
        spread = 0
        price = ask + bid # shortener for price = ask or price = bid

    return isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price


# ------------------------------------------------------------------------------------
# pretrade_list_to_dict: convert list data to dictionary 
# ------------------------------------------------------------------------------------
def extra_list_to_dict(extra):
    # [counter, open,high,low,close, no-pre-bid,no-pre-ask,no-post, vola_profit, bid_long,bid_short,ask_long,ask_short, tmp_last_bid,tmp_last_ask]
    ret = {}
    ret['counter'] = extra[0]
    ret['open'] = extra[1]
    ret['high'] = extra[2]
    ret['low'] = extra[3]
    ret['close'] = extra[4]
    ret['no-pre-bid'] = extra[5]
    ret['no-pre-ask'] = extra[6]
    ret['no-post'] = extra[7]
    ret['vola_profit'] = extra[8]
    ret['bid_long'] = extra[9]
    ret['bid_short'] = extra[10]
    ret['ask_long'] = extra[11]
    ret['ask_short'] = extra[12]
    #ret['tmp_last_bid'] = extra[13]
    #ret['tmp_last_ask'] = extra[14]

    return ret

# ------------------------------------------------------------------------------------
# pretrade_list_to_dict: convert list data to dictionary 
# ------------------------------------------------------------------------------------
def pretrade_list_to_dict(trade_list):
    #idx 0 = timestamp in minutes of the day
    #idx 1, 2 = bid_size: max, min
    #idx 3, 4 = ask_size: max, min
    #idx 5, 6 = spread: max, min
    #idx 7, 8, 9, 10 = price: open, high, low, close
    #idx 11 = activity: count data
    #idx 12, 13 = volatility: long, short
    #idx 14, 15, 16 = volatility activity: long, short, equal (no changes)

    ret = {}
    ret['timestamp'] = trade_list[0]
    ret['ask_size_max'] = trade_list[1]
    ret['ask_size_min'] = trade_list[2]    
    ret['bid_size_max'] = trade_list[3]
    ret['bid_size_min'] = trade_list[4]
    ret['spread_max'] = trade_list[5]
    ret['spread_min'] = trade_list[6]
    ret['price_open'] = trade_list[7]
    ret['price_high'] = trade_list[8]
    ret['price_low'] = trade_list[9]
    ret['price_close'] = trade_list[10]
    ret['activity'] = trade_list[11]
    ret['volatility_long'] = trade_list[12]
    ret['volatility_short'] = trade_list[13]
    ret['volatility_activity_long'] = trade_list[14]
    ret['volatility_activity_short'] = trade_list[15]
    ret['volatility_activity_equal'] = trade_list[16]

    return ret


# ------------------------------------------------------------------------------------
# get_isin_groups_and_ignore_list 
# ------------------------------------------------------------------------------------
def get_isin_groups_and_ignore_list(group):

    isin_group = []
    ignore_isin = []

    if group and 'ISIN_GROUPS' in globals() and group in ISIN_GROUPS.keys():
        isin_group = ISIN_GROUPS.get(group)
        #print('isin_group:', isin_group)
    elif group: 
        group = None
        #print ('Hint: group was set to None') 

    #generate ignore-list, append all isin if     
    if not group and 'ISIN_GROUPS' in globals():
        ignore_isin += [item for sublist in ISIN_GROUPS.values() for item in sublist]
        #print ('ignore_isin - len:', len(ignore_isin))

    check_ignore = False
    if len(ignore_isin) > 0: check_ignore = True

    return set(isin_group), set(ignore_isin), check_ignore
    #return isin_group, ignore_isin, check_ignore


# ------------------------------------------------------------------------------------
# init_trade_data 
# ------------------------------------------------------------------------------------
def init_trade_data(isin_dict):
    return [[] for _ in range(len(isin_dict))]

# ------------------------------------------------------------------------------------
# init_trade_sub_lists
# ------------------------------------------------------------------------------------
def init_trade_sub_lists(isin_trade_data):

    len_data = len(isin_trade_data)

    # no data, init sub-lists
    if len_data == 0:
        # sub-list #0 is for pretrade row data
        isin_trade_data.append([]) 
        # sub-list #1 is for extra data and summary: 
        # [counter, open,high,low,close, no-pre-bid,no-pre-ask,no-post, vola_profit, bid_long,bid_short,ask_long,ask_short, tmp_last_bid,tmp_last_ask]
        isin_trade_data.append([0, 0,0,0,0, 0,0,0, 0, 0,0,0,0, 0,0])
        # new len 
        len_data = len(isin_trade_data)
        
    if len_data == 2:
        #sub-lists #2 is for posttrade row data
        isin_trade_data.append([]) 

# ------------------------------------------------------------------------------------
# isin_skip: continue / skip logic for 'read_gz_posttrade' and 'read_gz_pretrade' functions 
# ------------------------------------------------------------------------------------
def isin_skip(tmp_isin_grp, check_ignore, isin_group, ignore_isin):
    skip = False

    if check_ignore:
        if tmp_isin_grp in ignore_isin:
            skip = True
    else:
        if tmp_isin_grp not in isin_group:
            skip = True                    

    return skip

# ------------------------------------------------------------------------------------
# read_gz_posttrade 
# ------------------------------------------------------------------------------------
def read_gz_posttrade(path, isin_dict, market_type, group = None, trade_data = []):

    print(f'GROUP: {str(group):15} FILE SIZE: {get_file_sizeinfo(path):>12}, PATH: {path}')

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0
    no_amount_counter = 0

    if len(trade_data) == 0:
        trade_data = init_trade_data(isin_dict)

    total = None

    with gzip.open(path, 'rt') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):
 
            if market_type != 'munc' and isin_skip(line[0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            tmp = line.split(',')

            isin, tm, seconds, currency, price, amount = cast_data_posttrade(tmp)
            if amount == 0: no_amount_counter +=1

            #TODO clean up code later, same isin-code for posttrade and pretrade
            isin_obj = isin_dict.get(isin)
            if isin_obj is not None: 
                isin_idx = isin_dict[isin]['id']
            else:
                trade_data.append([])
                isin_idx = len(trade_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}

            init_trade_sub_lists(trade_data[isin_idx])

            if price == 0 or amount == 0:
                trade_data[isin_idx][1][7] += 1 # no-post
            else:
                trade_data[isin_idx][2].append([tm, seconds, price, amount, 0]) #idx(5) -> 0 = unknown, 1 = ask, 2 = bid


    return isin_dict, trade_data

# ------------------------------------------------------------------------------------
# read_gz_pretrade 
# ------------------------------------------------------------------------------------
def read_gz_pretrade(path, isin_dict, market_type, group = None, trade_data = []):
    """
    ### Parameters:
    - path: gettex pretrade file, gz format
    - isin_dict: dictionary loaded from isin.pickle
    - market_type: 'munc' or 'mund' from filename
    - group: if is None, ignore all isin from global dictionary 'ISIN_GROUPS' (file: isin_groups.py)
    - trade_data: list with posttrade data

    ### Returns:
    - dict: isin_dict
    - list: trade_data
    """

    print(f'GROUP: {str(group):15} FILE SIZE: {get_file_sizeinfo(path):>12}, PATH: {path}')

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0

    # faster dictionary, only id 
    #TODO clean up code later, same isin-code for posttrade and pretrade
    search_isin_dict = {}
    for isin in isin_dict:
        search_isin_dict[isin] =  isin_dict[isin]['id']

    if len(trade_data) == 0:
        trade_data = init_trade_data(isin_dict)

    total = None

    with gzip.open(path, 'rt') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):

            if market_type != 'munc' and isin_skip(line[0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            tmp = line.split(',')

            isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade(tmp)

            #very rare, isin may be blank: line: ,13:55:20.689694,EUR,0.99,150000,1.01,150000
            #if len(isin) < 12: print(f'ERROR ISIN len < 12: {isin}', 'line:', line) 

            trade = True
            bNewData = True
            len_data = 0

            #TODO clean up code later, same isin-code for posttrade and pretrade
            #isin_obj = isin_dict.get(isin)
            isin_obj = search_isin_dict.get(isin) #faster
            if isin_obj is not None: 
                #isin_idx = isin_dict[isin]['id']
                isin_idx = isin_obj #faster
            else:
                trade_data.append([])
                isin_idx = len(trade_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
                search_isin_dict[isin] = isin_idx #faster

            #extra data, no 'bid' or 'ask'
            if len (trade_data[isin_idx]) > 1:
                #print(trade_data[isin_idx])
                if bid == 0 or bid_size == 0: trade_data[isin_idx][1][5] += 1
                if ask == 0 or ask_size == 0: trade_data[isin_idx][1][6] += 1

            if bid == 0 and ask == 0 and bid_size == 0 and ask_size == 0: 
                continue #ignore 0 values
                #trade = False  
                #bNewData = False


            #idx 0 = timestamp in minutes of the day
            #idx 1, 2 = bid_size: max, min
            #idx 3, 4 = ask_size: max, min
            #idx 5, 6 = spread: max, min
            #idx 7, 8, 9, 10 = price: open, high, low, close
            #idx 11 = activity: count data
            #idx 12, 13 = volatility: long, short
            #idx 14, 15, 16 = volatility activity: long, short, equal (no changes)
            data = [tm,  bid_size,bid_size,  ask_size,ask_size,  spread,spread,  price,price,price,price,  1,  0,0,  0,0,1] 

            if trade:

                init_trade_sub_lists(trade_data[isin_idx])

                len_data = len(trade_data[isin_idx])
                if len_data == 3: #init posttrade bid / ask
                    init_posttrade_bid_ask(trade_data[isin_idx][2], tm, seconds, bid, ask)

                #extra data / summary
                tmp_open = trade_data[isin_idx][1][1]
                tmp_high = trade_data[isin_idx][1][2]
                tmp_low = trade_data[isin_idx][1][3]

                trade_data[isin_idx][1][0] += 1 #counter
                if tmp_open  == 0: trade_data[isin_idx][1][1] = price #open
                if price > tmp_high: trade_data[isin_idx][1][2] = price #high
                if price < tmp_low or tmp_low == 0: trade_data[isin_idx][1][3] = price #low
                trade_data[isin_idx][1][4] = price #close

                #init bid / ask vola
                # #9 = bid_long, #10 = bid_short, #11 = ask_long, #12 = ask_short, #13 = tmp_last_bid, #14 = tmp_last_ask 
                tmp_last_bid = trade_data[isin_idx][1][13]
                tmp_last_ask = trade_data[isin_idx][1][14]

                bid_vola = 0
                ask_vola = 0

                if tmp_last_bid > 0: bid_vola = bid - tmp_last_bid
                if tmp_last_ask > 0: ask_vola = ask - tmp_last_ask

                if bid_vola > 0: trade_data[isin_idx][1][9] += bid_vola # bid_long
                elif bid_vola < 0: trade_data[isin_idx][1][10] += bid_vola # bid_short
                
                if ask_vola > 0: trade_data[isin_idx][1][11] += ask_vola # bid_long
                elif ask_vola < 0: trade_data[isin_idx][1][12] += ask_vola # bid_short

                trade_data[isin_idx][1][13] = bid
                trade_data[isin_idx][1][14] = ask


                len_data = len(trade_data[isin_idx][0])

            if len_data > 0:
                last_data = trade_data[isin_idx][0][len_data - 1]

                if last_data[0] == tm:
                    bNewData = False

                    #bid_size, idx:1 = max, idx:2 = min
                    if data[1] > last_data[1]: last_data[1] = data[1]
                    if data[2] < last_data[2]: last_data[2] = data[2]
                    
                    #ask_size, idx:3 = max, idx:4 = min
                    if data[3] > last_data[3]: last_data[3] = data[3]
                    if data[4] < last_data[4]: last_data[4] = data[4]

                    if trade:
                        #spread, idx: 5 = max, idx: 6 = min
                        if data[5] > last_data[5]: last_data[5] = data[5]
                        if data[6] < last_data[6]: last_data[6] = data[6]
                        
                        #price: high(8), low(9), close(10)  -> hint: open(7) already initialized
                        if data[8] > last_data[8]: last_data[8] = data[8] #high
                        if data[9] < last_data[9]: last_data[9] = data[9] #low

                        # (close - prev_close) + (open - prev-close)
                        #volatility = (data[10] - last_data[10]) + (data[7] - last_data[10])
                        volatility = data[7] - last_data[10]
                        #volatility = data[10] - last_data[10]
                        last_data[10] = data[10] # close
                        
                        #activity
                        last_data[11] += 1

                        #idx 12, 13 = volatility long, short
                        #idx 14, 15, 16 = volatility activity long, short, equal (no changes)
                        if volatility > 0: 
                            last_data[12] = round(last_data[12] + volatility, 3)
                            last_data[14] += 1 
                        elif volatility < 0: 
                            last_data[13] = round(last_data[13] + volatility, 3)
                            last_data[15] += 1
                        else:
                            last_data[16] += 1
                        
                        #DEV-TEST
                        #if isin == 'DE000GT02HM8':
                        #    print ('activity -> long:', last_data[14], ' short:', last_data[15], ' equal:', last_data[16])


            if bNewData and trade:
                #DEV-TEST
                #if isin == 'DE000GT02HM8':
                #    print ('NEW activity -> long:', data[14], ' short:', data[15], ' equal:', data[16])

                trade_data[isin_idx][0].append(data) #row data
                
        #END for

    return isin_dict, trade_data

# ------------------------------------------------------------------------------------
# get_min_spread: 
# ------------------------------------------------------------------------------------
def get_min_spread(pre_trade_data):
    min_spread = 0

    for trade in pre_trade_data:
        if trade[6] < min_spread or min_spread == 0:
            min_spread = trade[6]

    # bid or ask = 0?
    if min_spread == 0:

        min_spread = 0.3
        tmp_count_null_bid_size = 0
        tmp_count_null_ask_size = 0

        for trade in pre_trade_data:
            if trade[2] == 0: tmp_count_null_bid_size += 1
            if trade[4] == 0: tmp_count_null_ask_size += 1

        #no 0 size, then min_spread = 0 (good spread)
        if tmp_count_null_bid_size == 0 and tmp_count_null_ask_size == 0: min_spread = 0

    return min_spread
# ------------------------------------------------------------------------------------
# summary_low_activity: 
# - without posttrade and low volatility, summary to 15 minutes
# - converting data, from list to tuple 
# ------------------------------------------------------------------------------------
def summary_low_activity(trade_data, isin_dict_idx, min_vola_profit = 0.1):

    #print('summary_low_activity, len:', len(trade_data))

    start = timeit.default_timer()

    summarized = 0
    isin_idx = -1

    for trade in trade_data:
        isin_idx += 1
        isin = isin_dict_idx[isin_idx]

        pre = []
        extra = []
        post = []
        data_len = len (trade)

        if data_len > 0: pre = trade[0]
        if data_len > 1: extra = trade[1]
        if data_len > 2: post = trade[2]

        min_spread = get_min_spread(pre)
        vola = 0
        vola_profit = 0
        count = 0

        if len(extra) > 0: 
            count = extra[0]
            vola = extra[2] - extra[3] # vola = high - low   

            # max. possible profit
            vola_profit = -999.0
            if extra[2] > 0:
                vola_profit = round((vola - min_spread * 2) / extra[2] * 100, 3) 

            extra[8] = vola_profit

            extra[9] = round(extra[9], 3) 
            extra[10] = round(extra[10], 3) 
            extra[11] = round(extra[11], 3)
            extra[12] = round(extra[12], 3)

            #remove tmp_last_bid and tmp_last_ask
            del extra[13]
            del extra[13]

        len_pre = len(pre)
        len_post = len(post)
        # summary to 15 minutes 
        if (len_post == 0 and vola_profit < min_vola_profit and len_pre > 1) or (len_pre > 1 and len_pre < 15 and len_post < 3):
            last_data = []

            summarized += len(pre)

            for data in pre:
                if len(last_data) == 0: 
                    last_data = data[:] # copy values
                    continue

                #bid_size, idx:1 = max, idx:2 = min
                if data[1] > last_data[1]: last_data[1] = data[1]
                if data[2] < last_data[2]: last_data[2] = data[2]
                
                #ask_size, idx:3 = max, idx:4 = min
                if data[3] > last_data[3]: last_data[3] = data[3]
                if data[4] < last_data[4]: last_data[4] = data[4]

                #spread, idx: 5 = max, idx: 6 = min
                if data[5] > last_data[5]: last_data[5] = data[5]
                if data[6] < last_data[6]: last_data[6] = data[6]
                
                #price: high(8), low(9), close(10)  -> hint: open(7) already initialized
                if data[8] > last_data[8]: last_data[8] = data[8]
                if data[9] < last_data[9]: last_data[9] = data[9]
                
                # (close - prev_close) + (open - prev-close)
                volatility = data[7] - last_data[10]
                last_data[10] = data[10]

                last_data[11] += data[11] # activity
                
                last_data[12] = round(last_data[12] + data[12], 3) # volatility_long
                last_data[13] = round(last_data[13] + data[13], 3) # volatility_short
                last_data[14] += data[14] # volatility_activity_long
                last_data[15] += data[15] # volatility_activity_short
                last_data[16] += data[16] # volatility_activity_equal

                if volatility > 0: 
                    last_data[12] = round(last_data[12] + volatility, 3)
                    last_data[14] += 1 
                    last_data[16] -= 1
                elif volatility < 0: 
                    last_data[13] = round(last_data[13] + volatility, 3)
                    last_data[15] += 1
                    last_data[16] -= 1

            #END for
            trade[0] = [last_data]

        # converting to tuple
        # pretrade
        idx = 0
        for data in pre:
            pre[idx] = tuple(data)
            idx += 1

        #extra
        if len(extra) > 0: trade[1] = tuple(trade[1])

        #posttrade
        idx = 0
        for data in post:
            post[idx] = tuple(data)
            idx += 1
        
    stop = timeit.default_timer()

    #print('created ret_data in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(trade_data), 'summarized:', summarized)

    return trade_data

# ------------------------------------------------------------------------------------
# init_posttrade_bid_ask
# ------------------------------------------------------------------------------------
def init_posttrade_bid_ask(posttrade_list, tm, seconds, bid, ask):

    for post in posttrade_list:
        if post[4] > 0: continue #
        if post[0] > tm and post[1] > seconds: break

        time_diff = post[0] * 60 + post[1] - tm * 60 - seconds

        if time_diff > -30.00 and time_diff <=0.1:
            price = post[2]
            abs_price_bid = abs(price - bid)
            abs_price_ask = abs(price - ask)

            if abs_price_bid > abs_price_ask: post[4] = 1 #ask
            elif abs_price_bid < abs_price_ask: post[4] = 2 #bid

# ------------------------------------------------------------------------------------
# get_filenames_from_mask
# ------------------------------------------------------------------------------------
def get_filenames_from_mask(file_mask):

    return [f'pretrade.{file_mask}.mund.csv.gz',
            f'pretrade.{file_mask}.munc.csv.gz',
            f'posttrade.{file_mask}.munc.csv.gz', 
            f'posttrade.{file_mask}.mund.csv.gz']

# ------------------------------------------------------------------------------------
# convert_files
# ------------------------------------------------------------------------------------
def convert_files(path, overwrite = False, file_mask = None):

    print ("convert_files, path:", path, "file_mask:", file_mask)

    start = timeit.default_timer()

    files = []
    if file_mask:
        search_files = get_filenames_from_mask(file_mask)
        for file in search_files:
            file_path = path + '/' + file
            if os.path.exists(file_path) and is_valid_gzip(file_path):
                files.append(file)
    else:
        files = list_gz_files(path, False, False)

    if len(files) > 0:
        #isin dictionary
        isin_grp_dict = get_all_isin_groups()
        groups = get_isin_group_keys()

    job_files = {}
    for file in files:

        tmp = file.split('.', 4)
        job_name = tmp[1] + '.' + tmp[2] + '.' + tmp[3]

        if not job_files.get(job_name): job_files[job_name] = []
        job_files[job_name].append(file)

    
    #key sort (job_name)
    job_files = dict(sorted(job_files.items(), key=lambda x: x[0]))

    # sort files -> post.XYZ.munc -> post.XYZ.mund -> pre.XYZ.munc -> pre.XYZ.mund
    # and convert files
    for job_name in job_files:
        
        #print ('JOBNAME:', job_name)
        job_files[job_name].sort(key=str.lower)

        for grp in groups:

            data_file = path + '/' + f'trade.{job_name}'
            if grp: data_file += f'.{grp}'
            data_file += '.pickle.zip'

            if overwrite == False and os.path.exists(data_file):
                print ("SKIP, already exists: ", data_file)
                continue

            arr = []
            isin_dict = isin_grp_dict[grp]['isin_dict']
            isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']

            for file in job_files[job_name]:

                tmp = file.split('.', 5)
                trade_type = tmp[0] # posttrade or pretrade
                market_type = tmp[4] # munc or mund

                #print ('file:', file, 'trade_type:', trade_type, 'market_type:', market_type)

                #'munc' files are small, do not group
                if market_type == 'munc' and grp != None:
                    #print (f"SKIP for grp = '{grp}'")
                    continue
                
                file_path = path + '/' + file

                if trade_type == 'posttrade':
                    isin_dict, arr = read_gz_posttrade(file_path, isin_dict, market_type, grp, arr)
                elif trade_type == 'pretrade':
                    isin_dict, arr = read_gz_pretrade(file_path, isin_dict, market_type, grp, arr)
            
            #END for file

            isin_dict_idx = get_isin_dict(isin_dict)
            isin_grp_dict[grp]['isin_dict_idx'] = isin_dict_idx
            isin_grp_dict[grp]['isin_dict'] = isin_dict

            save_isin_dict(isin_dict, grp)

            min_vola_profit = 0.1
            if grp != None: min_vola_profit = 3.0
            arr = summary_low_activity(arr, isin_dict_idx, min_vola_profit)

            save_as_pickle(data_file, arr)
            
            #free RAM (garbage collector) 
            del arr

        #END for grp    
                    
    #END for job_name   

    stop = timeit.default_timer()
    if len(files) > 0: show_runtime(f'{datetime.datetime.now()} - files converted in', start, stop)

# ===========================================================================================


if __name__ == '__main__':

    overwrite = True

    # SSD
    path = "../data_ssd"
    #convert_files(path, overwrite, '20230111.21.00')
    #convert_files('../data/2023-02-03', overwrite)

    # HDD
    path = "../data"
    sub_path = datetime.date.today().strftime('%Y-%m-%d')
    convert_files(path + '/' + sub_path)


    #file_mask = '20230214.14.00'
    #convert_files(path + '/2023-02-14', overwrite, file_mask)



