from utils import *
from isin_groups import *

# ---------------------------------------------------------------------------------
# get_isin_group_keys
# ---------------------------------------------------------------------------------
def get_isin_group_keys():
    return [None] + list(ISIN_GROUPS.keys())

# ---------------------------------------------------------------------------------
# get_all_isin_groups
# ---------------------------------------------------------------------------------
def get_all_isin_groups():

    groups = get_isin_group_keys()
    ret = {}

    for grp in groups:
        isin_dict, isin_dict_idx = load_isin_dict(grp)
        ret[grp] = {'isin_dict': isin_dict, 'isin_dict_idx': isin_dict_idx}

    return ret

# ---------------------------------------------------------------------------------
# get_isin_path
# ---------------------------------------------------------------------------------
def get_isin_path(group = None):

    path = './isin'
    if not os.path.exists(path): os.makedirs(path)

    filename = 'isin.pickle'
    if group: filename = f'isin.{group}.pickle'

    path += '/' + filename

    return path

# ---------------------------------------------------------------------------------
# save_isin_dict: save isin dictionary as pickle file
# ---------------------------------------------------------------------------------
def save_isin_dict(isin_dict, group = None):

    global _ISIN_SIZE_START
    if not '_ISIN_SIZE_START' in globals(): _ISIN_SIZE_START = {}

    path = get_isin_path(group)

    start = timeit.default_timer()

    if _ISIN_SIZE_START.get(group) != len(isin_dict):

        print('save ISIN, _ISIN_SIZE_START: ', _ISIN_SIZE_START.get(group), ', new len:', len(isin_dict)) 
        with open(path, 'wb') as f:
            pickle.dump(isin_dict, f)
        
        _ISIN_SIZE_START[group] = len(isin_dict)
    
    stop = timeit.default_timer()
    #print('saved isin_dict in: %.2f s' % (stop - start)) 

# ---------------------------------------------------------------------------------
# load_isin_dict
# ---------------------------------------------------------------------------------
def load_isin_dict(group = None):

    global _ISIN_SIZE_START
    if not '_ISIN_SIZE_START' in globals(): _ISIN_SIZE_START = {}

    path = get_isin_path(group)

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('load_isin_dict:', path, ', file size:', get_file_sizeinfo(path))

    isin_dict = {}

    start = timeit.default_timer()

    if os.path.exists(path):
        with open(path, 'rb') as f:
            isin_dict = pickle.load(f)

    _ISIN_SIZE_START[group] = len(isin_dict)

    stop = timeit.default_timer()
    print('loaded isin_dict in: %.2f s' % (stop - start), ', len:', _ISIN_SIZE_START.get(group), ', sizeof:', get_sizeof_info(isin_dict)) 


    # isin_dict_idx
    isin_dict_idx = get_isin_dict(isin_dict)
    print('----------------------------------------------------------')

    return isin_dict, isin_dict_idx

# ---------------------------------------------------------------------------------
# get_isin_dict
# ---------------------------------------------------------------------------------
def get_isin_dict(isin_dict):
    # isin_dict_idx
    isin_dict_idx = {}

    start = timeit.default_timer()
    for key, value in isin_dict.items():
        isin_dict_idx[value['id']] = key
    stop = timeit.default_timer()
    
    #print('created isin_dict_idx in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(isin_dict_idx) )
    return isin_dict_idx

# ------------------------------------------------------------------------------------
# pretrade_debug
# ------------------------------------------------------------------------------------
def pretrade_debug(path, debug_isin, debug_time = None):

    print('pretrade_debug:', path, debug_isin, debug_time)

    total = None #sum(1 for _ in gzip.open(path, 'rb'))

    output = []

    with gzip.open(path, 'rb') as f:

        #TODO: volatility (long, short, activity)
        last_price = 0
        vola_long = 0
        vola_short = 0

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade(tmp)

            if debug_isin == isin or (debug_time is not None and debug_time == timestamp_to_strtime(tm)):
                
                if last_price > 0:
                    vola = price - last_price
                    if vola > 0: vola_long += vola
                    elif vola < 0: vola_short += vola

                last_price = price

                out = f"{tmp[1]} | {bid:>8.3f} | {bid_size:>8} | {ask:>8.3f} | {ask_size:>8} | {spread:>8.3f} | {price:>8.3f}"
                output.append (out)

        # output ascii table
        str_format = "{:>15} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8}"
        print (str_format.format('timestamp', 'bid', 'bid_size', 'ask', 'ask_size', 'spread', 'price'))
        print ("=" * 81)

        for out in output:
            print(out)

        print ("=" * 81)
        print ('vola_long:', round(vola_long, 3), 'vola_short:', round(vola_short, 3), 'diff:', round(vola_long+vola_short, 3))
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
    bid = float(arr[3]) #bid
    bid_size = int(arr[4]) #bid_size
    ask = float(arr[5]) #ask
    ask_size = int(arr[6]) #ask_size

    if bid > 0 and ask > 0:
        spread = round(ask - bid, 3)
        price = round(bid + spread / 2, 3)
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
    #idx 17, 18 = no values counter: bid, ask  (price or size = 0)

    ret = {}
    ret['timestamp'] = trade_list[0]
    ret['bid_size_max'] = trade_list[1]
    ret['bid_size_min'] = trade_list[2]
    ret['ask_size_max'] = trade_list[3]
    ret['ask_size_min'] = trade_list[4]
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

    return isin_group, ignore_isin, check_ignore


# ------------------------------------------------------------------------------------
# init_trade_data 
# ------------------------------------------------------------------------------------
def init_trade_data(isin_dict):
    trade_data = []
    for x in range(0, len(isin_dict)):
        trade_data.append([])

    #print('empty list - trade_data len: ', len(trade_data))

    return trade_data

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

    #print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('read_gz_posttrade:', path, ', file size:', get_file_sizeinfo(path), ', group:', group)

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0
    no_amount_counter = 0

    if len(trade_data) == 0:
        trade_data = init_trade_data(isin_dict)

    total = None

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):
 
            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            if market_type != 'munc' and isin_skip(tmp[0][0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            isin, tm, seconds, currency, price, amount = cast_data_posttrade(tmp)
            if amount == 0: no_amount_counter +=1

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                trade_data.append([])
                isin_idx = len(trade_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']

            #DEV-Test
            #if isin == 'US5949181045':
            #    print(isin, timestamp_to_strtime(tm), price, amount)

            init_trade_sub_lists(trade_data[isin_idx])

            if price == 0 or amount == 0:
                trade_data[isin_idx][1][7] += 1 # no-post
            else:
                trade_data[isin_idx][2].append([tm, seconds, price, amount, 0]) #idx(5) -> 0 = unknown, 1 = bid, 2 = ask

    #print('pretrade_data len:', len(trade_data), ', no_amount_counter:', no_amount_counter)
    #print('----------------------------------------------------------')

    return isin_dict, trade_data

# ------------------------------------------------------------------------------------
# read_gz_pretrade 
# ------------------------------------------------------------------------------------
def read_gz_pretrade(path, isin_dict, market_type, group = None, trade_data = []):
    """
    ### Parameters:
    - path: gettex pretrade file, gz format
    - isin_dict: dictionary loaded from isin.pickle
    - group: if is None, ignore all isin from global dictionary 'ISIN_GROUPS' (file: isin_groups.py)

    ### Returns:
    - dict: isin_dict
    - list: ret_data
    """

    #print('+' * 60)
    print('read_gz_pretrade:', path, ', file size:', get_file_sizeinfo(path), ', group:', group)

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0
    start = timeit.default_timer()

    if len(trade_data) == 0:
        trade_data = init_trade_data(isin_dict)

    total = None

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            if market_type != 'munc' and isin_skip(tmp[0][0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade(tmp)

            trade = True
            bNewData = True
            len_data = 0

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                trade_data.append([])
                isin_idx = len(trade_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']

            #extra data, no 'bid' oder 'ask'
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
                if tmp_open  == 0: trade_data[isin_idx][1][1] = price #high
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

    stop = timeit.default_timer()

    #print('created ret_data in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(trade_data) )
    #print('ignore_counter:', ignore_counter)
    #print('-' * 60)

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

            if abs_price_bid < abs_price_ask: post[4] = 1 #bid
            elif abs_price_bid > abs_price_ask: post[4] = 2 #ask

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

    print('files converted in: %.2f s' % (stop - start))

# ===========================================================================================


if __name__ == '__main__':

    overwrite = True
    #path = "../data"
    path = "/Volumes/Downloads/gettex/data"

    #convert_files('../data', overwrite, '20230111.21.00')
    #convert_files('../data/2023-02-03', overwrite)
    #convert_files('../data', overwrite)

    convert_files(path + '/2023-01-17', overwrite)
    convert_files(path + '/2023-01-18', overwrite)
    convert_files(path + '/2023-01-19', overwrite)
    convert_files(path + '/2023-01-20', overwrite)

    convert_files(path + '/2023-01-23', overwrite)
    convert_files(path + '/2023-01-24', overwrite)

