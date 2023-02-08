import gzip
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

    print('_ISIN_SIZE_START: ', _ISIN_SIZE_START.get(group), ', new len:', len(isin_dict)) 

    if _ISIN_SIZE_START.get(group) != len(isin_dict):
        with open(path, 'wb') as f:
            pickle.dump(isin_dict, f)
    
    stop = timeit.default_timer()
    print('saved isin_dict in: %.2f s' % (stop - start)) 

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
    isin_dict_idx = {}

    start = timeit.default_timer()
    for key, value in isin_dict.items():
        isin_dict_idx[value['id']] = key
    stop = timeit.default_timer()
    print('created isin_dict_idx in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(isin_dict_idx) )
    print('----------------------------------------------------------')

    return isin_dict, isin_dict_idx


# ------------------------------------------------------------------------------------
# pretrade_debug
# ------------------------------------------------------------------------------------
def pretrade_debug(path, debug_isin, debug_time):

    print('pretrade_debug:', path, debug_isin, debug_time)

    total = None #sum(1 for _ in gzip.open(path, 'rb'))

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            isin, tm, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade(tmp)

            if debug_isin == isin and debug_time == timestamp_to_strtime(tm):
                print (tmp[1], ', bid:', bid, ', bid_size:', bid_size, ' ask:', ask, ' ask_size:', ask_size, ', spread:', spread, ', price:', price)



# ------------------------------------------------------------------------------------
# cast_data_posttrade
# ------------------------------------------------------------------------------------
def cast_data_posttrade(arr):
    isin = arr[0]
    tm = strtime_to_timestamp(arr[1])
    currency = arr[2]

    price = float(arr[3])
    amount = int(arr[4])

    return isin, tm, currency, price, amount


# ------------------------------------------------------------------------------------
# cast_data_pretrade
# ------------------------------------------------------------------------------------
def cast_data_pretrade(arr):
    isin = arr[0]
    tm = strtime_to_timestamp(arr[1])
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

    return isin, tm, currency, bid, bid_size, ask, ask_size, spread, price




# ------------------------------------------------------------------------------------
# trade_list_to_dict: convert list data to dictionary 
# ------------------------------------------------------------------------------------
def trade_list_to_dict(trade_list):
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
    ret['no_value_counter_bid'] = trade_list[17]
    ret['no_value_counter_ask'] = trade_list[18]

    return ret


# ------------------------------------------------------------------------------------
# get_isin_groups_and_ignore_list 
# ------------------------------------------------------------------------------------
def get_isin_groups_and_ignore_list(group):

    isin_group = []
    ignore_isin = []

    if group and 'ISIN_GROUPS' in globals() and group in ISIN_GROUPS.keys():
        isin_group = ISIN_GROUPS.get(group)
        print('isin_group:', isin_group)
    elif group: 
        group = None
        print ('Hint: group was set to None') 

    #generate ignore-list, append all isin if     
    if not group and 'ISIN_GROUPS' in globals():
        ignore_isin += [item for sublist in ISIN_GROUPS.values() for item in sublist]
        print ('ignore_isin - len:', len(ignore_isin))

    check_ignore = False
    if len(ignore_isin) > 0: check_ignore = True

    return isin_group, ignore_isin, check_ignore


# ------------------------------------------------------------------------------------
# init_ret_data 
# ------------------------------------------------------------------------------------
def init_ret_data(isin_dict):
    ret_data = []
    for x in range(0, len(isin_dict)):
        ret_data.append([])

    print('empty list - ret_data len: ', len(ret_data))

    return ret_data

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
def read_gz_posttrade(path, isin_dict, group = None, pretrade_data = []):

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('read_gz_posttrade:', path, ', file size:', get_file_sizeinfo(path), ', group:', group)

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0
    no_amount_counter = 0

    if len(pretrade_data) == 0:
        pretrade_data = init_ret_data(isin_dict)

    total = None

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):
 
            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            if isin_skip(tmp[0][0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            isin, tm, currency, price, amount = cast_data_posttrade(tmp)
            if amount == 0: no_amount_counter +=1

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                pretrade_data.append([])
                isin_idx = len(pretrade_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']

            #DEV-Test
            #if isin == 'US5949181045':
            #    print(isin, timestamp_to_strtime(tm), price, amount)

            len_data = len(pretrade_data[isin_idx])

             #no data, init sub-arrays
            if len_data == 0:
                #sub-array #0 is for pretrade row data
                pretrade_data[isin_idx].append([]) 
                #sub-array #1 is for extra data and summary, [counter, open, high, low, close]
                pretrade_data[isin_idx].append([0, 0, 0, 0, 0])
                #new len 
                len_data = len(pretrade_data[isin_idx])

            if len_data == 2:
                #sub-array #2 is for posttrade row data
                pretrade_data[isin_idx].append([]) 

            pretrade_data[isin_idx][2].append((tm, price, amount))

    print('pretrade_data len:', len(pretrade_data), ', no_amount_counter:', no_amount_counter)
    print('----------------------------------------------------------')

    return isin_dict, pretrade_data

# ------------------------------------------------------------------------------------
# read_gz_pretrade 
# ------------------------------------------------------------------------------------
def read_gz_pretrade(path, isin_dict, group = None):
    """
    ### Parameters:
    - path: gettex pretrade file, gz format
    - isin_dict: dictionary loaded from isin.pickle
    - group: if is None, ignore all isin from global dictionary 'ISIN_GROUPS' (file: isin_groups.py)

    ### Returns:
    - dict: isin_dict
    - list: ret_data
    """

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('read_gz_pretrade:', path, ', file size:', get_file_sizeinfo(path), ', group:', group)

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0
    start = timeit.default_timer()
    ret_data = init_ret_data(isin_dict)

    total = None

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            if isin_skip(tmp[0][0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            isin, tm, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade(tmp)

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                ret_data.append([])
                isin_idx = len(ret_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']

            #idx 0 = timestamp in minutes of the day
            #idx 1, 2 = bid_size: max, min
            #idx 3, 4 = ask_size: max, min
            #idx 5, 6 = spread: max, min
            #idx 7, 8, 9, 10 = price: open, high, low, close
            #idx 11 = activity: count data
            #idx 12, 13 = volatility: long, short
            #idx 14, 15, 16 = volatility activity: long, short, equal (no changes)
            #idx 17, 18 = no values counter: bid, ask  (price or size = 0)
            data = [tm,  bid_size,bid_size,  ask_size,ask_size,  spread,spread,  price,price,price,price,  1,  0,0,  0,0,0, 0,0] 

            trade = True
            bNewData = True
            len_data = 0

            if bid == 0 and ask == 0 and bid_size == 0 and ask_size == 0: 
                trade = False  
                bNewData = False


            if trade:

                len_data = len(ret_data[isin_idx])
                if len_data == 0: #no data, init sub-arrays

                    #sub-array #0 is for row data
                    ret_data[isin_idx].append([]) 
                    #sub-array #1 is for extra data and summary, [counter, open, high, low, close]
                    ret_data[isin_idx].append([0, price,price,price,price]) 

                #extra data / summary
                ret_data[isin_idx][1][0] += 1 #counter
                if price > ret_data[isin_idx][1][2]: ret_data[isin_idx][1][2] = price #high
                if price < ret_data[isin_idx][1][3]: ret_data[isin_idx][1][3] = price #low
                ret_data[isin_idx][1][4] = price #close

                len_data = len(ret_data[isin_idx][0])

            if len_data > 0:
                last_data = ret_data[isin_idx][0][len_data - 1]

                if bid == 0 or bid_size == 0: last_data[17] += 1
                if ask == 0 or ask_size == 0: last_data[18] += 1

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
                        if data[8] > last_data[8]: last_data[8] = data[8]
                        if data[9] < last_data[9]: last_data[9] = data[9]

                        volatility = data[10] - last_data[10]
                        last_data[10] = data[10]
                        
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

            if bNewData and trade:
                ret_data[isin_idx][0].append(data) #row data
                
                #convert to tuple
                if len_data > 1:
                    ret_data[isin_idx][0][len_data-2] = tuple(ret_data[isin_idx][0][len_data-2])
        
        #END for

    stop = timeit.default_timer()

    print('created ret_data in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(ret_data) )
    print('ignore_counter:', ignore_counter)
    print('----------------------------------------------------------')

    return isin_dict, ret_data


# ------------------------------------------------------------------------------------
# pretrade_convert_to_numpy
# ------------------------------------------------------------------------------------
def pretrade_convert_to_numpy(data):

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('pretrade_convert_to_numpy')

    start = timeit.default_timer()

    type_struct = [('time', np.uint32), 
                    ('bid', np.float32), ('bid_size', np.uint32), ('ask', np.float32), ('ask_size', np.uint32)]

    np_arr = {}
    for key in data:
        np_arr[key] = np.array(data[key], dtype=type_struct)

    stop = timeit.default_timer()
    print('created np_arr in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(np_arr) )  
    print('----------------------------------------------------------')  

    return np_arr

# ===========================================================================================


if __name__ == '__main__':

    #isin dictionary
    isin_grp_dict = get_all_isin_groups()

    #file = '../data/pretrade.20230111.21.00.munc.csv.gz' #60 KB
    #file = '../data/pretrade.20230111.21.00.mund.csv.gz' #207 MB
    #file = '../data/pretrade.20230112.11.00.mund.csv.gz' #240 MB
    #file = '../data/pretrade.20230112.10.45.mund.csv.gz' #293 MB
    #file = '../data/pretrade.20230118.14.45.mund.csv.gz' #551 MB

    file = '../data/pretrade.20230201.08.15.mund.csv.gz' #362 MB
    file_posttrade = '../data/posttrade.20230201.08.15.mund.csv.gz' #2.8 MB
    

    #isin_dict, arr = read_gz_pretrade(file, isin_dict) #saved data in: 1.67 s file size: 1.4427 MB
    #isin_dict, arr = read_gz_pretrade(file, isin_dict, 'HSBC') #saved data in: 3.15 s file size: 3.1542 MB
    #isin_dict, arr = read_gz_pretrade(file, isin_dict, 'Goldman_Sachs') #saved data in: 9.41 s file size: 10.7039 MB
    #isin_dict, arr = read_gz_pretrade(file, isin_dict, 'UniCredit') #saved data in: 2.86 s file size: 3.1331 MB
    
    print('ISIN_GROUPS:', ISIN_GROUPS)
    groups = get_isin_group_keys()
    print('groups:', groups)

    for grp in groups:
        print('grp: ', str(grp)) 

        #DEV-TEST
        #if grp != 'Goldman_Sachs': continue
        #if grp != None: continue

        isin_dict = isin_grp_dict[grp]['isin_dict']
        isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']
        isin_dict, arr = read_gz_pretrade(file, isin_dict, grp)

        data_file = '../data.pickle.zip'
        if grp: data_file = f'../data.{grp}.pickle.zip'

        #arr = load_from_pickle(data_file)
        #arr = []
        isin_dict, arr = read_gz_posttrade(file_posttrade, isin_dict, grp, arr)

        save_as_pickle(data_file, arr)
        save_isin_dict(isin_dict, grp)

        #free RAM (garbage collector) 
        del arr




    # DE0007236101 Siemens
    #isin_idx = isin_dict['DE0007236101']['id']

    #data = arr[isin_idx]

    #for d in data[0]:
    #    print(timestamp_to_strtime(d[0]), d)