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

    total = abs(sum(1 for _ in gzip.open(path, 'rb')) / 2)

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit='lines'):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            isin, tm, currency, bid, bid_size, ask, ask_size, spread, price = cast_data(tmp)

            if debug_isin == isin and debug_time == timestamp_to_strtime(tm):
                print (tmp[1], ', bid:', bid, ', bid_size:', bid_size, ' ask:', ask, ' ask_size:', ask_size, ', spread:', spread, ', price:', price)

            line = f.readline()


# ------------------------------------------------------------------------------------
# cast_data
# ------------------------------------------------------------------------------------
def cast_data(arr):
    isin = arr[0]
    tm = strtime_to_timestamp(arr[1])
    currency = arr[2]

    #cast to int and float, idx = 0 (timestamp) is already int
    bid = float(arr[3]) #bid
    bid_size = int(arr[4]) #bid_size
    ask = float(arr[5]) #ask
    ask_size = int(arr[6]) #ask_size

    spread = round(ask - bid, 3)
    price = round(bid + spread / 2, 3)

    return isin, tm, currency, bid, bid_size, ask, ask_size, spread, price


# ------------------------------------------------------------------------------------
# read_gz_posttrade 
# ------------------------------------------------------------------------------------
def read_gz_posttrade(path, isin_dict, pretrade_data):
    #TODO: pretrade_data vervollständigen, idx 18, 19

    #idx 18, 19 = traded(for posttrade file): Volume, number of shares

    return 

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
    print('read_gz:', path, ', file size:', get_file_sizeinfo(path), ', group:', group)

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
    ignore_counter = 0
    if len(ignore_isin) > 0: check_ignore = True

    start = timeit.default_timer()

    ret_data = []
    for x in range(0, len(isin_dict)):
        ret_data.append([])

    print('empty arrays - ret_data len: ', len(ret_data))

    total = abs(sum(1 for _ in gzip.open(path, 'rb')) / 2)

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit='lines'):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            # continue / skip logic
            # ++++++++++++++++++++++++++++++++++++++++++++
            tmp_isin_grp = tmp[0][0:7] #idx 0 = isin
            if check_ignore:
                if tmp_isin_grp in ignore_isin:
                    ignore_counter += 1
                    line = f.readline()
                    continue
            else:
                if tmp_isin_grp not in isin_group:
                    ignore_counter += 1
                    line = f.readline()
                    continue                    
            # --------------------------------------------

            isin, tm, currency, bid, bid_size, ask, ask_size, spread, price = cast_data(tmp)

            

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                ret_data.append([])
                isin_idx = len(ret_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']


            len_data = len(ret_data[isin_idx])
            if len_data == 0: #no data, init sub-arrays
                ret_data[isin_idx].append([]) #sub-array #0 is for row data
                ret_data[isin_idx].append([0, 0, 0, 0, 0]) #sub-array #1 is for extra data, [counter, ?, ? ,? ,?]

            #init bid_size, idx:1 = max, idx:2 = min
            #init ask_size, idx:3 = max, idx:4 = min
            #spread, idx: 5 = max, idx: 6 = min
            #price (idx 7 to 10): open, high, low, close
            #idx 11 = activity, count data
            #idx 12, 13 = volatility long, short #TODO: richtig auswerten Close kann mit nächsten Open abweichen, da auch Vola berechnen
            #idx 14, 15, 16 = volatility activity long, short, equal (no changes)
            #idx 17 = no trade counter (bid, ask and size = 0)
            #idx 18, 19 = traded(for posttrade file): Volume, number of shares
            data = [tm,  bid_size,bid_size,  ask_size,ask_size,  spread,spread,  price,price,price,price,  1,  0,0,  0,0,0, 0, 0,0] 

            bNewData = True
            len_data = len(ret_data[isin_idx][0])
            #print ('isin_idx:',isin_idx, 'data:', ret_data[isin_idx][0])
            if len_data > 0:
                last_data = ret_data[isin_idx][0][len_data - 1]

                if last_data[0] == tm:
                    bNewData = False

                    #bid_size, idx:1 = max, idx:2 = min
                    if data[1] > last_data[1]: last_data[1] = data[1]
                    if data[2] < last_data[2]: last_data[2] = data[2]
                    
                    #ask_size, idx:3 = max, idx:4 = min
                    if data[3] > last_data[3]: last_data[3] = data[3]
                    if data[4] < last_data[4]: last_data[4] = data[4]

                    trade = True
                    if bid == 0 and ask == 0 and bid_size == 0 and ask_size == 0: trade = False  


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
                    else:
                        #idx 17 = no trade counter (bid, ask and size = 0)
                        last_data[17] += 1

            if bNewData:
                ret_data[isin_idx][0].append(data) #row data
                
                #convert to tuple
                if len_data > 1:
                    ret_data[isin_idx][0][len_data-2] = tuple(ret_data[isin_idx][0][len_data-2])

            ret_data[isin_idx][1][0] += 1 #counter

            line = f.readline()

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
    file = '../data/pretrade.20230118.14.45.mund.csv.gz' #551 MB

    #file = '../data/pretrade.20230201.08.15.mund.csv.gz' #362 MB
    #file_posttrade = '../data/posttrade.20230201.08.15.mund.csv.gz' #2.8 MB
    

    #isin_dict, arr = read_gz_pretrade(file, isin_dict) #saved data in: 1.67 s file size: 1.4427 MB
    #isin_dict, arr = read_gz_pretrade(file, isin_dict, 'HSBC') #saved data in: 3.15 s file size: 3.1542 MB
    #isin_dict, arr = read_gz_pretrade(file, isin_dict, 'Goldman_Sachs') #saved data in: 9.41 s file size: 10.7039 MB
    #isin_dict, arr = read_gz_pretrade(file, isin_dict, 'UniCredit') #saved data in: 2.86 s file size: 3.1331 MB
    
    print('ISIN_GROUPS:', ISIN_GROUPS)
    groups = get_isin_group_keys()
    print('groups:', groups)

    for grp in groups:
        print('grp: ', str(grp)) 

        isin_dict = isin_grp_dict[grp]['isin_dict']
        isin_dict, arr = read_gz_pretrade(file, isin_dict, grp)

        #TODO: read_gz_posttrade(file_posttrade, isin_dict, arr)

        data_file = '../data.pickle.zip'
        if grp: data_file = f'../data.{grp}.pickle.zip'

        save_as_pickle(data_file, arr)
        save_isin_dict(isin_dict, grp)

        #del arr




    # DE0007236101 Siemens
    #isin_idx = isin_dict['DE0007236101']['id']

    #data = arr[isin_idx]

    #for d in data[0]:
    #    print(timestamp_to_strtime(d[0]), d)