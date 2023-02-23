from utils import *
from isin_groups import *
from convert import *
from io import StringIO


# Performance Tests
# ----------------------------------------------------------


# isin dictionary
start = timeit.default_timer()
isin_grp_dict = get_all_isin_groups()
groups = get_isin_group_keys()
stop = timeit.default_timer()
print('get_all_isin_groups + get_isin_group_keys: %.2f s' % (stop - start))

# ------------------------------------------------------------------------------------
# search_test 
# ------------------------------------------------------------------------------------
def search_test():
    search = 'DE000HY'
    #search = 'DE000TT'

    arr = ['DE000TT', 'DE000HG', 'DE000TB', 'DE000TD', 'DE000TR', 'DE000HE', 'DE000GZ', 'DE000GX', 'DE000GF', 'DE000GK', 'DE000GH', 'DE000GC', 'DE000GB', 'DE000GA', 'DE000GS', 'DE000GM', 'DE000CH', 'DE000HC', 'DE000HB', 'DE000HR', 'DE000HV', 'DE00078', 'DE000HZ', 'DE000HX', 'DE000HW', 'DE000HU', 'DE000HY']
    arr_set = set(arr)
    search_dict = {search: None for search in arr}
    arr_dict = {k: True for k in arr}

    start = timeit.default_timer()
    for x in range(0, 10000000):
        skip = False
        #if search in arr:
        if search in arr_set:  # fastest
        #if search in search_dict:
        #if search in arr_dict:
            skip = True
    stop = timeit.default_timer()
    print('search_test: %.2f s' % (stop - start))


#search_test()
#exit()

# ------------------------------------------------------------------------------------
# init_trade_data_test 
# ------------------------------------------------------------------------------------
def init_trade_data_test(isin_dict):
    return [[] for _ in range(len(isin_dict))]


# loading group isin
for grp in groups:

    start = timeit.default_timer()
    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(grp)
    isin_dict = isin_grp_dict[grp]['isin_dict']
    isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']
    trade_data = init_trade_data_test(isin_dict)
    stop = timeit.default_timer()

    print(f'INIT data: isin + trade_data {stop-start:>6.3f} s, LEN: {len(isin_dict):>7}, GROUP: {grp}')

    #t = timeit.Timer(lambda: init_trade_data_test(isin_dict))
    #print('300x init_trade_data:', t.timeit(300))
    #print ('len:', len(trade_data), 'idx#0:', trade_data[0])
    


# ---------------------------------------------------------------------------------
# strtime_to_timestamp_test: day timestamp in minutes
# ---------------------------------------------------------------------------------
def strtime_to_timestamp_test(str):

    tmp = str.split(':')
    h = int(tmp[0])
    m = int(tmp[1])
    seconds = float(tmp[2])

    timestamp = h * 60 + m

    return timestamp, seconds

# ------------------------------------------------------------------------------------
# cast_data_pretrade_test
# ------------------------------------------------------------------------------------
def cast_data_pretrade_test(arr):
    isin = arr[0]
    tm, seconds = strtime_to_timestamp_test(arr[1]) # ohne -> 20.351 s, speed: 11.261 MB/s
    currency = arr[2]

    # cast to int and float, idx = 0 (timestamp) is already int
    bid = float(arr[3]) #bid
    bid_size = int(arr[4]) #bid_size
    ask = float(arr[5]) #ask
    ask_size = int(arr[6]) #ask_size

    if bid > 0 and ask > 0:
        #spread = round(ask - bid, 3)
        #price = round(bid + spread / 2, 3)
        spread = ask - bid
        price = bid + spread / 2
    else: # 'bid' or 'ask' can be 0 values
        spread = 0
        price = ask + bid # shortener for price = ask or price = bid

    return isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price
   

# ------------------------------------------------------------------------------------
# isin_skip: continue / skip logic for 'read_gz_posttrade' and 'read_gz_pretrade' functions 
# ------------------------------------------------------------------------------------
def isin_skip_test(tmp_isin_grp, check_ignore, isin_group, ignore_isin):
    skip = False

    #33.751 s, speed:  6.790 MB/s
    if check_ignore:
        if tmp_isin_grp in ignore_isin:
            skip = True    
    else:
        if tmp_isin_grp not in isin_group:
            skip = True    
                
    return skip

# read gz files
def read_gz(path, isin_dict, market_type, group = None, trade_data = []):

    total = None
    size = get_file_sizeinfo(path)
    size_val = size.replace(' MB', '')
    print (f'GROUP: {group}, FILE: {path}, SIZE: {size}')

    isin_group, ignore_isin, check_ignore = get_isin_groups_and_ignore_list(group)
    ignore_counter = 0
    total_lines = 0

    print(isin_group, ignore_isin)

    start = timeit.default_timer()
    with gzip.open(path, 'rt') as f:

        idx = 0

        for line in tqdm(f, total=total, unit=' lines', unit_scale=True):
            
            total_lines += 1

            #ohne -> 20.810 s, speed: 11.012 MB/s
            if market_type != 'munc' and isin_skip_test(line[0:7], check_ignore, isin_group, ignore_isin):
                ignore_counter += 1
                continue

            #list-search: 32.458 s, speed:  7.060 MB/s
            #set-search: 28.318 s, speed:  8.093 MB/s
            tmp = line.split(',')

            #ohne -> 35.428 s, speed:  6.469 MB/s
            isin, tm, seconds, currency, bid, bid_size, ask, ask_size, spread, price = cast_data_pretrade_test(tmp)

            #nach 'split' und 'cast' mit 'round' ALL GROUP: 52.742 s, speed:  4.345 MB/s
            # ---->                 ohne 'round' ALL GROUP: 46.502 s, speed:  4.928 MB/s


            # None: 10.029 s, speed: 22.852 MB/s
            # HSBC: 9.403 s, speed: 24.373 MB/s
            # Goldman_Sachs: 13.776 s, speed: 16.635 MB/s
            # UniCredit: 9.948 s, speed: 23.036 MB/s
            #continue

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                trade_data.append([])
                isin_idx = len(trade_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']
            
            # None: 11.115 s, speed: 20.617 MB/s
            # HSBC: 10.730 s, speed: 21.359 MB/s
            # Goldman_Sachs: 19.179 s, speed: 11.949 MB/s
            # UniCredit: 11.708 s, speed: 19.574 MB/s


            #print (line)
            #idx += 1
            #if idx > 10: break
            continue

    stop = timeit.default_timer()
    runtime = stop-start
    ignore_p = ignore_counter / total_lines * 100
    print(f'read gz file: {runtime:>6.3f} s, speed: {float(size_val)/runtime:>6.3f} MB/s')
    print(f'ignore_counter: {ignore_counter:>15} ({ignore_p:>3.1f}%), GROUP: {group}' )
    print ('=' * 60)

# from ssd

path = '../data_ssd/pretrade.20230112.11.00.mund.csv.gz' #240 MB
market_type = 'mund'
print ('FROM SSD')

start = timeit.default_timer()
for grp in groups:

    #if grp != None: continue
    #if grp != 'HSBC': continue
    #if grp != 'Goldman_Sachs': continue
    if grp != 'UniCredit': continue

    arr = []
    isin_dict = isin_grp_dict[grp]['isin_dict']
    isin_dict_idx = isin_grp_dict[grp]['isin_dict_idx']

    read_gz(path,isin_dict, market_type, grp, arr)

stop = timeit.default_timer()
runtime = stop-start
size = get_file_sizeinfo(path)
size_val = size.replace(' MB', '')
print(f'ALL GROUP -> read gz file: {runtime:>6.3f} s, speed: {float(size_val)/runtime:>6.3f} MB/s')

# from hdd
path = '../data/2023-01-12/pretrade.20230112.11.00.mund.csv.gz' #240 MB
#print ('FROM HDD')
#read_gz(path)
