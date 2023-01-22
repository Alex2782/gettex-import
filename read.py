import sys
import os
import gzip
import numpy as np
import timeit
import pickle
from tqdm import tqdm
import math

# -------------------------------------------------
# class DownloadProgressBar
# -------------------------------------------------
class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

# ---------------------------------------------------------------------------------
# strtime_to_timestamp: day timestamp in ms (max. value -> 23:59:59.999 = 86399999)
# ---------------------------------------------------------------------------------
def strtime_to_timestamp(str):

    tmp = str.split('.')
    time = tmp[0]
    ms = int(tmp[1][0:3])

    tmp = time.split(':')
    h = int(tmp[0])
    m = int(tmp[1])
    s = int(tmp[2])

    timestamp = h * 3600000 + m * 60000 + s * 1000 + ms

    return timestamp

# ---------------------------------------------------------------------------------
# timestamp_to_strtime: HH:MM:SS.ms (max. value -> 86399999 = 23:59:59.999)
# ---------------------------------------------------------------------------------
def timestamp_to_strtime(tm):

    h = math.floor(tm / 3600000)
    tm -= h * 3600000
    m = math.floor(tm / 60000)
    tm -= m * 60000
    s = math.floor(tm / 1000)
    tm -= s * 1000
    ms = tm

    return '{:02d}:{:02d}:{:02d}.{:03d}'.format(h, m, s, ms)

# ---------------------------------------------------------------------------------
# get_file_sizeinfo
# ---------------------------------------------------------------------------------
def get_file_sizeinfo(path):
    file_stats = os.stat(path)
    return "%.4f MB" % (file_stats.st_size / 1024 / 1024)

# ---------------------------------------------------------------------------------
# get_sizeof_info
# ---------------------------------------------------------------------------------
def get_sizeof_info(obj):
    return '%.4f MB' % (sys.getsizeof(obj) / 1024 / 1024)


# ---------------------------------------------------------------------------------
# load_isin_dict
# ---------------------------------------------------------------------------------
def load_isin_dict(path = None):

    if path is None: path = '../isin.pickle'
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('load_isin_dict:', path, ', file size:', get_file_sizeinfo(path))

    isin_dict = {}

    start = timeit.default_timer()

    if os.path.exists(isin_file):
        with open(isin_file, 'rb') as f:
            isin_dict = pickle.load(f)

    stop = timeit.default_timer()
    print('loaded isin_dict in: %.2f s' % (stop - start), ', len:', len(isin_dict), ', sizeof:', get_sizeof_info(isin_dict)) 


    # isin_dict_idx
    isin_dict_idx = {}

    start = timeit.default_timer()
    for key, value in isin_dict.items():
        isin_dict_idx[value] = key
    stop = timeit.default_timer()
    print('created isin_dict_idx in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(isin_dict_idx) )
    print('----------------------------------------------------------')

    return isin_dict, isin_dict_idx


# ------------------------------------------------------------------------------------
# read_gz
# ------------------------------------------------------------------------------------
def read_gz(path, isin_dict):


    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('read_gz:', path, ', file size:', get_file_sizeinfo(path))

    start = timeit.default_timer()

    ret_data = []

    total = abs(sum(1 for _ in gzip.open(path, 'rb')) / 2)

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit='lines'):
            #print(line)
            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            tmp[1] = strtime_to_timestamp(tmp[1])
            key = tmp[0] + ':' + tmp[2]

            isin_idx = isin_dict.get(key)
            if isin_idx is None: 
                isin_idx = len(isin_dict) + 1
                isin_dict[key] = isin_idx

            tmp[0] = isin_idx

            del tmp[0]

            #delete currency
            del tmp[1]
            arr = tuple(tmp)

            ret_data.append(arr)
            line = f.readline()

    stop = timeit.default_timer()

    print('created ret_data in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(ret_data) )
    print('----------------------------------------------------------')

    return isin_dict, ret_data


# ------------------------------------------------------------------------------------
# pretrade_convert_to_numpy
# ------------------------------------------------------------------------------------
def pretrade_convert_to_numpy(data):

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('pretrade_convert_to_numpy')

    start = timeit.default_timer()

    type_struct = [('isin_idx', np.uint32), ('time', np.uint32), 
                    ('bid', np.float32), ('bid_size', np.uint32), ('ask', np.float32), ('ask_size', np.uint32)]

    type_struct = [('time', np.uint32), 
                    ('bid', np.float32), ('bid_size', np.uint32), ('ask', np.float32), ('ask_size', np.uint32)]


    np_arr = np.array(data, dtype=type_struct)

    stop = timeit.default_timer()
    print('created np_arr in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(np_arr) )  
    print('----------------------------------------------------------')  

    return np_arr

# ------------------------------------------------------------------------------------
# save_npz
# ------------------------------------------------------------------------------------
def save_npz(path, data):

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('save_npz:', path)

    start = timeit.default_timer()
    np.savez_compressed(path, data = data)
    stop = timeit.default_timer()

    print('savez_compressed in: %.2f s' % (stop - start), ', file size:', get_file_sizeinfo(path))  
    print('----------------------------------------------------------')  

# =====================================================================================

data_file = '../data.npz'

#isin dictionary
isin_file = '../isin.pickle'
isin_dict, isin_dict_idx = load_isin_dict(isin_file)


file = '../data/pretrade.20230111.21.00.munc.csv.gz' #60 KB
#file = '../data/pretrade.20230111.21.00.mund.csv.gz' #207 MB
#file = '../data/pretrade.20230112.11.00.mund.csv.gz' #240 MB
#file = '../data/pretrade.20230112.10.45.mund.csv.gz' #293 MB


isin_dict, arr = read_gz(file, isin_dict)

np_arr = pretrade_convert_to_numpy(arr)

print(np_arr)

#save_npz(data_file, np_arr)

for x in range (1, 33):
    print(x, 'Bits -> max. ', int(math.pow(2, x) - 1))


exit()
start = timeit.default_timer()
x = np.load(data_file)['data']
stop = timeit.default_timer()
print('Timer np.load: %.2f s' % (stop - start) )

print(x)
print("np.array Size: %.4f MB" % (sys.getsizeof(x) / 1024 / 1024))



idx_siemens = isin_dict.get("DE0007236101:EUR")

start = timeit.default_timer()
indices = np.where(x['isin_idx'] == idx_siemens)
#print('indices:', indices)
#print('data isin_idx = {idx_siemens}:', x[indices])
stop = timeit.default_timer()
print('Timer np.where: %.2f s' % (stop - start) )

print(x[5554], x[6696], x[53146])

#TODO timestamp nach str und die Werte vergleichen bei Siemens (gz mit npz)

exit()
isin_size_start = len(isin_dict);

start = timeit.default_timer()
isin_dict, arr = read_gz(file, isin_dict)
stop = timeit.default_timer()

print('Timer read_gz: %.2f s' % (stop - start) )

print ("isin_dict len: %d" % len(isin_dict))
print("arr Size: %.4f MB" % (sys.getsizeof(arr) / 1024 / 1024))

type_struct = [('isin_idx', np.uint32), ('time', np.uint32), 
                ('bid', np.float32), ('bid_size', np.uint32), ('ask', np.float32), ('ask_size', np.uint32)]

x = np.array(arr, dtype=type_struct)

print(x, x.dtype)

print("np.array Size: %.4f MB" % (sys.getsizeof(x) / 1024 / 1024))

# save + load
# =========================
np.savez_compressed(data_file, data = x)

#save isin dictionary as pickle file
if isin_size_start != len(isin_dict):
    with open(isin_file, 'wb') as f:
        pickle.dump(isin_dict, f)


file_stats = os.stat(data_file)
print("data.npz Size: %.4f MB" % (file_stats.st_size / 1024 / 1024))    

file_stats = os.stat(isin_file)
print("isin.pickle Size: %.4f MB" % (file_stats.st_size / 1024 / 1024))    


x = np.load(data_file)['data']

print(x)
print("np.array Size: %.4f MB" % (sys.getsizeof(x) / 1024 / 1024))
print("isin_dict Size: %.4f MB" % (sys.getsizeof(isin_dict) / 1024 / 1024))


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