import sys
import os
import gzip
import numpy as np
import timeit

# ---------------------------------------------------------------------------------
# strtime_to_timestamp: day timestamp in ms (max. value -> 23:59:59.999 = 86399999)
# ---------------------------------------------------------------------------------
def strtime_to_timestamp(str):

    tmp = str.split('.')
    time = tmp[0]; 
    ms = int(tmp[1]);

    tmp = time.split(':')
    h = int(tmp[0])
    m = int(tmp[1])
    s = int(tmp[2])

    timestamp = h * 3600000 + m * 60000 + s * 1000 + ms

    return timestamp

# -------------------------------------------------
# get_key_index
# -------------------------------------------------
def get_key_index(arr, key):

    found = False
    idx = 1
    arrlen = len(arr)

    if arrlen > 0:
        idx = np.flatnonzero(np.core.defchararray.find(arr, key)!=-1)

        if idx.size: 
            idx = idx[0]
            found = True
        else: 
            idx = arrlen + 1

    return found, idx

# ------------------------------------------------------------------------------------
# read_gz
#
# ISIN (idx = 0): 
# EN: https://en.wikipedia.org/wiki/International_Securities_Identification_Number
# DE: https://de.wikipedia.org/wiki/Internationale_Wertpapierkennnummer
# ------------------------------------------------------------------------------------
def read_gz(path):

    file_stats = os.stat(path)
    print("File Size: %.4f MB" % (file_stats.st_size / 1024 / 1024))

    ret_keys = []
    ret_data = []

    with gzip.open(path, 'rb') as f:

        line = f.readline()

        while line:
            #print(line)
            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            tmp[1] = strtime_to_timestamp(tmp[1])
            key = tmp[0] + ':' + tmp[2]
            found, tmp[0] = get_key_index(ret_keys, key)
    

            #delete currency
            del tmp[2]
            arr = tuple(tmp)

            if not found: ret_keys.append(key)
            ret_data.append(arr)
            line = f.readline()

    return ret_keys, ret_data


# =====================================================================================


file = '../data/pretrade.20230111.21.00.munc.csv.gz' #60 KB
file = '../data/pretrade.20230111.21.00.mund.csv.gz' #207 MB

#read_gz('../data/posttrade.20230111.21.00.munc.csv.gz')
start = timeit.default_timer()
keys, arr = read_gz(file)
stop = timeit.default_timer()

print('Timer read_gz: %.2f s' % (stop - start) )

start = timeit.default_timer()
found, idx = get_key_index(keys, 'DE0005772206:EUR')
stop = timeit.default_timer()
print(keys[0] + ' -> Timer get_key_index: %.6f s' % (stop - start), ", idx: %d " % idx)

start = timeit.default_timer()
found, idx = get_key_index(keys, 'DE0005140008:EUR')
stop = timeit.default_timer()
print(keys[len(keys)-1] + ' -> Timer get_key_index: %.6f s' % (stop - start), ", idx: %d " % idx)

print(keys[len(keys)-1], keys[42])


print ("keys len: %d" % len(keys))
print("arr Size: %.4f MB" % (sys.getsizeof(arr) / 1024 / 1024))

type_struct = [('isin_idx', np.uint32), ('time', np.uint32), 
                ('bid', np.float32), ('bid_size', np.uint16), ('ask', np.float32), ('ask_size', np.uint16)]

x = np.array(arr, dtype=type_struct)

print(x, x.dtype)


print("np.array Size: %.4f MB" % (sys.getsizeof(x) / 1024 / 1024))

# save + load
# =========================
save_file = '../test.npz'
np.savez_compressed(save_file, isin_keys = keys, data = x)

file_stats = os.stat(save_file)
print("npz File Size: %.4f MB" % (file_stats.st_size / 1024 / 1024))    

x = np.load(save_file)['data']
keys = np.load(save_file)['isin_keys']

print(x)
print("np.array Size: %.4f MB" % (sys.getsizeof(x) / 1024 / 1024))
print("isin_keys Size: %.4f MB" % (sys.getsizeof(keys) / 1024 / 1024))