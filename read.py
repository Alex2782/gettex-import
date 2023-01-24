import sys
import os
import gzip
import zipfile
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
    
    if os.path.exists(path):
        file_stats = os.stat(path)
        return "%.4f MB" % (file_stats.st_size / 1024 / 1024)

# ---------------------------------------------------------------------------------
# get_sizeof_info
# ---------------------------------------------------------------------------------
def get_sizeof_info(obj):
    return '%.4f MB' % (sys.getsizeof(obj) / 1024 / 1024)


# ---------------------------------------------------------------------------------
# save_isin_dict: save isin dictionary as pickle file
# ---------------------------------------------------------------------------------
def save_isin_dict(isin_dict, path = None):

    global isin_size_start

    if path is None: path = './isin.pickle'

    start = timeit.default_timer()

    print('isin_size_start: ', isin_size_start, ', new len:', len(isin_dict)) 

    if isin_size_start != len(isin_dict):
        with open(path, 'wb') as f:
            pickle.dump(isin_dict, f)
    
    stop = timeit.default_timer()
    print('saved isin_dict in: %.2f s' % (stop - start)) 

# ---------------------------------------------------------------------------------
# save_as_pickle: save data as pickle file
# ---------------------------------------------------------------------------------
def save_as_pickle(path, data):

    start = timeit.default_timer()

    print('save_as_pickle:', path, 'data:', len(data)) 

    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_BZIP2, compresslevel=9) as f:
        f.writestr("data.pickle", pickle.dumps(data))

    stop = timeit.default_timer()
    print('saved data in: %.2f s' % (stop - start), 'file size:', get_file_sizeinfo(path)) 

# ---------------------------------------------------------------------------------
# load_from_pickle: load data from a pickle file
# ---------------------------------------------------------------------------------
def load_from_pickle(path):

    start = timeit.default_timer()

    print('load_from_pickle:', path)

    ret = []
    with zipfile.ZipFile(path, "r") as zf:
        ret = pickle.loads(zf.read("data.pickle"))

    stop = timeit.default_timer()
    print('loaded data in: %.2f s' % (stop - start)) 

    return ret

# ---------------------------------------------------------------------------------
# load_isin_dict
# ---------------------------------------------------------------------------------
def load_isin_dict(path = None):

    global isin_size_start

    if path is None: path = './isin.pickle'
    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('load_isin_dict:', path, ', file size:', get_file_sizeinfo(path))

    isin_dict = {}

    start = timeit.default_timer()

    if os.path.exists(path):
        with open(path, 'rb') as f:
            isin_dict = pickle.load(f)

    isin_size_start = len(isin_dict)

    stop = timeit.default_timer()
    print('loaded isin_dict in: %.2f s' % (stop - start), ', len:', isin_size_start, ', sizeof:', get_sizeof_info(isin_dict)) 


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
# read_gz
# ------------------------------------------------------------------------------------
def read_gz(path, isin_dict):


    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('read_gz:', path, ', file size:', get_file_sizeinfo(path))

    start = timeit.default_timer()

    ret_data = []
    for x in range(0, len(isin_dict)):
        ret_data.append([])

    print('empty arrays, ret_data len: ', len(ret_data))

    total = abs(sum(1 for _ in gzip.open(path, 'rb')) / 2)

    with gzip.open(path, 'rb') as f:

        for line in tqdm(f, total=total, unit='lines'):

            data = str(line).replace("b'", "").replace("\\n'", '')
            tmp = data.split(',')

            tmp[1] = strtime_to_timestamp(tmp[1])
            isin = tmp[0]
            currency = tmp[2]

            isin_obj = isin_dict.get(isin)
            if isin_obj is None: 
                ret_data.append([])
                isin_idx = len(ret_data) - 1
                isin_dict[isin] = {'id': isin_idx, 'c': currency}
            else:
                isin_idx = isin_dict[isin]['id']

            #delete isin and currency
            del tmp[0]
            del tmp[1]

            len_data = len(ret_data[isin_idx])
            if len_data == 0: #no data, init sub-arrays
                ret_data[isin_idx].append([]) #sub-array #0 is for row data
                ret_data[isin_idx].append([0, 0, 0, 0, 0]) #sub-array #1 is for extra data, [counter, ?, ? ,? ,?]

            #cast to int and float, idx = 0 (timestamp) is already int
            tmp[1] = float(tmp[1]) #bid
            tmp[2] = int(tmp[2]) #bid_size
            tmp[3] = float(tmp[3]) #ask
            tmp[4] = int(tmp[4]) #ask_size

            tmp = tuple(tmp)

            bNewData = True
            len_data = len(ret_data[isin_idx][0])
            #print ('isin_idx:',isin_idx, 'data:', ret_data[isin_idx][0])
            if len_data > 1:
                last_data = ret_data[isin_idx][0][len_data - 1]
                
                if last_data[1:5] == tmp[1:5]: #check without timestamp (bid, bid_size, ask, ask_size)
                    bNewData = False

            if bNewData:
                ret_data[isin_idx][0].append(tmp) #row data

            ret_data[isin_idx][1][0] += 1 #counter

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

    type_struct = [('time', np.uint32), 
                    ('bid', np.float32), ('bid_size', np.uint32), ('ask', np.float32), ('ask_size', np.uint32)]

    np_arr = {}
    for key in data:
        np_arr[key] = np.array(data[key], dtype=type_struct)

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
    np.savez_compressed(path, data=data)
    #np.savez(path, **{name: data})
    stop = timeit.default_timer()
    
    print('savez_compressed in: %.2f s' % (stop - start), ', file size:', get_file_sizeinfo(path))  
    print('----------------------------------------------------------')  


# ------------------------------------------------------------------------------------
# load_npz
# ------------------------------------------------------------------------------------
def load_npz(path):

    print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
    print('load_npz:', path)

    start = timeit.default_timer()
    np_data = np.load(path, allow_pickle=True)

    file = np_data.files[0]
    print('np_data.files:', np_data.files)
    #ret = np_data[np_data.files[0]].item()
    ret = np_data[file]
    stop = timeit.default_timer()

    print('np.load + read [' + file + '] in: %.2f s' % (stop - start), ', sizeof:', get_sizeof_info(ret))  
    print('----------------------------------------------------------')  
    return ret

# =====================================================================================

data_file = '../data.pickle.zip'

#isin dictionary
isin_dict, isin_dict_idx = load_isin_dict()


file = '../data/pretrade.20230111.21.00.munc.csv.gz' #60 KB
#file = '../data/pretrade.20230111.21.00.mund.csv.gz' #207 MB
#file = '../data/pretrade.20230112.11.00.mund.csv.gz' #240 MB
#file = '../data/pretrade.20230112.10.45.mund.csv.gz' #293 MB
#file = '../data/pretrade.20230118.14.45.mund.csv.gz' #551 MB




#TODO Python verbraucht nach 'read_gz' 2,8 GB RAM
#wenn die Wiederholungen entfernt werden, dann bei 1,34 GB
#TODO auf 1 Sekunde oder 1 Minute zusammenfassen? berechnen: open, close, high, low
isin_dict, arr = read_gz(file, isin_dict)

count_row_data = 0
saved_data = 0
for data in arr:
    if len(data) > 0:
        count_row_data += len(data[0])
        #print ('len row data:', len(data[0]), ', extra data:', data[1])
        saved_data += data[1][0] - len(data[0])

print('count_row_data:', count_row_data)
print('saved_data:', saved_data)


#DEV - Debug, Daten zusammenfassen, open, close, high, low
idx = 0
for data in arr:
    if len(data) > 0:
        print(idx, isin_dict_idx[idx], 'anz:', len(data[0]))
    
    idx += 1

#19 DE0005140008 anz: 62
for data in arr[19][0]:
    print (timestamp_to_strtime(data[0]), data)





#save_isin_dict(isin_dict)

#print('data len:', len(arr), 'size:', get_sizeof_info(arr))
#save_as_pickle(data_file, arr)

#data = load_from_pickle(data_file)
#print('data len:', len(data), 'size:', get_sizeof_info(data))


exit()


np_data = load_npz(data_file)

max_len = 0
max_isin = ''
for isin in np_data.keys():
    if max_len < len(np_data[isin]):
        max_len = len(np_data[isin])
        max_isin = isin


#print(np_data.get('CH1135202179'))
print('MAX:', max_isin, ', len:', max_len)
print('Siemens MAX:', isin, ', len:', len (np_data['DE0007236101']))
print(np_data['DE0007236101'])