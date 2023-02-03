import urllib.request
import json
import re
from tqdm import tqdm
import math
import sys
import os
import numpy as np
import timeit
import zipfile
import pickle

# -------------------------------------------------
# init_request: add header 'User-agent'
# -------------------------------------------------
def init_request():
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler)
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)

# -------------------------------------------------
# class DownloadProgressBar
# -------------------------------------------------
class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)

# -------------------------------------------------
# download_url
# -------------------------------------------------
def download_url(url, output_path):

    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)

# -------------------------------------------------
# get_html
# -------------------------------------------------
def get_html(url, log = True):
    if log: print ("get_html: " + url)
    
    html = ''

    try:
        html = urllib.request.urlopen(url).read()
    except Exception as e:
        print(f"Error: {str(e)}, url: {url}")

    return str(html)

# -------------------------------------------------
# get_json
# -------------------------------------------------
def get_json(url, log = True):
    if log: print ("get_json: " + url)
    json_str = urllib.request.urlopen(url).read()
    return json.loads(json_str)


# -------------------------------------------------
# onvista_search
# -------------------------------------------------
def onvista_search(searchValue, website = False, snapshot = False, log = True):

    ret = {}

    url = f'https://api.onvista.de/api/v1/instruments/search/facet?searchValue={searchValue}'
    ret['search'] = get_json(url, log)
    
    instrument = ret['search']['instrument']
    isin = instrument['isin']
    entityType = instrument['entityType']
    entityValue = instrument['entityValue']

    if entityType == 'DERIVATIVE':
        url = f'https://api.onvista.de/api/v1/derivatives/{entityValue}/snapshot'
        ret['derivative_snapshot'] = get_json(url, log)

    # website
    if website:
        url = instrument['urls']['WEBSITE']
        ret['website'] = get_html(url, log)

        start = '<script id="__NEXT_DATA__" type="application/json">'
        end = '</script>'

        match = re.search(f'{start}(.*){end}', ret['website'], re.DOTALL)
        if match:
            data = match.group(1).encode('utf-8').decode('unicode_escape')
            ret['__NEXT_DATA__'] = json.loads(data)

    # snapshot
    if snapshot:
        url = f'https://api.onvista.de/api/v1/instruments/{entityType}/ISIN:{isin}/snapshot'
        ret['snapshot'] = get_json(url, log)

    return ret


# ---------------------------------------------------------------------------------
# strtime_to_timestamp: day timestamp in minutes
# ---------------------------------------------------------------------------------
def strtime_to_timestamp(str):

    tmp = str.split('.')
    time = tmp[0]

    tmp = time.split(':')
    h = int(tmp[0])
    m = int(tmp[1])

    timestamp = h * 60 + m

    # TODO: Performance überprüfen, Vorschlag von ChatGPT
    # h, m = map(int, str.split(':')[:2])
    # timestamp = h * 60 + m

    return timestamp

# ---------------------------------------------------------------------------------
# timestamp_to_strtime: HH:MM
# ---------------------------------------------------------------------------------
def timestamp_to_strtime(tm):

    h = math.floor(tm / 60)
    tm -= h * 60
    m = tm

    return '{:02d}:{:02d}'.format(h, m)

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