from utils import download_compressed_url
import os
import csv
import pickle
from datetime import datetime

_CACHE_DAYS = 7

# -------------------------------------------------
# download_instruments
# -------------------------------------------------
def download_instruments():
    url = 'https://mein.finanzen-zero.net/assets/searchdata/downloadable-instruments.csv'
    output_path = '../'
    output_file =  'downloadable-instruments.csv'
    out_filepath = output_path + output_file

    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d_%H%M%S')
    download_new_file = False

    if os.path.exists(out_filepath):

        SECONDS_PER_DAY = 86400
        
        file_ctime = os.path.getctime(out_filepath)
        str_file_ctime = datetime.fromtimestamp(file_ctime).strftime('%Y-%m-%d %H:%M:%S')
        days_ago = int((now.timestamp() - file_ctime) / SECONDS_PER_DAY)
        print (f'file {out_filepath} created at {str_file_ctime} --> {days_ago} day(s) ago')

        if days_ago > _CACHE_DAYS: 
            tmp = output_file.split('.')
            new_file = f'{output_path}{tmp[0]}_{now_str}.csv'
            print (f'rename to {new_file} ...')
            os.rename(out_filepath, new_file)
            download_new_file = True
    else:
        download_new_file = True

    if download_new_file:
        print (f'download new file {out_filepath} ...')
        download_compressed_url(url, out_filepath)
    else:
        print (f'the file {out_filepath} is still up to date, downloaded {days_ago} day(s) ago (_CACHE_DAYS = {_CACHE_DAYS})')

# -------------------------------------------------
# download_instruments
# -------------------------------------------------
def convert_csv_to_type_dict():
    #DE: ISIN;WKN;Typ;Sparplan;Name;
    #EN: isin;wkn;type;savings_plan;name
    csv_path = '../downloadable-instruments.csv'
    pickle_out = '../finanzen.net.pickle'

    data = {}

    with open(csv_path, 'rt') as f_in:
        reader = csv.reader(f_in, delimiter=';')
        next(reader) # skip first line
        for row in reader:
            isin, wkn, type, savings_plan, name, _ = row
            if data.get(type) is None: data[type] = []
            dict_data = {'wkn': wkn, 'isin': isin, 'savings_plan': savings_plan, 'name': name}
            data[type].append(dict_data)

    print (f'saving file {pickle_out} ...')
    with open(pickle_out, 'wb') as f_out:
        pickle.dump(data, f_out)

    pass

# -------------------------------------------------
# download_instruments
# -------------------------------------------------
def load_dict_data():
    pickle_path = '../finanzen.net.pickle'

    ret = None
    with open(pickle_path, 'rb') as f_in:
        ret = pickle.loads(f_in.read())

    return ret

# ================================================================================
if __name__ == '__main__':
    download_instruments()
    convert_csv_to_type_dict()

    print('load_dict_data ...')
    print('-'*15)
    data = load_dict_data()
    for type in data:
        print (type, 'len:', len(data[type]))
