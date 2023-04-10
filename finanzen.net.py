from utils import download_compressed_url
import os
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

# =======================
if __name__ == '__main__':
    download_instruments()
