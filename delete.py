# DELETE files: pretrade.{DATE}.{HH}.{MM}.mund.csv.gz

from utils import *
import datetime

path = '../data/'
#path = '../data_test/' #DEV-TEST

from_date = '2023-03-01'
number_of_days = 40

start = timeit.default_timer()
date = datetime.datetime.strptime(from_date, '%Y-%m-%d')


for i in range(number_of_days):

    str_date = f'{date.strftime("%Y-%m-%d")}'
    sub_path = str_date + '/'
    date += datetime.timedelta(days=1)
    tmp_path = path + sub_path

    if not os.path.exists(tmp_path): continue

    files = list_gz_files(tmp_path, full_path = False, sub_directories = False, validate = False)

    for filepath in tqdm(files, unit=' files', unit_scale=True, desc=str_date):
        
        basename = os.path.basename(filepath)
        tmp = basename.split('.')

        file_type = tmp[0] 
        file_date = tmp[1]
        file_hh = tmp[2]
        file_mm = tmp[3]
        file_grp = tmp[4]

        if file_type == "pretrade" and file_grp == "mund":
            delete_path = tmp_path + filepath
            str_size = get_file_sizeinfo(delete_path)
            print (f"DELETE {str_size} {delete_path}")
            os.remove(delete_path)