from utils import *
from isin_groups import *
from convert import *


path = '../data'
#isin = "DE0007236101" #Siemens AG Namens-Aktien o.N.
#isin = "DE0008232125" #Lufthansa
#isin = "US83406F1021" #SoFi Technologies Inc.
isin = "FI0009000681" #Nokia

#pretrade_debug('../data/2023-01-12/pretrade.20230112.10.45.mund.csv.gz', isin)

#path = "../data"
#sub_path = '2023-01-12'
#convert_files(path + '/' + sub_path, overwrite=True, file_mask='20230112.10.45')
#exit()

output_file = f'../data_ssd/{isin}.csv'

isin_grp = ISIN_GROUPS_IDX.get(isin)
isin_grp_dict = get_all_isin_groups()
isin_dict = isin_grp_dict[isin_grp]['isin_dict']

print ('path:', path)
print ('isin:', isin)
print ('isin_grp:', isin_grp)

isin_idx = isin_dict.get(isin)
if isin_idx is None:
    print(f'isin {isin} not found')
    exit()

isin_idx = isin_idx['id']
print ('isin_idx:', isin_idx)

start = timeit.default_timer()
files = list_zip_files(path, True, True)

line_counter = 0

with open(output_file, "wt") as f_out:
    f_out.write('Time,Open,High,Low,Close,Volume' + '\n')
    for filepath in tqdm(files, unit=' files', unit_scale=True):
        
        basename = os.path.basename(filepath)
        tmp = basename.split('.')

        file_date = tmp[1]
        file_hh = tmp[2]
        file_mm = tmp[3]
        file_grp = tmp[4]

        if file_grp == 'pickle': file_grp = None
        if isin_grp != file_grp: continue

        trade = load_from_pickle(filepath)

        if len(trade) <= isin_idx: continue

        data = trade[isin_idx]

        if len(data) == 0: continue

        pre = []
        extra = []
        post = []
        if len(data) > 0: pre = data[0]
        if len(data) > 1: extra = data[1]
        if len(data) > 2: post = data[2]

        open = round(extra[1], 3)
        high = round(extra[2], 3)
        low = round(extra[3], 3)
        close = round(extra[4], 3)

        #skip 0 values
        if open == 0: continue

        volume = 0
        for p in post:
            #print (p) #(1135, 47.66113, 142.62, 1, 1) -> time, sec, price, amount, type 
            volume += p[2] * p[3]

        volume = round(volume, 3)
        out = f'{file_date} {file_hh}:{file_mm},{open},{high},{low},{close},{volume}'
        #print (out)
        f_out.write(out + '\n')
        line_counter += 1

        pass


print ('files-len: ', len(files))

stop = timeit.default_timer()
show_runtime(f'csv_export, {line_counter} lines: ', start, stop)