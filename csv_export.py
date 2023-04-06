from utils import *
from isin_groups import *
from convert import *


path = '../data'

date = None
#date = '2023-03-29'
if date is not None: path += f'/{date}'

# DE0007236101 #Siemens AG Namens-Aktien o.N.
# DE0008232125 #Lufthansa
# US83406F1021 #SoFi Technologies Inc.
# FI0009000681 #Nokia
# ----------------------
# US4581401001 #INTEL CORP. DL-,001
# NL0011821202 #ING GROEP NV EO -,01
# DE0005552004 #DEUTSCHE POST AG NA O.N.
# US02079K3059 #Alphabet Inc A
# US5949181045 #MICROSOFT DL
# DE0005552004 #DEUTSCHE POST AG NA O.N. 

isin_list = ["DE0007236101", "DE0008232125", "US83406F1021", "FI0009000681"]
isin_list += ["US4581401001", "NL0011821202", "DE0005552004", "US02079K3059", "US5949181045", "DE0005552004"]

start = timeit.default_timer()

isin_grp_dict = get_all_isin_groups()

files = list_zip_files(path, True, True)

isin_grp_data = {}
isin_out = {}

for isin in isin_list:
    isin_grp = ISIN_GROUPS_IDX.get(isin)
    isin_dict = isin_grp_dict[isin_grp]['isin_dict']

    print ('isin:', isin)
    print ('isin_grp:', isin_grp)

    isin_idx = isin_dict.get(isin)
    if isin_idx is None:
        print(f'isin {isin} not found')
        continue

    print ('isin_idx:', isin_idx)

    if isin_grp_data.get(isin_grp) is None: isin_grp_data[isin_grp] = []
    isin_grp_data[isin_grp].append(dict(isin_idx=isin_idx['id'], isin=isin))
    isin_out[isin] = 'Time,Open,High,Low,Close,Volume' + '\n'


for filepath in tqdm(files, unit=' files', unit_scale=True):
    
    basename = os.path.basename(filepath)
    tmp = basename.split('.')

    file_date = tmp[1]
    file_hh = tmp[2]
    file_mm = tmp[3]
    file_grp = tmp[4]

    if file_grp == 'pickle': file_grp = None
    if file_grp not in isin_grp_data: continue

    trade = load_from_pickle(filepath)

    isin_idx_list = isin_grp_data.get(file_grp)

    for isin_data in isin_idx_list:
        
        isin_idx = isin_data['isin_idx']
        isin = isin_data['isin']

        if len(trade) <= isin_idx: continue

        data = trade[isin_idx]

        if len(data) == 0: continue

        pre = []
        extra = []
        post = []
        if len(data) > 0: pre = data[0]
        if len(data) > 1: extra = data[1]
        if len(data) > 2: post = data[2]

        open_p = round(extra[1], 3)
        high = round(extra[2], 3)
        low = round(extra[3], 3)
        close = round(extra[4], 3)

        #skip 0 values
        if open_p == 0: continue

        volume = 0
        for p in post:
            #print (p) #(1135, 47.66113, 142.62, 1, 1) -> time, sec, price, amount, type 
            volume += p[2] * p[3]

        volume = round(volume, 3)
        out = f'{file_date} {file_hh}:{file_mm},{open_p},{high},{low},{close},{volume}'
        isin_out[isin] += out + '\n'


# writing csv files

for isin in isin_out:

    if date is not None:
        output_file = f'../data_ssd/{isin}.{date}.csv'
    else:
        output_file = f'../data_ssd/{isin}.csv'

    with open(output_file, "wt") as f_out:
        f_out.write(isin_out[isin])

print ('files-len: ', len(files))

stop = timeit.default_timer()
show_runtime(f'csv_export: ', start, stop)
