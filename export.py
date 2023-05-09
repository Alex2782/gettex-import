from utils import *
from isin_groups import *
from convert import *
from finanzen_net import *

path = '../data'

date = None
date = '2023-05-08'
if date is not None: path += f'/{date}'

#TODO from_date, to_date, binary-IO
# https://pandas.pydata.org/docs/user_guide/io.html
# https://pandas.pydata.org/docs/user_guide/io.html#io-perf
from_date = '2023-04-13'
to_date = '2023-04-23'


isin_list = ["US88160R1014"] #DEBUG-Test Tesla

# get all isin from type 'stock' ('AKTIE', check: finanzen_net.py)
#isin_list = load_dict_data()['AKTIE']['isin_list']
#print ('isin_list - len:', len(isin_list))


start = timeit.default_timer()

isin_grp_dict = get_all_isin_groups()
files = list_zip_files(path, True, True)

ignore_group_list = ['HSBC', 'Goldman_Sachs', 'UniCredit']

isin_grp_data = {}
isin_out = {}

for isin in isin_list:

    isin_grp = get_isin_group(isin)

    if isin_grp in ignore_group_list:
        print (f'IGNORE isin {isin}, GROUP: {isin_grp}')
        continue

    isin_dict = isin_grp_dict[isin_grp]['isin_dict']

    #print ('isin:', isin)
    #print ('isin_grp:', isin_grp)

    isin_idx = isin_dict.get(isin)
    if isin_idx is None:
        print(f'isin {isin} not found')
        continue

    #print ('isin_idx:', isin_idx)

    if isin_grp_data.get(isin_grp) is None: isin_grp_data[isin_grp] = []
    isin_grp_data[isin_grp].append(dict(isin_idx=isin_idx['id'], isin=isin))
    isin_out[isin] = 'Date,HH,MM,Open,High,Low,Close,Volume,Volume_Ask,Volume_Bid,' \
                     +'no_pre_bid,no_pre_ask,no_post,vola_profit,bid_long,bid_short,ask_long,ask_short' \
                     +'\n'


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

        # pre-data
        #idx 0 = timestamp in minutes of the day
        #idx 1, 2 = bid_size: max, min
        #idx 3, 4 = ask_size: max, min
        #idx 5, 6 = spread: max, min
        #idx 7, 8, 9, 10 = price: open, high, low, close
        #idx 11 = activity: count data
        #idx 12, 13 = volatility: long, short
        #idx 14, 15, 16 = volatility activity: long, short, equal (no changes)
        #idx 17, 18 = no values counter: bid, ask  (price or size = 0)        
        #TODO

        # extra-data
        # [counter, open,high,low,close, no-pre-bid,no-pre-ask,no-post, vola_profit, bid_long,bid_short,ask_long,ask_short]
        open_p = round(extra[1], 3)
        high = round(extra[2], 3)
        low = round(extra[3], 3)
        close = round(extra[4], 3)

        no_pre_bid = extra[5]
        no_pre_ask = extra[6]
        no_post = extra[7]
        vola_profit = extra[8]
        bid_long = extra[9]
        bid_short = extra[10]
        ask_long = extra[11]
        ask_short = extra[12]

        #skip 0 values
        if open_p == 0: continue

        volume = 0
        volume_ask = 0
        volume_bid = 0

        for p in post:
            #print (p) #(1135, 47.66113, 142.62, 1, 1) -> time, sec, price, amount, type (1 = ask, 2 = bid)
            v = round(p[2] * p[3], 3)
            volume += v
            type = p[4]
            if type == 1: volume_ask += v
            elif type == 2: volume_bid += v

        volume = round(volume, 3)
        #out = f'{file_date} {file_hh}:{file_mm},{open_p},{high},{low},{close},{volume}'
        out = f'{file_date},{file_hh},{file_mm},{open_p},{high},{low},{close},{volume},{volume_ask},{volume_bid},'\
              + f'{no_pre_bid},{no_pre_ask},{no_post},{vola_profit},{bid_long},{bid_short},{ask_long},{ask_short}'
        isin_out[isin] += out + '\n'


# writing csv files

_MIN_SIZE_IN_KB = 0 # export only > 10 KB files 

for isin in isin_out:

    out_len = len(isin_out[isin]) / 1024
    if  out_len < _MIN_SIZE_IN_KB: 
        print (f'IGNORE isin {isin}, size: {out_len:.3f}')
        continue 

    if date is not None:
        output_file = f'../data_ssd/{isin}.{date}.csv'
    else:
        output_file = f'../data_ssd/{isin}.csv'

    with open(output_file, "wt") as f_out:
        f_out.write(isin_out[isin])

print ('files-len: ', len(files))

stop = timeit.default_timer()
show_runtime(f'csv_export: ', start, stop)
