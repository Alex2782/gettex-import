from utils import *
from isin_groups import *
from convert import *
from finanzen_net import *
import datetime
import pandas as pd

path = '../data/'
#path = '../data_test/' #DEV-TEST

from_date = '2023-05-15'
number_of_days = 10

isin_list = []
isin_list += ["US88160R1014"] #DEBUG-Test Tesla
#isin_list += ['US83304A1060','US12468P1049','US19260Q1076','US62914V1061','US86771W1053']

#auf NASDAQ 100 [US6311011026] Long Hebel 20x
#isin_list += ['DE000GK70097','DE000GP4ADR4','DE000GP1J3M8']

#auf NASDAQ 100 [US6311011026] Short Hebel 20x
#isin_list += ['DE000HB5RRM1','DE000GZ7M7K1','DE000HG2LRA1']

#auf DAX [DE0008469008] Knock-Out ohne Stop-Loss Hebel 20 Long
#isin_list += ['DE000HC5J8K3','DE000GP1AD87','DE000HG88UU8']

#auf DAX [DE0008469008] Knock-Out ohne Stop-Loss Hebel 20 Short
#isin_list += ['DE000GP27435','DE000GZ920Z0','DE000HR6YUZ3']



# get all isin from type 'stock' ('AKTIE', check: finanzen_net.py)
#isin_list = load_dict_data()['AKTIE']['isin_list']
#print ('isin_list - len:', len(isin_list))

start = timeit.default_timer()
date = datetime.datetime.strptime(from_date, '%Y-%m-%d')


#prepare Output
isin_grp_dict = get_all_isin_groups()
ignore_group_list = [] #['HSBC', 'Goldman_Sachs', 'UniCredit']

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
    isin_out[isin] = []
    #isin_out[isin] = 'Date,HH,MM,Open,High,Low,Close,Volume,Volume_Ask,Volume_Bid,' \
    #                +'no_pre_bid,no_pre_ask,no_post,vola_profit,bid_long,bid_short,ask_long,ask_short' \
    #                +'\n'



for i in range(number_of_days):

    str_date = f'{date.strftime("%Y-%m-%d")}'
    sub_path = str_date + '/'
    date += datetime.timedelta(days=1)
    tmp_path = path + sub_path

    if not os.path.exists(tmp_path): continue

    files = list_zip_files(tmp_path, True, True)
    files = sorted(files)

    for filepath in tqdm(files, unit=' files', unit_scale=True, desc=str_date):
        
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
            #out = f'{file_date},{file_hh},{file_mm},{open_p},{high},{low},{close},{volume},{volume_ask},{volume_bid},'\
            #    + f'{no_pre_bid},{no_pre_ask},{no_post},{vola_profit},{bid_long},{bid_short},{ask_long},{ask_short}'
            
            # pre-data
            #idx 0 = timestamp in minutes of the day
            #idx 1, 2 = bid_size: max, min
            #idx 3, 4 = ask_size: max, min
            #idx 5, 6 = spread: max, min
            #idx 7, 8, 9, 10 = price: open, high, low, close
            #idx 11 = activity: count data
            #idx 12, 13 = volatility: long, short
            #idx 14, 15, 16 = volatility activity: long, short, equal (no changes)      
            for p in pre:
                p = list(p)
                p[0] = timestamp_to_strtime(p[0])
                tmp = [int(file_date)] + p
                isin_out[isin].append(tmp)


            #out = (file_date,file_hh,file_mm,open_p,high,low,close,volume,volume_ask,volume_bid,
            #        no_pre_bid,no_pre_ask,no_post,vola_profit,bid_long,bid_short,ask_long,ask_short)
            #isin_out[isin].append(out)




# writing files
#columns = ['date','hh','mm','open','high','low','close','volume','volume_ask','volume_bid',
#            'no_pre_bid','no_pre_ask','no_post','vola_profit','bid_long','bid_short','ask_long','ask_short']

columns = ['date','time','bid_size_max','bid_size_min','ask_size_max','ask_size_min','spread_max','spread_min',
            'open','high','low','close', 'activity', 'volatility_long', 'volatility_short', 
            'vola_activity_long','vola_activity_short','vola_activity_equal']

for isin in isin_out:

    data = isin_out[isin]
    out_len = len(data)

    df = pd.DataFrame(data, columns=columns)
    tm = df['date'].astype(str) + ' ' + df['time'].astype(str)
    tm_data = pd.to_datetime(tm).dt.tz_localize('UTC')
    df.insert(0, 'datetime', tm_data)
    df.set_index('datetime', inplace=True)

    del df['date']
    del df['time']

    #output_file = f'../data_ssd/{isin}.csv'
    output_file = f'../data_ssd/{isin}.feather'

    # concat if exists, remove duplicated timestamps
    if os.path.exists(output_file):
        df_old = pd.read_feather(output_file)
        df_old.set_index('datetime', inplace=True)
        df_old = df_old[~df_old.index.isin(df.index)]
        df = pd.concat([df_old, df], ignore_index=False)
 

    #df.to_csv(output_file)
    df.reset_index(inplace=True)
    df.to_feather(output_file)

stop = timeit.default_timer()
show_runtime(f'export: ', start, stop)
