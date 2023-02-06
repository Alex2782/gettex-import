from read import *


# ------------------------------------------------------------------------------------
# analyze_activity
# ------------------------------------------------------------------------------------
def analyze_activity(file, group, min_count = 1, max_count = 9999, max_vola = 9999):

    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = 0
    isin_list = []

    for isin_data in arr:

        if len(isin_data) == 0: 
            isin_idx += 1
            continue

        try:
            d = isin_data[0]
            extra = isin_data[1]

            count = extra[0]
            open = extra[1]
            high = extra[2]
            low = extra[3]
            close = extra[4]

            vola = high - low

            #print (isin_dict_idx[isin_idx], 'vola:', vola, 'max_vola', max_vola)

            #skip min_count / max_count
            if count > max_count or count < min_count: 
                isin_idx += 1
                continue
            elif vola < max_vola:
                #print ("APPEND")
                isin_list.append(isin_dict_idx[isin_idx])

        except Exception as e:
            print(e, isin_data, 'isin_idx: ', isin_idx)
            isin_idx += 1
            continue
        
        isin_idx += 1
    #END for loop ---------------------------------------------------------------

    print(isin_list)
    print('=====================')
    print('isin_list len:', len(isin_list))



# ------------------------------------------------------------------------------------
# show_row_data
# ------------------------------------------------------------------------------------
def show_row_data(file, group, isin):
    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = isin_dict[isin]['id']

    try:
        d = arr[isin_idx][0]
        extra = arr[isin_idx][1]
    except Exception as e:
        print(e, arr[isin_idx], 'isin_idx: ', isin_idx)

    print('ISIN: ', isin)
    print('extra:', extra)
    print('======================================')
    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)
    print('======================================')

# ------------------------------------------------------------------------------------
# analyze_max_long_max_short
# ------------------------------------------------------------------------------------
def analyze_max_long_max_short(file, group, max_open_price = 3):

    #isin dictionary
    isin_grp_dict = get_all_isin_groups()
    arr = load_from_pickle(file)

    print(file, ', data len:', len(arr))

    isin_dict = isin_grp_dict[group]['isin_dict']
    isin_dict_idx = isin_grp_dict[group]['isin_dict_idx']

    isin_idx = 0
    max_vola_long = 0
    max_vola_short = 0
    isin_idx_max_vola_long = 0
    isin_idx_max_vola_short = 0

    for isin_data in arr:

        if len(isin_data) == 0: 
            isin_idx += 1
            continue

        try:
            d = isin_data[0]
            extra = isin_data[1]

            #skip price > 3 EURO 
            trade_dict = trade_list_to_dict(d[0])
            if trade_dict['price_open'] > max_open_price: 
                isin_idx += 1
                continue

        except Exception as e:
            print(e, isin_data, 'isin_idx: ', isin_idx)
            isin_idx += 1
            continue


        #if isin_idx != 37135:
        #    isin_idx += 1
        #    continue

        #print('isin:', isin_dict_idx[isin_idx], isin_idx)    
        #print('extra:', extra)


        sum_long = 0
        sum_short = 0
        sum_vola_long = 0
        sum_vola_short = 0
        sum_vola_open_close_long = 0
        sum_vola_open_close_short = 0

        prev_trade = False
        for trade in d:

            #print(timestamp_to_strtime(trade[0]), trade)
            sum_vola_long += trade[12]
            sum_vola_short += trade[13]

            open = trade[7]
            close = trade[10]

            if prev_trade :
                vola =  round(open - prev_trade[10], 3)
                if vola > 0: sum_vola_open_close_long += vola
                else: sum_vola_open_close_short += vola

            prev_trade = trade
        
        #print('----------------------------------------------')
        sum_vola_long = round(sum_vola_long, 3)
        sum_vola_short = round(sum_vola_short, 3)
        sum_vola_open_close_long = round(sum_vola_open_close_long, 3)
        sum_vola_open_close_short = round(sum_vola_open_close_short, 3)

        #print('sum_vola long / short           :', sum_vola_long,  sum_vola_short)
        #print('sum_vola_open_close long / short:', sum_vola_open_close_long, sum_vola_open_close_short)

        sum_long = round(sum_vola_open_close_long + sum_vola_long, 3)
        #print('sum_long:', sum_long)

        sum_short = round(sum_vola_open_close_short + sum_vola_short, 3)
        #print('sum_short:', sum_short)

        sum = round(sum_vola_open_close_long + sum_vola_long + sum_vola_open_close_short + sum_vola_short, 3)
        #print('sum:', sum)

        if sum_long > max_vola_long:
            #print ('sum_long > max_vola_long', sum_long, max_vola_long, ', isin_idx:', isin_idx)
            max_vola_long = sum_long
            isin_idx_max_vola_long = isin_idx

        if sum_short < max_vola_short:
            #print ('sum_short < max_vola_short', sum_short, max_vola_short, ', isin_idx:', isin_idx)
            max_vola_short = sum_short
            isin_idx_max_vola_short = isin_idx

        isin_idx += 1

        #if isin_idx > 10: break
        

    print('-----------------------------------------------------------------------')
    print ('MAX_LONG:', max_vola_long)
    print('isin:', isin_dict_idx[isin_idx_max_vola_long], 'isin_idx:', isin_idx_max_vola_long)    
    print('extra:', arr[isin_idx_max_vola_long][1])
    print ('=======================')
    d = arr[isin_idx_max_vola_long][0]

    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)

    print('------------------------')

    print ('MAX_SHORT:', max_vola_short)
    print('isin:', isin_dict_idx[isin_idx_max_vola_short], 'isin_idx:', isin_idx_max_vola_short)    
    print('extra:', arr[isin_idx_max_vola_short][1])
    print ('=======================')
    d = arr[isin_idx_max_vola_short][0]

    for trade in d:
        print(timestamp_to_strtime(trade[0]), trade)

    print('------------------------')

#================================================================================================


#TODO: genauer überprüfen, Zusammenfassung möglich?
group = 'Goldman_Sachs'
file = f'../data.{group}.pickle.zip'

#analyze_max_long_max_short(file, group, 2)

min_count = 10
max_count = 9999
max_vola = 0.01
#analyze_activity(file, group, min_count, max_count, max_vola)


# TEST Data
#  'DE000GX6RGY2', 'DE000GZ0DMC7', 'DE000GX5JPY2', 'DE000GX49664', 'DE000GZ18TQ7', 
# 'DE000GK0TF29', 'DE000GZ5YEQ9', 'DE000GX1V5B4', 'DE000GH7N0B2', 'DE000GK4HD54', 
# 'DE000GH68AV0', 'DE000GX0W752', 'DE000GK4RVD5', 'DE000GK6J1L0', 'DE000GK3FAJ3', 
# 'DE000GH8K6Q9', 'DE000GX8WWV1', 'DE000GZ6LPM9', 'DE000GK4X7U2', 'DE000GZ3MGN1', 
# 'DE000GZ6J606', 'DE000GX4X9J4', 'DE000GK9URK1', 'DE000GZ0GRZ0', 'DE000GZ01CV9', 'DE000GK9FT20', 'DE000GZ6UK13', 
# 'DE000GK5H7Q9', 'DE000GZ653V6', 'DE000GZ0QJ83', 'DE000GZ4FRM2', 'DE000GZ3UU89'
isin = 'DE000GX5JPY2'
show_row_data(file, group, isin)







exit()
# Maximalen Wert aus der Spalte 'bid_size' ermitteln
max_bid_size = np.amax(x['bid_size'])
print("Maximaler Wert in bid_size:", max_bid_size)

# Maximalen Wert aus der Spalte 'ask_size' ermitteln
max_ask_size = np.amax(x['ask_size'])
print("Maximaler Wert in ask_size:", max_ask_size)


# Maximalen Wert aus der Spalte 'bid' ermitteln
max_bid_size = np.amax(x['bid'])
print("Maximaler Wert in bid:", max_bid_size)

# Maximalen Wert aus der Spalte 'ask' ermitteln
max_ask_size = np.amax(x['ask'])
print("Maximaler Wert in ask:", max_ask_size)


max_bid_index = np.argmax(x['bid'])
isin_idx = x[max_bid_index]['isin_idx']
print("Maximaler Wert in bid:", x[max_bid_index]['bid'], 'isin_idx:', isin_idx)
print("ISIN:", isin_dict_idx.get(isin_idx))

max_ask_index = np.argmax(x['ask'])
isin_idx = x[max_ask_index]['isin_idx']
print("Maximaler Wert in ask:", x[max_ask_index]['ask'], 'isin_idx:', isin_idx)
print("ISIN:", isin_dict_idx.get(isin_idx))

indices = np.where(x['isin_idx'] == 44)
print('data isin_idx = 44:', x[indices])