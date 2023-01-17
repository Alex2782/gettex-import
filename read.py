import gzip

with gzip.open('../data/posttrade.20230111.21.00.munc.csv.gz', 'rb') as f:

    line = f.readline()

    while line:
        print(line)
        data = str(line).replace("b'", "").replace("\\n'", '')
        arr = data.split(',')
        print(arr)

        line = f.readline()

        

exit()
#=================================================================================
print("----------------------")

with gzip.open('../data/pretrade.20230111.21.00.munc.csv.gz', 'rb') as f:

    line = f.readline()

    while line:
        print(line)
        line = f.readline()
        