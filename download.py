import urllib.request

from tqdm import tqdm


class DownloadProgressBar(tqdm):
    def update_to(self, b=1, bsize=1, tsize=None):
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)


def download_url(url, output_path):

    opener = urllib.request.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    urllib.request.install_opener(opener)

    with DownloadProgressBar(unit='B', unit_scale=True,
                             miniters=1, desc=url.split('/')[-1]) as t:
        urllib.request.urlretrieve(url, filename=output_path, reporthook=t.update_to)



#Download pretrade csv.gz
#++++++++++++++++++++++++++++++++++++++++++++++++++++++
#date = "20230116"
#time = "10.30"
#url = "https://erdk.bayerische-boerse.de/?u=edd-MUNCD&p=public&path=/pretrade/pretrade."+date+"."+time+".munc.csv.gz"
#output_path = "../data/pretrade."+date+"."+time+".munc.csv.gz"
#download_url(url, output_path)
#---------------------------------------------------------


#download_url('https://www.gettex.de/handel/delayed-data/pretrade-data/', '../pretrade-data.html')
#download_url('https://www.gettex.de/fileadmin/rts27/MUNC-MUND/2023/01/20230111_MUNC.zip', '../data/20230111_MUNC.zip')     
#download_url('https://www.gettex.de/fileadmin/rts27/MUNC-MUND/2023/01/20230111_MUND.zip', '../data/20230111_MUND.zip')     
#download_url('https://www.google.de', '../data/test2.html')     