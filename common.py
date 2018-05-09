from urllib.request import urlopen
from traceback import format_exc
from itertools import chain

def download(url):
    try:
        return urlopen(url).read().decode()
    except Exception as e:
        print('download of {} failed ({})'.format(url, format_exc()))
        
def flatten(ll):
    return list(chain(*ll))
