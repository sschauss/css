import string
import requests
import json
import time
import math
from datetime import datetime, timedelta

api_key = "40b18ece52bb491490934f6126028ddc"

file  = open("urlstimes.csv","w")
def generate_dates(min_date):
    delta = datetime.today() - min_date
    return [min_date + timedelta(days = n) for n in range(delta.days)]



dates = generate_dates (datetime(2000,1,1)) 

for date in dates:

    date1=date.strftime('%Y%m%d')
    date2=date+timedelta(days=1)
    date2=date2.strftime('%Y%m%d')
    
       


    time.sleep(1)
    r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q=refugees&begin_date="+str(date1)+"&end_date="+str(date2) +"&api-key="+api_key)
    data = r.json()
    pages = math.ceil(data['response']['meta']['hits'] / 10)
    page = range(0,pages)
    for x in page :
        time.sleep(1)
        r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q=refugees&begin_date="+str(date1)+"&end_date="+str(date2)+"&page="+str(x)+"&api-key="+api_key)
        data = r.json()
        doclen = range(0,len(data['response']['docs']))
        for y in doclen:
            urldate = []
            urldate.append(data['response']['docs'][y]['web_url'])
            pubdate=(data['response']['docs'][y]['pub_date'])
            pubdate=pubdate.split('T',1)[0]
            urldate.append(pubdate)
            file.write(str(urldate) + ";")
file.close()
