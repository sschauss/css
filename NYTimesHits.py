import time
import json
import requests
nations= ['Syria','Germany','Afghanistan', 'Iraq', 'Iran', 'Palestine', 'Mexico' , 'Africa' , 'Libya' , 'Sudan' , 'Somalia' , 'Yemen' , 'Arabia' , 'North Korea']
fil = "refugees"
startyear = 1930
endyear = 2017
year = range(startyear,endyear+1)
for x in year:
    for i in nations:
      time.sleep(0.5)
      r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="+i+"&fq="+fil+"&begin_date="+str(x)+"0101&end_date="+str(x)+"1231&api-key=40b18ece52bb491490934f6126028ddc")
      time.sleep(0.5)
      data= r.json()
      print(""+str(x)+": "+i+" "+str(data['response']['meta']['hits']))
    time.sleep(0.5)
    r=r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="+fil+"&begin_date="+str(x)+"0101&end_date="+str(x)+"1231&api-key=40b18ece52bb491490934f6126028ddc")
    time.sleep(0.5)
    data= r.json()
    print(""+str(x)+": General "+str(data['response']['meta']['hits'])) 
time.sleep(500)  