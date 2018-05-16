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
"""
Hey, wie ihr seht ist das ganze ziemlich hässlich geworden, aber wie gesagt, meine Coding-Skills sind ziemlich beschränkt.
Ich hab als Ausgabe einfach nen kleinen String gemacht um mir die Werte mal direkt anzuschauen, dürfte aber kein Problem sein die irgendwie
als CSV oder was auch immer zu speichern, je nachdem ob und wie wir damit weiterarbeiten wollen.
Das einzige Problem bei der Funktionalität ist in den time.sleep() Funktionen, lass ich diese weg (bis auf die letzte) bekomm ich 2-3 Werte 
ausgegeben und danach geht die ganze Kiste den Bach runter. Wieso ist mir nicht klar und ich war grade zu faul da noch errorhandler mit 
einzubauen ;) Vielleicht wisst ihr ja wo da der Fehler liegt.
"""
