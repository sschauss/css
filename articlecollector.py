import string
import requests
import json
import time
import math
from bs4 import BeautifulSoup
from html.parser import HTMLParser

api_key = "40b18ece52bb491490934f6126028ddc"
nations = ['general' ,'syria', 'iran', 'afghanistan', 'germany' , 'congo' ,'sudan', 'iraq']
year = range(2015, 2018)

for nation in nations:
    for i in year:
        zerocount = 0
        urllist = []
        articles = []
        time.sleep(1)
        if nation == 'general' :
         r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q=refugees&begin_date="+str(i)+"0101&end_date="+str(i)+"1231&api-key="+api_key)  
        else: 
         r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="+nation+"&fq=refugees&begin_date="+str(i)+"0101&end_date="+str(i)+"1231&api-key="+api_key)
    
        data = r.json()
    
        pages = math.ceil(data['response']['meta']['hits'] / 10)
        page=range(0, pages)
        
        for x in page:
            time.sleep(1)
            if nation == 'general' :
                r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q=refugees&begin_date="+str(i)+"0101&end_date="+str(i)+"1231&api-key="+api_key)
            else:
                r = requests.get("http://api.nytimes.com/svc/search/v2/articlesearch.json?q="+nation+"&fq=refugees&begin_date="+str(i)+"0101&end_date="+str(i)+"1231&page="+str(x)+"&api-key="+api_key)
            data = r.json()
            doclen = range(0,len(data['response']['docs']))
            for y in doclen:
                urllist.append(data['response']['docs'][y]['web_url'])
                try:
                 html = requests.get(data['response']['docs'][y]['web_url']).text
                except (requests.Timeout , requests.ConnectionError):
                    html =""
                    print("Timeouterror detected for: " + data['response']['docs'][y]['web_url'] )
                    pass
                soup = BeautifulSoup(html, 'html.parser')
                article= ""
                tags = soup.find_all('h1')
                tags += soup.find_all('p')
                for tag in tags:
                    article += tag.get_text() + " "
                
                if len(article) < 100:
                    zerocount +=1
                
                article = article.replace("Advertisement Supported", "")
                article = article.replace("Advertisement    Collapse SEE MY OPTIONS", "")
                article = article.replace("Advertisement", "")
                article = article.replace("\n", " ")
                article = article.replace("Go to Home Page »","")
                article = article.replace("We’re interested in your feedback on this page. Tell us what you think.", "")
                article = article.replace("Please verify you're not a robot by clicking the box.", "")
                article = article.replace("Invalid email address.", "")
                article = article.replace("Please re-enter.", "")
                article = article.replace("You must select a newsletter to subscribe to.", "")
                article = article.replace("View all New York Times newsletters.", "")
                article = article.replace("Follow The New York Times Opinion section on Facebook and Twitter", "")
                article = article.replace("(@NYTopinion)", "") 
                article = article.replace("sign up for the Opinion Today newsletter", "")
                article = article.replace("contributed reporting","")
                article = article.replace("Get politics and Washington news updates via Facebook, Twitter and in the Morning Briefing newsletter" ,"")
                article = article.replace("Page Not Found We’re sorry, we seem to have lost this page, but we don’t want to lose you. Report the broken link here" , "")   
                article = article.replace("Collapse SEE MY OPTIONS","")
                article = article.replace("Get news and analysis from Europe and around the world delivered to your inbox every day with the Today’s Headlines","")
                article = article.replace("European Morning newsletter. Sign up here","")
                article = article.replace("A version of this article appears in print on", "") 
                article = article.replace("on Page","") 
                article = article.replace("of the New York edition with the headline:","")
                article = article.replace("Order Reprints| Today's Paper|Subscribe","")
                article = article.replace("The Times welcomes comments and suggestions, or complaints about errors that warrant correction","") 
                article = article.replace("Messages on news coverage can be e-mailed to nytnews@nytimes.com or left toll-free at 1-888-NYT-NEWS (1-888-698-6397)","") 
                article = article.replace("Comments on editorials may be e-mailed to letters@nytimes.com or faxed to (212) 556-3622. Readers dissatisfied with a response or concerned about the paper’s journalistic integrity may reach the public editor at public@nytimes.com or (212) 556-7652" , "")
                article = article.replace("For newspaper delivery questions: 1-800-NYTIMES (1-800-698-4637) or e-mail customercare@nytimes.com", "")
                article = article.replace("&apos;&apos","")
                article = article.replace("in previews", "") 
                article = article.replace("opens on","")
                article = article.replace("(CTSS –","")
                article = article.replace("‘social’","")
                article = article.replace("Language Arts Standard","")
                article = article.replace("Benchmarks:","")
                article = article.replace("Benchmark:","")
                article = article.replace("informational texts","")
                articles.append(article)
                


                
        file = open("articles/"+nation+"-"+str(i)+".txt","w", encoding="utf-8")
        file.write(str(articles))
        file.close()

        file = open("urllists/"+nation+"-"+str(i)+".txt","w")
        file.write(str(urllist))
        print("urls and articles "+ str(i) + " " + nation +  " processed; zerocount: "+str(zerocount) )
        file.close()

    
    

