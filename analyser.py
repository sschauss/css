import string 
from collections import Counter
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from bs4 import BeautifulSoup
import requests
from html.parser import HTMLParser
from nltk.stem import PorterStemmer
import json
import nltk
from nltk import ngrams
import time
import collections

nations = ['iraq' ,'syria', 'iran', 'afghanistan', 'germany' , 'congo' ,'sudan', 'general']



years = range(2000,2018)
own_stopwords = ['click' ,'box','email','e-mail','re-enter' ,'...' , 'newsletter', 'subscribe', "n't" , 'nyt' , "'re" ,'go', 'get','make', '•','like' , 'also' ,'advertisementsupport', 'dec.', 'one' , 'two' ,'year' , 'new' ,'time','think', 'go', 'read', 'say', 'year', 'month', 'day', 'today' , 'Mr.', 'Mrs.', 'Ms.', 'said', 'I', '“', '”', '’', 'Mr', 'Mrs','—',"''" , "'s", "``","--" , "‘", "would" , "could"  ]
stemmer= PorterStemmer()
stop_list = stopwords.words('english')+ list(string.punctuation) +own_stopwords
poswordlist= ""
negwordlist = ""

sentimentanalyse = open("results/sentimentanalyse.csv", "w", encoding="utf-8")
sentimentanalyse.write("Year , Nation , relational Sentiment" + "\n")

file =open("positive-words-cut.txt","r")
for line in file:
    for word in line.split():
        poswordlist += " "+ word
file.close()

file =open("negative-words-cut.txt","r")
for line in file:
    for word in line.split():
        negwordlist += " "+ word
file.close()

pos_tokens = [t for t in word_tokenize(poswordlist) if t not in stop_list]
neg_tokens = [t for t in word_tokenize(negwordlist) if t not in stop_list]



for nation in nations:
        
    for year in years:
        
        articlelist= []
        article=""
        file = open("articles/"+nation+"-"+str(year)+".txt","r", encoding="utf-8")
        articlelist = eval(file.readline())
        file.close()
        file = open("results/"+nation+"-"+str(year)+".csv","w", encoding="utf-8")
        for i in articlelist:
            
            article += i + " "
        
        try:
            all_tokens = [t for t in word_tokenize(article) if t not in stop_list]
        except UnicodeDecodeError:
            all_tokens = [t for t in word_tokenize(article.decode('utf8'))]

        
        all_tokens = [token for token in all_tokens if token not in stop_list]
        all_tokens = [t.lower() for t in all_tokens]
        all_tokens_pos =[token for token in all_tokens if token in pos_tokens]
        all_tokens_neg =[token for token in all_tokens if token in neg_tokens]
        
        posterm_freq = Counter(all_tokens_pos)
        negterm_freq = Counter(all_tokens_neg)
        
        sentiment = len(all_tokens_pos) - len(all_tokens_neg)
        
        
        
        all_tokens = [stemmer.stem(t) for t in all_tokens if t not in stop_list]
        
        term_freq = Counter(all_tokens)
        phrases = Counter(ngrams(all_tokens,2))

        file.write("Word , relational frequency , absolute frequency"  +"\n")
        for word, freq in term_freq.most_common(100):
            file.write(word + " , " + str(freq/len(all_tokens))+ " ," + str(freq) +"\n")
        
        file.write("\n" + "positive sentimentwords:" +"\n")
        for word, freq in posterm_freq.most_common(50):
            file.write(word + " , " + str(freq/len(all_tokens))+" ,"+ str(freq)+"\n")

        file.write("\n" + "negative sentimentwords:" +"\n")
        for word, freq in negterm_freq.most_common(50):
            file.write(word +" , " + str(freq/len(all_tokens))+" ,"+ str(freq)+"\n")
        
       
        file.write("\n" + "common phrases:" + "\n")
        for phrase, freq in phrases.most_common(50):
            file.write(str(phrase[0]) + " " + str(phrase[1]) + " , " + str(freq)+ " , "+ str(freq)+ "\n")

       
        file.write("\n" + "total sentiment: positives: " + str(len(all_tokens_pos)) +" negatives: "+str(len(all_tokens_neg))+ " relational sentimentvalue: " + str(sentiment/len(all_tokens))+"\n")
        print("Results "+nation+" "+str(year)+" processed")
        sentimentanalyse.write(str(year) + " , "+ nation +" , "+ str(sentiment/len(all_tokens))+ "\n")
        
       
        file.close()
        
        
sentimentanalyse.close()      

        
    
        
 


 

 


 