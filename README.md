[![Binder](https://mybinder.org/badge.svg)](https://mybinder.org/v2/gh/sschauss/css/master)

# Group 9 Refugees/Foreigners in Media

## About

This project concentrates on articles published on Spiegel Online from 01.01.2000 - 05.05.2018 for the sentiment analysis.
The code for crawling the articles from Spiegel Online is contained in `spiegel_scraper.py`.
Everything else (except the SentiWS files and the trained punkt tokenizer model) is contained in the `notebook.ipynb`.
As we use pyspark to parallelize the process of crawling and the sentiment analysis sufficient memory should be provided.
If you want to use the mybinder.org link you need to decrease the sample size, as they only assure you 1GB of memory.
