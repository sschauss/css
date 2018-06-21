from csv import reader

# Source of sentiment analysis http://wortschatz.uni-leipzig.de/de/download#sentiWSDownload
if __name__ == '__main__':
    with open('SentiWS_v1.8c_Negative.txt', 'r', encoding="utf-8") as csvfile:
        sentiment = reader(csvfile, delimiter='\t')
        for row in sentiment:
            primary, pos = row[0].split('|')
            score = float(row[1])
            alternatives = row[2].split(',') if len(row) > 2 else list()
            print("Primary " + primary)
            print("POS " + pos)
            print("Score " + str(score))
            print("Alternatives " + "; ".join(alternatives))
            print()
