from pattern.text.de import parse

if __name__ == '__main__':
    # Source cites POS tags as STTS tags http://wortschatz.uni-leipzig.de/de/download#sentiWSDownload
    s = parse("Die Katze liegt auf der Matte.", tagset="STTS")
    print(s)
