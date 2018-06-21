from pattern.text.de import parse

from german.punkt import sentences_german

if __name__ == '__main__':
    # Source cites POS tags as STTS tags http://wortschatz.uni-leipzig.de/de/download#sentiWSDownload
    source = "Die Katze liegt auf der Matte. Sie ist aus Gold gemacht."
    sentences = sentences_german(source)
    for s in sentences:
        print("Sentence: " + s)
        print(parse(s, tagset="STTS"))
