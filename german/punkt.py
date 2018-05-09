from os.path import dirname, join, exists
from pickle import dump, load

from nltk.corpus import gutenberg
from nltk.tokenize.punkt import PunktTrainer, PunktSentenceTokenizer

# File that stores the trained model.
params_file = join(dirname(__file__), 'punkt-de')

# If punctuation parameters do not exist yet, rebuild, this requires gutenberg and punkt packages to be installed.
if not exists(params_file):
    print('Punctuation params do not exist, rebuilding.')
    text = ''
    for file_id in gutenberg.fileids():
        text += gutenberg.raw(file_id)

    print('Got model (Gutenberg).')

    trainer = PunktTrainer()
    trainer.INCLUDE_ALL_COLLOCS = True
    trainer.train(text)

    print('Trained model.')

    original = trainer.get_params()

    print('Saving params.')

    dump(original, open(params_file, 'wb+'))

# Tokenizer for string tokenization based on German model.
tokenizer = PunktSentenceTokenizer(load(open(params_file, 'rb')))


def sentences_german(input):
    """
    Splits input for individual sentences, based on german language model trained on Gutenberg NLTK corpus.
    :param input: The text to split.
    :return: Returns the split input.
    """
    return tokenizer.tokenize(input)
