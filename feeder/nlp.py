from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem.snowball import EnglishStemmer
from nltk.corpus import stopwords


snowball = EnglishStemmer()
stop = filter(lambda w: len(w) > 2, stopwords.words('english'))

def has_char(w):
    for c in w:
        if c.isalpha(): return True
    return False

def is_good_word(w):
    return len(w) > 2 and w not in stop and has_char(w)

def tokenise(text):
    return [w for s in sent_tokenize(text) for w in word_tokenize(s)]

def normalise(words):
    words = enumerate(words)
    words = filter(lambda p: is_good_word(p[1]), words)
    words = [(i, snowball.stem(w)) for i, w in words]
    return words
