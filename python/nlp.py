from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem.snowball import EnglishStemmer
from nltk.corpus import stopwords


snowball = EnglishStemmer()
stop = filter(lambda w: len(w) > 2, stopwords.words('english')) + [
            "n't",
       ]

def has_char(w):
    for c in w:
        if c.isalpha(): return True
    return False

def is_good_word(w):
    return len(w) > 2 and has_char(w)

def tokenise(text):
    return [w for s in sent_tokenize(text) for w in word_tokenize(s)]

def normalise(words):
    words = enumerate(words)
    words = filter(lambda p: is_good_word(p[1]), words)
    words = [(i, w.lower()) for i, w in words]
    words = filter(lambda p: p[1] not in stop, words)
    words = [(i, snowball.stem(w)) for i, w in words]
    return words

def normalise_drop(words):
    words = filter(is_good_word, words)
    words = map(lambda w: w.lower(), words)
    words = filter(lambda w: w not in stop, words)
    words = map(snowball.stem, words)
    return words

def stem(w):
    return snowball.stem(w)
