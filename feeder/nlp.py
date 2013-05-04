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

def prepare(text):
    words = [w for s in sent_tokenize(text) for w in word_tokenize(s)]
    words = filter(is_good_word, words)
    words = [snowball.stem(w) for w in words]
    return words
