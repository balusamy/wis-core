from nltk.data import load
from nltk.tokenize.treebank import TreebankWordTokenizer
from nltk.tokenize.api import TokenizerI
from nltk.stem.snowball import EnglishStemmer
from nltk.corpus import stopwords
import re


class AwesomeTokenizer(TokenizerI):
    def __init__(self, tokeniser):
        self._toker = tokeniser

    @staticmethod
    def prepare(text):
        ## They'll do those substitutions, so I'm doing them in advance
        ## to be able to find separate tokens
        text = re.sub(r'^\"', r'``', text)
        text = re.sub(r'([ (\[{<])"', r'\1 `` ', text)
        text = re.sub(r'"', " '' ", text)
        return text

    def span_tokenize(self, text):
        pos = 0

        for token in self._toker.tokenize(text):
            s = text.index(token[0], pos)
            yield (s, s + len(token))
            pos = s + len(token)


sent_tokeniser = load('tokenizers/punkt/english.pickle')
word_tokeniser = AwesomeTokenizer(TreebankWordTokenizer())

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

def itokenise(text):
    text = word_tokeniser.prepare(text)
    def gen():
        for s_s, s_e in sent_tokeniser.span_tokenize(text):
            for w_s, w_e in word_tokeniser.span_tokenize(text[s_s:s_e]):
                yield (s_s + w_s, s_s + w_e)
    return (text, gen())

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
