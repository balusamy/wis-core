from nltk.data import load
from nltk.tokenize.treebank import TreebankWordTokenizer
from nltk.tokenize.api import TokenizerI
from nltk.stem.snowball import EnglishStemmer
from nltk.corpus import stopwords
import re


class BetterTreebankWordTokenizer(TreebankWordTokenizer):
    def tokenize(self, text):
        #punctuation
        text = re.sub(r'([:,])([^\d])', r' \1 \2', text)
        text = re.sub(r'\.\.\.', r' ... ', text)
        text = re.sub(r'[;@#$%&]', r' \g<0> ', text)
        text = re.sub(r'([^\.])(\.)([\]\)}>"\']*)\s*$', r'\1 \2\3 ', text)
        text = re.sub(r'[?!]', r' \g<0> ', text)

        text = re.sub(r"([^'])' ", r"\1 ' ", text)

        #parens, brackets, etc.
        text = re.sub(r'[\]\[\(\)\{\}\<\>]', r' \g<0> ', text)
        text = re.sub(r'--', r' -- ', text)

        #add extra space to make things easier
        text = " " + text + " "

        #quotes
        text = re.sub(r"(\"|''|``)", r' \1 ', text)

        text = re.sub(r"([^' ])('[sS]|'[mM]|'[dD]|') ", r"\1 \2 ", text)
        text = re.sub(r"([^' ])('ll|'re|'ve|n't|) ", r"\1 \2 ", text)
        text = re.sub(r"([^' ])('LL|'RE|'VE|N'T|) ", r"\1 \2 ", text)

        for regexp in self.CONTRACTIONS2:
            text = regexp.sub(r' \1 \2 ', text)
        for regexp in self.CONTRACTIONS3:
            text = regexp.sub(r' \1 \2 ', text)

        # We are not using CONTRACTIONS4 since
        # they are also commented out in the SED scripts
        # for regexp in self.CONTRACTIONS4:
        #     text = regexp.sub(r' \1 \2 \3 ', text)

        text = re.sub(" +", " ", text)
        text = text.strip()

        #add space at end to match up with MacIntyre's output (for debugging)
        if text != "":
            text += " "

        return text.split()


class AwesomeTokenizer(TokenizerI):
    def __init__(self, tokeniser):
        self._toker = tokeniser

    def span_tokenize(self, text):
        pos = 0

        for token in self._toker.tokenize(text):
            s = text.index(token[0], pos)
            yield (s, s + len(token))
            pos = s + len(token)


sent_tokeniser = load('tokenizers/punkt/english.pickle')
word_tokeniser = AwesomeTokenizer(BetterTreebankWordTokenizer())

snowball = EnglishStemmer()
stop = filter(lambda w: len(w) > 2, stopwords.words('english')) + [
            "n't", 'amp'
       ]

def has_char(w):
    for c in w:
        if c.isalpha(): return True
    return False

def is_good_word(w):
    return len(w) > 2 and has_char(w)

def itokenise(text):
    for s_s, s_e in sent_tokeniser.span_tokenize(text):
        for w_s, w_e in word_tokeniser.span_tokenize(text[s_s:s_e]):
            yield (s_s + w_s, s_s + w_e)

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

def normalise_gently(words):
    words = map(lambda w: w.lower(), words)
    return words

def stem(w):
    return snowball.stem(w)
