from sklearn.feature_extraction.text import TfidfVectorizer


class TFIDF:

    def __init__(self):
        self.tfidf = TfidfVectorizer(binary=True)

    def tfidf_features(self, txt, flag):
        if flag == "train":
            x = self.tfidf.fit_transform(txt)
        else:
            x = self.tfidf.transform(txt)
        return x
