from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

sia = SIA()


def get_sentiment(text):
    scores = sia.polarity_scores(text)
    return scores['compound']


# import nltk
# import ssl

# try:
#     _create_unverified_https_context = ssl._create_unverified_context

# except AttributeError:
#     pass
# else:
#     ssl._create_default_https_context = _create_unverified_https_context

# nltk.download('vader_lexicon')