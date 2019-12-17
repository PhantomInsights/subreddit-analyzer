"""
This script extracts features from the comments csv file and saves them to .csv files
so they can be used in any toolkkit.
"""

import csv
import random

import spacy


def main():
    """Loads the model and processes it.

    The model used can be installed by running this command on your CMD/Terminal:

    python -m spacy download es_core_news_sm

    """

    comments_list = list()

    for row in csv.DictReader(open("./mexico-comments.csv", "r", encoding="utf-8")):
        comments_list.append(row["body"])

    # We take 50,000 random comments from the comments list.
    corpus = random.sample(comments_list, 50000)
    
    nlp = spacy.load("es_core_news_sm")

    # Our corpus is bigger than the default limit, we will set
    # a new limit.
    nlp.max_length = 10000000

    get_tokens(nlp, corpus)
    get_entities(nlp, corpus)


def get_tokens(nlp, corpus):
    """Get the tokens and save them to .csv

    Parameters
    ----------
    nlp : spacy.nlp
        A nlp object.

    corpus: list
        All the comments in a list.

    """

    data_list = [["text", "text_lower", "lemma", "lemma_lower",
                  "part_of_speech", "is_alphabet", "is_stopword"]]

    for i in range(0, len(corpus), 1000):

        doc = nlp(" ".join(corpus[i:i+1000]))

        for token in doc:
            data_list.append([
                token.text, token.lower_, token.lemma_, token.lemma_.lower(),
                token.pos_, token.is_alpha, token.is_stop
            ])

    with open("./tokens.csv", "w", encoding="utf-8", newline="") as tokens_file:
        csv.writer(tokens_file).writerows(data_list)


def get_entities(nlp, corpus):
    """Get the entities and save them to .csv

    Parameters
    ----------
    nlp : spacy.nlp
        A nlp object.

    corpus: list
        All the comments in a list.

    """

    data_list = [["text", "text_lower", "label"]]
    
    for i in range(0, len(corpus), 1000):

        doc = nlp(" ".join(corpus[i:i+1000]))

        for ent in doc.ents:
            data_list.append([ent.text, ent.lower_, ent.label_])

    with open("./entities.csv", "w", encoding="utf-8", newline="") as entities_file:
        csv.writer(entities_file).writerows(data_list)


if __name__ == "__main__":

    main()
