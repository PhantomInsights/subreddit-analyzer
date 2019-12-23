"""
This script contains several functions that will create plots and generate insights from
the 4 datasets (submissions, comments, tokens and entities).
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import wordcloud
from pandas.plotting import register_matplotlib_converters
from PIL import Image

register_matplotlib_converters()

sns.set(style="ticks",
        rc={
            "figure.figsize": [12, 7],
            "text.color": "white",
            "axes.labelcolor": "white",
            "axes.edgecolor": "white",
            "xtick.color": "white",
            "ytick.color": "white",
            "axes.facecolor": "#222222",
            "figure.facecolor": "#222222"}
        )


MASK_FILE = "./assets/cloud.png"
FONT_FILE = "./assets/sofiapro-light.otf"
EN_STOPWORDS = "./assets/stopwords-en.txt"
ES_STOPWORDS = "./assets/stopwords-es.txt"


def get_most_common_domains(df):
    """Prints the 20 most frequent domains from submissions.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    """

    df = df["domain"].value_counts()[0:20]
    print(df)


def get_most_common_submitters(df):
    """Prints the 20 most frequent submitters.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    """

    # Optional: Remove the [deleted] user.
    df.drop(df[df["author"] == "[deleted]"].index, inplace=True)

    df = df["author"].value_counts()[0:20]
    print(df)


def get_most_common_commenters(df):
    """Prints the 20 most frequent commenters.

    Parameters
    ----------
    df : pandas.DataFrame
        The comments DataFrame.

    """

    # Optional: Remove the [deleted] user.
    df.drop(df[df["author"] == "[deleted]"].index, inplace=True)

    df = df["author"].value_counts()[0:20]
    print(df)


def get_insights(df, df2):
    """Prints several interesting insights.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    df2 : pandas.DataFrame
        The comments DataFrame.

    """

    # Get DataFrame totals.
    print("Total submissions:", len(df))
    print("Total comments:", len(df2))

    # Get unique submitters and commenters.
    submitters_set = set(df.groupby("author").count().index.tolist())
    commenters_set = set(df2.groupby("author").count().index.tolist())

    print("Total Submitters:", len(submitters_set))
    print("Total Commenters:", len(commenters_set))

    print("Common Submitters and Commenters:", len(
        submitters_set.intersection(commenters_set)))

    print("Not common submitters:", len(submitters_set.difference(commenters_set)))
    print("Not common commenters:", len(
        commenters_set.difference(submitters_set)))

    print("\Submissions stats:\n")
    resampled_submissions = df.resample("D").count()
    print("Most submissions on:", resampled_submissions.idxmax()["author"])
    print("Least submissions on:", resampled_submissions.idxmin()["author"])
    print(resampled_submissions.describe())

    print("\nComments stats:\n")
    resampled_comments = df2.resample("D").count()
    print("Most comments on:", resampled_comments.idxmax()["author"])
    print("Least comments on:", resampled_comments.idxmin()["author"])
    print(resampled_comments.describe())


def plot_submissions_and_comments_by_weekday(df, df2):
    """Creates a vertical bar plot with the percentage of
    submissions and comments by weekday.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    df2 : pandas.DataFrame
        The comments DataFrame.

    """

    # Days of the week in English.
    labels = ["Monday", "Tuesday", "Wednesday",
              "Thursday", "Friday", "Saturday", "Sunday"]

    # These will be used for calculating percentages.
    total = len(df)
    total2 = len(df2)

    # 0 to 6 (Monday to Sunday).
    submissions_weekdays = {i: 0 for i in range(0, 7)}
    comments_weekdays = {i: 0 for i in range(0, 7)}

    # We filter the DataFrames and set each weekday value
    # equal to its number of records.
    for k, v in submissions_weekdays.items():
        submissions_weekdays[k] = len(df[df.index.weekday == k])

    for k, v in comments_weekdays.items():
        comments_weekdays[k] = len(df2[df2.index.weekday == k])

    # The first set of vertical bars have a little offset to the left.
    # This is so the next set of bars can fit in the same place.
    bars = plt.bar([i - 0.2 for i in submissions_weekdays.keys()], [(i / total) * 100 for i in submissions_weekdays.values()], 0.4,
                   color="#1565c0", linewidth=0)

    # This loop creates small texts with the absolute values above each bar.
    for bar in bars:
        height = bar.get_height()
        real_value = int((height * total) / 100)

        plt.text(bar.get_x() + bar.get_width()/2.0, height,
                 "{:,}".format(real_value), ha="center", va="bottom")

    # This set of bars have a little offset to the right so they can fit
    # with the previous ones.
    bars2 = plt.bar([i + 0.2 for i in comments_weekdays.keys()], [(i / total2) * 100 for i in comments_weekdays.values()], 0.4,
                    color="#f9a825", linewidth=0)

    # This loop creates small texts with the absolute values above each bar (second set of bars).
    for bar2 in bars2:
        height2 = bar2.get_height()
        real_value2 = int((height2 * total2) / 100)

        plt.text(bar2.get_x() + bar2.get_width()/2.0, height2,
                 "{:,}".format(real_value2), ha="center", va="bottom")

    # We remove the top and right spines.
    plt.gca().spines["top"].set_visible(False)
    plt.gca().spines["right"].set_visible(False)

    # For the xticks we use the previously defined English weekdays.
    plt.xticks(list(submissions_weekdays.keys()), labels)

    # We add final customizations.
    plt.xlabel("Day of the Week")
    plt.ylabel("Percentage")
    plt.title("Submissions and Comments by Day")
    plt.legend(["Submissions", "Comments"])
    plt.tight_layout()
    plt.savefig("submissionsandcommentsbyweekday.png", facecolor="#222222")


def plot_submissions_and_comments_by_hour(df, df2):
    """Creates a horizontal bar plot with the percentage of
    submissions and comments by hour of the day.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    df2 : pandas.DataFrame
        The comments DataFrame.

    """

    # The hours of the day labels, from midnight to 11 pm.
    labels = ["00:00", "01:00", "02:00", "03:00", "04:00", "05:00",
              "06:00", "07:00", "08:00", "09:00", "10:00", "11:00",
              "12:00", "13:00", "14:00", "15:00", "16:00", "17:00",
              "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"]

    # This plot will require a lot of vertical space, we increase it.
    plt.figure(figsize=(12, 20))

    # These will be used for calculating percentages.
    total = len(df)
    total2 = len(df2)

    # We create dictionaries with keys from 0 to 23 (11 pm) hours.
    submissions_hours = {i: 0 for i in range(0, 24)}
    comments_hours = {i: 0 for i in range(0, 24)}

    # We filter the DataFrames and set each hour value
    # equal to its number of records.
    for k, v in submissions_hours.items():
        submissions_hours[k] = len(df[df.index.hour == k])

    for k, v in comments_hours.items():
        comments_hours[k] = len(df2[df2.index.hour == k])

    # The first set of horizontal bars have a little offset to the top.
    # This is so the next set of bars can fit in the same place.
    bars = plt.barh(y=[i + 0.2 for i in submissions_hours.keys()],
                    width=[(i / total) * 100 for i in submissions_hours.values()],
                    height=0.4, color="#1565c0",  linewidth=0)

    # This loop creates small texts with the absolute values next to each bar.
    for bar in bars:
        width = bar.get_width()
        real_value = int((width * total) / 100)

        plt.text(width + 0.03, bar.get_y() + 0.08,
                 "{:,}".format(real_value), ha="left", va="bottom")

    # This set of bars have a little offset to the bottom so they can fit
    # with the previous ones.
    bars2 = plt.barh(y=[i - 0.2 for i in comments_hours.keys()],
                     width=[(i / total2) * 100 for i in comments_hours.values()],
                     height=0.4, color="#f9a825", linewidth=0)

    # This loop creates small texts with the absolute values next to each bar (second set of bars).
    for bar2 in bars2:
        width2 = bar2.get_width()
        real_value2 = int((width2 * total2) / 100)

        plt.text(width2 + 0.03, bar2.get_y() + 0.08,
                 "{:,}".format(real_value2), ha="left", va="bottom")

    # We remove the top and right spines.
    plt.gca().spines["top"].set_visible(False)
    plt.gca().spines["right"].set_visible(False)

    # For the yticks we use the previously defined hours labels.
    plt.yticks(list(submissions_hours.keys()), labels)

    # We add final customizations.
    plt.xlabel("Percentage")
    plt.ylabel("Hour of the Day")
    plt.title("Submissions and comments by Hour")
    plt.legend(["Submissions", "Comments"])
    plt.tight_layout()
    plt.savefig("submissionsandcommentsbyhour.png", facecolor="#222222")


def plot_yearly_submissions_and_comments(df, df2):
    """Creates 2 line subplots with the counts of
    submissions and comments by day.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    df2 : pandas.DataFrame
        The comments DataFrame.

    """

    # we first resample both DataFrames for daily counts.
    df = df.resample("D").count()
    df2 = df2.resample("D").count()

    # We create a fig with 2 subplots that will shere their x-axis (date).
    fig, (ax1, ax2) = plt.subplots(2, sharex=True)

    # We set tht title now.
    fig.suptitle("Daily Submissions and Comments")

    # We plot the first DataFrame and remove the top spine.
    ax1.plot(df.index, df.author, color="#1565c0")
    ax1.spines["top"].set_visible(False)
    ax1.legend(["Submissions"])

    # We plot the second DataFrame.
    ax2.plot(df2.index, df2.author, color="#f9a825")
    ax2.legend(["Comments"])

    # We add the final customization.
    fig.tight_layout()
    plt.savefig("dailysubmissionsandcomments.png", facecolor="#222222")


def plot_submissions_by_user(df):
    """Plots a pie chart with the distribution
    of submissions by user groups.

    Parameters
    ----------
    df : pandas.DataFrame
        The submissions DataFrame.

    """

    # We first get the total submissions by each user.
    df = df["author"].value_counts()
    total = len(df)

    # We define our custom buckets, feel free to tweak them as you need.
    one = len(df[df.between(1, 1, inclusive=True)])
    two_to_five = len(df[df.between(2, 5, inclusive=True)])
    six_to_ten = len(df[df.between(6, 10, inclusive=True)])
    eleven_to_twenty = len(df[df.between(11, 20, inclusive=True)])
    twentyone_to_fifty = len(df[df.between(21, 50, inclusive=True)])
    fiftyone_to_onehundred = len(df[df.between(51, 100, inclusive=True)])
    more_than_onehundred = len(df[df.between(101, 10000, inclusive=True)])

    print("One:", one)
    print("Two to Five:", two_to_five)
    print("Six to Ten:", six_to_ten)
    print("Eleven to Twenty:", eleven_to_twenty)
    print("Twenty One to Fifty:", twentyone_to_fifty)
    print("Fifty One to One Hundrer:", fiftyone_to_onehundred)
    print("More than One Hundred:", more_than_onehundred)

    # We define labels, explodes and values, they must have the same length.
    labels = ["1", "2-5", "6-10", "11-20", "21-50", "51-100", "100+"]
    explode = (0, 0, 0, 0, 0, 0, 0)

    values = [one, two_to_five, six_to_ten, eleven_to_twenty,
              twentyone_to_fifty, fiftyone_to_onehundred, more_than_onehundred]

    # We will make our own legend labels calculating the percentages of each bucket.
    final_labels = list()

    for index, item in enumerate(values):

        final_labels.append("{} - {:.2f}% ({:,})".format(
            labels[index], item / total * 100, item))

    # We eemove the lines that separate the pie sections.
    plt.rcParams["patch.linewidth"] = 0

    # We plot our values, remove labels and shadows.
    plt.pie(values, explode=explode, labels=None, shadow=False)

    # We draw a circle in the Pie chart to make it a donut chart.
    centre_circle = plt.Circle(
        (0, 0), 0.75, color="#222222", fc="#222222", linewidth=0)

    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)

    # We add the final customization.
    plt.axis("equal")
    plt.legend(final_labels)
    plt.savefig("submissionsbyuser.png", facecolor="#222222")


def plot_comments_by_user(df):
    """Plots a pie chart with the distribution
    of comments by user groups.

    Parameters
    ----------
    df : pandas.DataFrame
        The comments DataFrame.

    """

    # We first get the total comments by each user.
    df = df["author"].value_counts()
    total = len(df)

    # We define our custom buckets, feel free to tweak them as you need.
    one = len(df[df.between(1, 1, inclusive=True)])
    two_to_ten = len(df[df.between(2, 10, inclusive=True)])
    eleven_to_twenty = len(df[df.between(11, 20, inclusive=True)])
    twentyone_to_fifty = len(df[df.between(21, 50, inclusive=True)])
    fiftyone_to_onehundred = len(df[df.between(51, 100, inclusive=True)])
    onehundredone_to_fivehundred = len(
        df[df.between(101, 500, inclusive=True)])
    fivehundredone_to_onethousand = len(
        df[df.between(501, 1000, inclusive=True)])
    morethanonethousand = len(df[df.between(1001, 100000, inclusive=True)])

    print("One:", one)
    print("Two to Ten:", two_to_ten)
    print("Eleven to Twenty:", eleven_to_twenty)
    print("Twenty One to Fifty:", twentyone_to_fifty)
    print("Fifty One to One Hundred:", fiftyone_to_onehundred)
    print("One Hundred One to Five Hundred:", onehundredone_to_fivehundred)
    print("Five Hundred One to One Thousand:", fivehundredone_to_onethousand)
    print("More than One Thousand:", morethanonethousand)

    # We define labels, explodes and values, they must have the same length.
    labels = ["1", "2-10", "11-20", "21-50",
              "51-100", "101-500", "501-1000", "1000+"]

    explode = (0, 0, 0, 0, 0, 0, 0, 0)

    values = [one, two_to_ten, eleven_to_twenty, twentyone_to_fifty, fiftyone_to_onehundred,
              onehundredone_to_fivehundred, fivehundredone_to_onethousand, morethanonethousand]

    # We will make our own legend labels calculating the percentages of each bucket.
    final_labels = list()

    for index, item in enumerate(values):

        final_labels.append(
            "{} - {:.2f}% ({:,})".format(labels[index], item / total * 100, item))

    # We eemove the lines that separate the pie sections.
    plt.rcParams["patch.linewidth"] = 0

    # We plot our values, remove labels and shadows.
    plt.pie(values, explode=explode, labels=None, shadow=False)

    # We draw a circle in the Pie chart to make it a donut chart.
    centre_circle = plt.Circle(
        (0, 0), 0.75, color="#222222", fc="#222222", linewidth=0)

    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)

    # We add the final customization.
    plt.axis("equal")
    plt.legend(final_labels)
    plt.savefig("commentsbyuser.png", facecolor="#222222")


def generate_most_common_words_word_cloud(df):
    """Generates a word cloud with the most used tokens.

    Parameters
    ----------
    df : pandas.DataFrame
        The tokens DataFrame.

    """

    # We load English and Spanish stop words that will be
    # get better results in our word cloud.
    stopwords = list()

    stopwords.extend(
        open(EN_STOPWORDS, "r", encoding="utf-8").read().splitlines())

    stopwords.extend(
        open(ES_STOPWORDS, "r", encoding="utf-8").read().splitlines())

    # We remove all the rows that are in our stopwords list.
    df = df[~df["lemma_lower"].isin(stopwords)]

    # We only take into account the top 1,000 words that are not numbers
    # are not stop words and are longer than one character.
    words = df[
        (df["is_alphabet"] == True) &
        (df["is_stopword"] == False) &
        (df["lemma_lower"].str.len() > 1)
    ]["lemma_lower"].value_counts()[:1000]

    # Now that we have the words and their counts we will create a list
    # with the words repeated equally to their counts.
    words_list = list()

    for index, value in words.items():
        for _ in range(value):
            words_list.append(index)

    # We create the mask from our cloud image.
    mask = np.array(Image.open(MASK_FILE))

    # We prepare our word cloud object and save it to disk.
    wc = wordcloud.WordCloud(background_color="#222222",
                             max_words=1000,
                             mask=mask,
                             contour_width=2,
                             colormap="summer",
                             font_path=FONT_FILE,
                             contour_color="white",
                             collocations=False)

    wc.generate(" ".join(words_list))
    wc.to_file("mostusedwords.png")


def generate_most_common_entities_word_cloud(df):
    """Generates a word cloud with the most used entities.

    Parameters
    ----------
    df : pandas.DataFrame
        The entities DataFrame.

    """

    # We load English and Spanish stop words that will be
    # get better results in our word cloud.
    stopwords = list()

    stopwords.extend(
        open(EN_STOPWORDS, "r", encoding="utf-8").read().splitlines())

    stopwords.extend(
        open(ES_STOPWORDS, "r", encoding="utf-8").read().splitlines())

    # We remove all the rows that are in our stopwords list.
    df = df[~df["text_lower"].isin(stopwords)]

    # We only take into account the top 1,000 entities that are longer than one character
    # and are in the the Location, Organization or Person categories.
    entities = df[
        (df["label"].isin(["LOC", "ORG", "PER"])) &
        (df["text"].str.len() > 1)]["text"].value_counts()[:1000]

    # Now that we have the entities and their counts we will create a list
    # with the entities repeated equally to their counts.
    entities_list = list()

    for index, value in entities.items():

        # This is specific to my dataset, feel free to remove it.
        if index == "Mexico":
            index = "México"

        for _ in range(value):
            entities_list.append(index)

    # We create the mask from our cloud image.
    mask = np.array(Image.open(MASK_FILE))

    # We prepare our word cloud object and save it to disk.
    wc = wordcloud.WordCloud(background_color="#222222",
                             max_words=1000,
                             mask=mask,
                             contour_width=2,
                             colormap="spring",
                             font_path=FONT_FILE,
                             contour_color="white",
                             collocations=False)

    wc.generate(" ".join(entities_list))
    wc.to_file("mostusedentities.png")


if __name__ == "__main__":

    submissions_df = pd.read_csv("mexico-submissions.csv",
                           parse_dates=["datetime"], index_col=0)

    comments_df = pd.read_csv("mexico-comments.csv",
                              parse_dates=["datetime"], index_col=0)

    tokens_df = pd.read_csv("tokens.csv")
    entities_df = pd.read_csv("entities.csv")
