"""
This script uses the Pushshift API to download comments from the specified subreddits.
By default it downloads all the comments from the newest one to the first one of the specified date.
"""

import csv
import sys
import time
from datetime import datetime

import requests

# 10,000 should cover at least 3 years of comments.
sys.setrecursionlimit(10000)


SUBREDDITS = ["mexico"]

HEADERS = {"User-Agent": "Comments Downloader v0.2"}
COMMENTS_LIST = list()

# Year month and day.
TARGET_DATE = "2019-01-01"

TARGET_TIMESTAMP = datetime.fromisoformat(TARGET_DATE).timestamp()


def init():
    """Iterates over all the subreddits and creates their csv files."""

    for subreddit in SUBREDDITS:

        writer = csv.writer(open("./{}-comments.csv".format(subreddit),
                                 "w", newline="", encoding="utf-8"))

        # Adding the header.
        writer.writerow(["datetime", "author", "body"])

        print("Downloading:", subreddit)
        load_comments(subreddit, writer)


def load_comments(subreddit, writer, latest_timestamp=None):
    """Keeps downloading comments using recursion, it saves them 500 at a time.

    Parameters
    ----------
    subreddit : str
        The desired subreddit.

    write: csv.writer
        A writer object that will save the comments to disk.

    latest_timestamp: int
        The timestampf of the latest comment.

    """

    base_url = "https://api.pushshift.io/reddit/comment/search/"

    params = {"subreddit": subreddit, "sort": "desc",
              "sort_type": "created_utc", "size": 500}

    stop_loading = False

    # After the first call of this function we will use the 'before' parameter.
    if latest_timestamp != None:
        params["before"] = latest_timestamp

    with requests.get(base_url, params=params, headers=HEADERS) as response:

        json_data = response.json()
        total_comments = len(json_data["data"])
        latest_timestamp = 0

        print("Downloading: {} comments".format(total_comments))

        for item in json_data["data"]:

            # We will only take 3 properties, the timestamp, author and body.

            latest_timestamp = item["created_utc"]

            iso_date = datetime.fromtimestamp(latest_timestamp)

            if latest_timestamp <= TARGET_TIMESTAMP:
                stop_loading = True
                break

            COMMENTS_LIST.append(
                [iso_date, item["author"], item["body"]])

        writer.writerows(COMMENTS_LIST)
        COMMENTS_LIST.clear()

        if total_comments < 500:
            print("No more r○esults.")
        elif stop_loading:
            print("Download complete.")
        else:
            time.sleep(1.2)
            load_comments(subreddit, writer, latest_timestamp)


if __name__ == "__main__":

    init()
