"""
This script uses the Pushshift API to download posts from the specified subreddits.
By default it downloads all the posts from the newest one to the first one of the specified date.
"""

import csv
import time
import sys
from datetime import datetime

import requests

import tldextract

# 10,000 should cover at least 2 years of posts.
sys.setrecursionlimit(10000)

SUBREDDITS = ["mexico"]

HEADERS = {"User-Agent": "Posts Downloader v0.1"}
POSTS_LIST = list()

# Year month and day.
TARGET_DATE = "2019-01-01"

TARGET_TIMESTAMP = datetime.fromisoformat(TARGET_DATE).timestamp()


def init():
    """Iterates over all the subreddits and creates their csv files."""

    for subreddit in SUBREDDITS:

        writer = csv.writer(open("./{}-posts.csv".format(subreddit),
                                 "w", newline="", encoding="utf-8"))

        # Adding the header.
        writer.writerow(["datetime", "author", "title", "url", "domain"])

        print("Downloading:", subreddit)
        load_posts(subreddit=subreddit)
        writer.writerows(POSTS_LIST)

        POSTS_LIST.clear()


def load_posts(subreddit, latest_timestamp=None):
    """Keeps downloading posts using recursion, it downloads them 500 at a time.

    Parameters
    ----------
    subreddit : str
        The desired subreddit.

    latest_timestamp: int
        The timestampf of the latest comment.

    """

    base_url = "https://api.pushshift.io/reddit/submission/search/"

    params = {"subreddit": subreddit, "sort": "desc",
              "sort_type": "created_utc", "size": 500}

    stop_loading = False

    # After the first call of this function we will use the 'before' parameter.
    if latest_timestamp != None:
        params["before"] = latest_timestamp

    with requests.get(base_url, params=params, headers=HEADERS) as response:

        json_data = response.json()
        total_posts = len(json_data["data"])
        latest_timestamp = 0

        print("Downloading: {} posts".format(total_posts))

        for item in json_data["data"]:

            # We will only take 3 properties, the timestamp, author and url.

            latest_timestamp = item["created_utc"]

            iso_date = datetime.fromtimestamp(latest_timestamp)
            tld = tldextract.extract(item["url"])
            domain = tld.domain + "." + tld.suffix

            if item["is_self"] == True:
                domain = "self-post"

            if domain == "youtu.be":
                domain = "youtube.com"

            if domain == "redd.it":
                domain = "reddit.com"

            if latest_timestamp <= TARGET_TIMESTAMP:
                stop_loading = True
                break

            POSTS_LIST.append(
                [iso_date, item["author"], item["title"], item["url"], domain])

        if total_posts < 500:
            print("No more results.")
        elif stop_loading:
            print("Download complete.")
        else:
            time.sleep(1.2)
            load_posts(subreddit, latest_timestamp)


if __name__ == "__main__":

    init()
