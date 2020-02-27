"""
This script uses the Pushshift API to download posts from the specified subreddits.
By default it downloads 10,000 posts starting from the newest one.
"""

import csv
import time
from datetime import datetime

import requests
import tldextract

SUBREDDITS = ["mexico"]

HEADERS = {"User-Agent": "Submissions Downloader v0.2"}
SUBMISSIONS_LIST = list()

MAX_SUBMISSIONS = 10000


def init():
    """Iterates over all the subreddits and creates their csv files."""

    for subreddit in SUBREDDITS:

        writer = csv.writer(open("./{}-submissions.csv".format(subreddit),
                                 "w", newline="", encoding="utf-8"))

        # Adding the header.
        writer.writerow(["datetime", "author", "title", "url", "domain"])

        print("Downloading:", subreddit)
        download_submissions(subreddit=subreddit)
        writer.writerows(SUBMISSIONS_LIST)

        SUBMISSIONS_LIST.clear()


def download_submissions(subreddit, latest_timestamp=None):
    """Keeps downloading submissions using recursion, it downloads them 500 at a time.

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

    # After the first call of this function we will use the 'before' parameter.
    if latest_timestamp != None:
        params["before"] = latest_timestamp

    with requests.get(base_url, params=params, headers=HEADERS) as response:

        json_data = response.json()
        total_submissions = len(json_data["data"])
        latest_timestamp = 0

        print("Downloading: {} submissions".format(total_submissions))

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

            SUBMISSIONS_LIST.append(
                [iso_date, item["author"], item["title"], item["url"], domain])

            if len(SUBMISSIONS_LIST) >= MAX_SUBMISSIONS:
                break

        if total_submissions < 500:
            print("No more results.")
        elif len(SUBMISSIONS_LIST) >= MAX_SUBMISSIONS:
            print("Download complete.")
        else:
            time.sleep(1.2)
            download_submissions(subreddit, latest_timestamp)


if __name__ == "__main__":

    init()
