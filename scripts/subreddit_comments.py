"""
This script uses the Pushshift API to download comments from the specified subreddits.
By default it downloads 10,000 comments starting from the newest one.
"""

import csv
import time
from datetime import datetime

import requests


SUBREDDITS = ["mexico"]

HEADERS = {"User-Agent": "Comments Downloader v0.1"}
COMMENTS_LIST = list()

MAX_COMMENTS = 10000


def init():

    for subreddit in SUBREDDITS:

        writer = csv.writer(open("./{}-comments.csv".format(subreddit),
                                 "w", newline="", encoding="utf-8"))

        # Adding the header.
        writer.writerow(["datetime", "author", "body"])

        print("Downloading:", subreddit)
        load_comments(subreddit=subreddit)
        writer.writerows(COMMENTS_LIST)

        COMMENTS_LIST.clear()


def load_comments(subreddit, latest_timestamp=None):

    base_url = "https://api.pushshift.io/reddit/comment/search/"

    params = {"subreddit": subreddit, "sort": "desc",
              "sort_type": "created_utc", "size": 500}

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
            
            COMMENTS_LIST.append(
                [iso_date, item["author"], item["body"]])

            if len(COMMENTS_LIST) >= MAX_COMMENTS:
                break

        if total_comments < 500:
            print("No more results.")
        elif len(COMMENTS_LIST) >= MAX_COMMENTS:
            print("Download complete.")
        else:
            time.sleep(1.2)
            load_comments(subreddit, latest_timestamp)


if __name__ == "__main__":

    init()
