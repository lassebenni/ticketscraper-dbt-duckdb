import requests
import pdb
import pandas as pd
import time

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cookie": "optimizely_id=07780e83-18de-4e29-a661-eb6622f7107e; intercom-id-f9d90yaf=095f5aae-5eb0-48df-84f5-70f09e5f2f2e; intercom-device-id-f9d90yaf=57715a71-626b-4767-8b50-46b2ac3ba648; favorites_banner_clicked=3; intercom-session-f9d90yaf=; rbzid=AgaJGHBVLd5wuupy9tkEmEesuUXCdTolCPbuIhpQUTpRZVck/EjthmmrfuxvjS5YWcWfBvUy8wwCoCxVYvZefJpCRr7EkvvfIAPg89v27i0NWnac289v0lL6jwPE77TbetSdKuM+lCv/1PY2ikCkSOeWcr21cNGCfql08arM5IUh1hYlQ1rC2Xp6S3z42fZuGDoehoHldMwXc1AacvkimQ==; rbzsessionid=674dc2097404a96ca0c9e6a132e4b8af; optimizely_id=69db5173-0103-472a-9865-418f0d165eeb",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "TE": "trailers",
}

counter = 0
SLEEP_TIME = 10


def check_expired(url: str) -> bool:
    response = requests.get(url, headers=headers)
    print(response.status_code)

    time.sleep(SLEEP_TIME)
    if "expired" in response.text:
        global counter
        counter += 1
        print(f"Found {counter} expired tickets")
        return True
    # elif "<strong>sold" in response.text:
    #     return False
    else:
        # pdb.set_trace()
        print(f"Found a ticket that is not expired: {url}")
        return False


def model(dbt, session):
    dbt.config(
        materialized="incremental",
        unique_key="ticket_id",
    )

    rel = dbt.ref("int_suspect_listings")
    df = rel.to_df()

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_date_in_model = session.sql(f"select max(updated) from {dbt.this}")
        max_date = max_date_in_model.df().values[0][0]
        if pd.isna(max_date):
            print("No max date found, returning all rows")
        else:
            df = df[df["updated"] > max_date]

            # only keep ticket_ids that are not already in the table
            checked_ids = session.sql(
                f"select distinct ticket_id from {dbt.this}"
            ).fetchall()
            df = df[~df["ticket_id"].isin(checked_ids)]

    print(f"Found {len(df)} suspect listings")
    # for each row in the df check if the 'url' is expired
    df["is_expired"] = df["url"].apply(lambda x: check_expired(x))

    # only return expired rows
    df = df[df["is_expired"] == True]

    return df
