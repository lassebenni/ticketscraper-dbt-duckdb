import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:101.0) Gecko/20100101 Firefox/101.0",
    "Accept": "*/*",
    "Accept-Language": "en",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.ticketswap.com/",
    "content-type": "application/json",
    "Origin": "https://www.ticketswap.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "TE": "trailers",
}


def check_expired(url: str) -> bool:
    response = requests.get(url, headers=headers)

    with open("response.html", "w") as f:
        f.write(response.text)

    if "Bummer, this ticket type has" in response.text:
        return True
    # elif "<strong>sold" in response.text:
    #     return False
    else:
        return False


def model(dbt, session):
    dbt.config(
        materialized="incremental",
        unique_key="id",
    )
    rel = dbt.ref("int_suspect_listings")
    df = rel.to_df()

    if dbt.is_incremental:
        # only new rows compared to max in current table
        max_from_this = f"select max(updated) from {dbt.this}"
        df = df.filter(df['updated'] >= session.sql(max_from_this).collect()[0][0])

    # for each row in the df check if the 'url' is expired
    df["expired"] = df["url"].apply(lambda x: check_expired(x))

    # only return expired rows
    df = df[df["expired"] == True]

    return df
