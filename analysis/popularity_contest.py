#!/usr/bin/env python3
import sys

import pandas as pd

from common import url_etld1


def main(argv):
    try:
        bagz_csv_file = argv[1]
        master_url_file = argv[2]
        privacy_file = argv[3]
    except IndexError:
        print(f"usage: {argv[0]} BAGZ_CSV MASTER_URLS_CSV PRIVACY_FLOWS_CSV")
        return
    
    bagz_df = pd.read_csv(bagz_csv_file)
    bagz_df = bagz_df.loc[(bagz_df.is_root == False) & (bagz_df.is_ad == False)]

    popular_frame_urls_df = bagz_df.groupby('frame_url').site_tag.nunique().sort_values(ascending=False).reset_index()
    popular_frame_urls_df["frame_etld1"] = popular_frame_urls_df.frame_url.map(url_etld1)
    popular_frame_urls_df = popular_frame_urls_df.set_index('frame_etld1')

    priv_df = pd.read_csv(privacy_file)
    cdf = priv_df.loc[(priv_df.source == "Cookie") & (priv_df.value.str.len() >= 8)]
    cdf['token'] = cdf['key'] + cdf['value']
    cdf2 = cdf.groupby(['http_etld1', 'profile']).token.nunique().unstack(fill_value=0)
    cdf2['cookie_score'] = cdf2.vanilla2 - cdf2.fullblock3p2
    domain_tokens = cdf2['cookie_score'].sort_values(ascending=False)

    wut = popular_frame_urls_df.join(domain_tokens, on="frame_etld1")
    wut.to_csv("popular_frame_urls.csv")

    url_df = pd.read_csv(master_url_file, index_col="site_tag")
    frame_map = bagz_df.join(url_df, on="site_tag")
    frame_map[['frame_url', 'crawl_url']].to_csv("frame_site_map.csv", index=False)



    #popular_frame_urls_df.to_csv("popularity_frame_urls.csv")



if __name__ == "__main__":
    main(sys.argv)