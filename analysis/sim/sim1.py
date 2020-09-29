#!/usb/bin/env python3
import os
import sys
import re
from collections import Counter

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


RE_KEYVAL = re.compile(r"(\w+=)([^;&]+)([;&]|$)")


def simify_url(url: str) -> str:
    def _subber(m: re.Match) -> str:
        return m.group(1)
    return RE_KEYVAL.sub(_subber, url)


def main(argv):
    try:
        csv_filename = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} ALL_GRAPHS_CSV [URL_LIST_CSV]")
        return

    all_graphs_df = pd.read_csv(csv_filename)
    csv_stem = os.path.splitext(csv_filename)[0]
    
    # optional per-site-tag error data used to filter out rows from URLs that encountered errors
    if len(argv) > 2:
        url_df = pd.read_csv(argv[2])
        err_df = url_df.set_index('site_tag').drop(['order', 'crawl_url'], axis=1).transpose().any().transpose()
        err_df = err_df[err_df == False]
        all_graphs_df = all_graphs_df[all_graphs_df.site_tag.isin(err_df.index)]
    else:
        url_df = None
    
    # keep only 3p-no-ad frames
    work_df = all_graphs_df[(all_graphs_df.is_root == False) & (all_graphs_df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)
    
    # strip out suspected-parameter-values from frame URLs to allow cross-profile matching of "same-frame" URLs for the same crawl-URL
    simified_urls = work_df.url.apply(simify_url)
    work_df.url = simified_urls

    # plot curves for each numeric feature in the matrix
    stats_fields = work_df.dtypes[work_df.dtypes == np.int64].index.values
    for field in stats_fields:
        # find crawled-URL/frame-URL field values that are equi-present (not equivalent!) across all profiles for that crawl
        # (i.e., count only frame-URLs that were loaded on all the profiles of a given crawl URL/site visit)
        matched_df = work_df.groupby(['site_tag', 'url', 'profile_tag'])[field].sum().unstack().dropna().reset_index()

        # from those rows, compute the per-frame-URL median for this field metric
        print(field, matched_df.median())
        t1 = matched_df.groupby('url').median()

        # hacky experiment/tunable: keep only medians showing a "high enough" variance amongst the baseline profiles (vanilla/fullblock3p)
        baseline_variance = t1[['vanilla1', 'vanilla2', 'fullblock3p1', 'fullblock3p2']].transpose().var()
        selected_variance = baseline_variance[baseline_variance >= 25.0]
        
        # cum-sum and plot the values we got for this filed across all equi-loaded frame URLs
        print(field, t1.shape)
        ax = t1.loc[selected_variance.index].cumsum().plot(title=f"Same-Frame-URL '{field}' Median Cumulatives")
        ax.set_xticks([])
        fig = ax.get_figure()
        fig.tight_layout()
        fig.savefig(f"{csv_stem}_sim1_{field}.pdf")
        plt.close(fig)



if __name__ == "__main__":
    main(sys.argv)
