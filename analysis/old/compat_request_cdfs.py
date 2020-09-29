#!/usr/bin/env python3
import glob
import itertools
import os
import sys
from collections import Counter, deque, defaultdict
from typing import Iterable, Sequence, Optional

import multiset
import networkx as nx
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from common import parallel_stats, get_profile_groups, rank_distinguished_items
from compat_request_bags import get_request_bag_for_dir

BASENAME = os.environ.get('BASENAME', 'request_bag_base')
TOP_COUNT = int(os.environ.get("TOP_COUNT", 50))


def main(argv):
    if len(argv) < 1:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]
    root_map = dict(zip(tags, directories))
    profile_groups = get_profile_groups(tags)

    request_bag_csv = f"{BASENAME}.csv"
    try:
        request_bag_df = pd.read_csv(request_bag_csv)
    except FileNotFoundError:
        rows = []
        for stem, bags in parallel_stats(root_map, get_request_bag_for_dir):
            for tag, bag in zip(tags, bags):
                for key, value in Counter(iter(bag)).items():
                    rows.append((stem, key[0], key[1], tag, value))
        request_bag_df = pd.DataFrame(rows, columns=['stem', 'etld1', 'type', 'profile', 'count'])
        request_bag_df.to_csv(request_bag_csv, index=False, header=True)
    

    # compute the TOP_COUNT-most-distinguished eTLD+1 domains
    domain_rank_df = rank_distinguished_items(request_bag_df, ['etld1', 'profile'], 'count', profile_groups)
    print(domain_rank_df)
    top_domains = list(domain_rank_df.index[:TOP_COUNT])
    for etld1 in top_domains:
        cdf = request_bag_df[request_bag_df.etld1 == etld1].groupby(['stem', 'profile']).sum().unstack(fill_value=0).cumsum()
        ax = cdf.plot(title=f"CDFs of all requests sent to 3p eTLD+1 '{etld1}'")
        fig = ax.get_figure()
        fig.savefig(f"{BASENAME}-DOMAIN_{etld1}.pdf")
        plt.close(fig)
    
    all_types = list(request_bag_df.type.unique())
    for rtype in all_types:
        cdf = request_bag_df[request_bag_df.type == rtype].groupby(['stem', 'profile']).sum().unstack(fill_value=0).cumsum()
        ax = cdf.plot(title=f"CDFs of all requests of type '{rtype}'")
        fig = ax.get_figure()
        fig.savefig(f"{BASENAME}-TYPE_{rtype}.pdf")
        plt.close(fig)
    
    cdf = request_bag_df.groupby(['stem', 'profile']).sum().unstack(fill_value=0).cumsum()
    ax = cdf.plot(title=f"CDFs of all requests")
    fig = ax.get_figure()
    fig.savefig(f"{BASENAME}-GRAND_TOTAL.pdf")
    plt.close(fig)


if __name__ == "__main__":
    main(sys.argv)
