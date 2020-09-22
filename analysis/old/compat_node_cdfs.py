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
from compat_node_bags import get_node_bag_for_dir

BASENAME = os.environ.get('BASENAME', 'node_bag_base')
TOP_COUNT = int(os.environ.get("TOP_COUNT", 50))


def main(argv):
    if len(argv) < 1:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]
    root_map = dict(zip(tags, directories))
    profile_groups = get_profile_groups(root_map)

    node_bag_csv = f"{BASENAME}.csv"
    try:
        node_bag_df = pd.read_csv(node_bag_csv)
    except FileNotFoundError:
        rows = []
        for stem, bags in parallel_stats(root_map, get_node_bag_for_dir):
            for tag, bag in zip(tags, bags):
                for key, value in Counter(iter(bag)).items():
                    rows.append((stem, key, tag, value))
        node_bag_df = pd.DataFrame(rows, columns=['stem', 'tag', 'profile', 'value'])
        node_bag_df.to_csv(node_bag_csv, index=False, header=True)
    
    # compute the TOP_COUNT-most-distinguished tag names using a hacky little in/cross-group variance measure
    ddf = rank_distinguished_items(node_bag_df, ['tag', 'profile'], 'value', profile_groups)
    print(ddf)
    top_items = list(ddf.index[:TOP_COUNT])

    for node_type in top_items:
        cdf = node_bag_df[node_bag_df.tag == node_type].set_index(['stem', 'tag', 'profile']).unstack(fill_value=0).cumsum()
        ax = cdf.plot(title=f"Per-Profile CDF of '{node_type}' HTML nodes seen")
        fig = ax.get_figure()
        fig.savefig(f"{BASENAME}_{node_type}.pdf")
        plt.close(fig)
    
    cdf = node_bag_df.groupby(['stem', 'profile']).sum().unstack().cumsum()
    ax = cdf.plot(title=f"Per-Profile CDF of all HTML nodes seen")
    fig = ax.get_figure()
    fig.savefig(f"{BASENAME}_GRAND_TOTAL.pdf")
    plt.close(fig)


if __name__ == "__main__":
    main(sys.argv)
