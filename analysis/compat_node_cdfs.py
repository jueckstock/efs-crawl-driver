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

from common import parallel_stats
from compat_node_bags import get_node_bag_for_dir

NODE_BAG_BASENAME = os.environ.get('NODE_BAG_BASENAME', 'node_bag_base')


def main(argv):
    if len(argv) < 1:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]
    root_map = dict(zip(tags, directories))

    node_bag_csv = f"{NODE_BAG_BASENAME}.csv"
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
    
    for node_type in node_bag_df.tag.value_counts().iloc[:50].index:
        cdf = node_bag_df[node_bag_df.tag == node_type].set_index(['stem', 'tag', 'profile']).unstack(fill_value=0).cumsum()
        ax = cdf.plot(title=f"Per-Profile CDF of '{node_type}' HTML nodes seen")
        fig = ax.get_figure()
        fig.savefig(f"{NODE_BAG_BASENAME}_{node_type}.pdf")
        plt.close(fig)
    
    cdf = node_bag_df.groupby(['stem', 'profile']).sum().unstack().cumsum()
    ax = cdf.plot(title=f"Per-Profile CDF of all HTML nodes seen")
    fig = ax.get_figure()
    fig.savefig(f"{NODE_BAG_BASENAME}_GRAND_TOTAL.pdf")
    plt.close(fig)

    """ sums = node_bag_df.groupby(['tag', 'profile']).sum().unstack(fill_value=0)
    means = sums.transpose().mean()
    diffs = {}
    for col in sums.columns:
        diffs[col] = sums[col] - means
    ddf = pd.DataFrame(diffs)
    ddf.transpose().product() """
    


if __name__ == "__main__":
    main(sys.argv)
