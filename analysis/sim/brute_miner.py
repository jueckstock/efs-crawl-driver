#!/usr/bin/env python
import csv
import os
import sys
from collections import defaultdict

import pandas as pd
from scipy import stats

from compare_full_bagz import ALL_NODE_TYPES


def node_set(mask: int) -> tuple:
    return tuple(ALL_NODE_TYPES[i] for i, b in enumerate(format(mask, "011d")) if b == '1')


def main(argv):
    try:
        big_df = pd.read_csv(argv[1])
        field = argv[2]
    except IndexError:
        print(f"usage: {argv[0]} BRUTE_BAGS.CSV FIELD_NAME")
        return
    csv_stem = os.path.splitext(argv[1])[0]

    ndf = big_df.groupby(['node_mask', 'site_tag', 'frame_url', 'p2', 'p1'])[field].sum().unstack().dropna().reset_index()
    
    cm1_map = [
        (gdf.transpose().apply(lambda r: (r.vanilla1 - r.fullblock3p1, r.vanilla1 - r.fullblock3p2)).transpose().mean(axis=1).sum(), node_set(mask))
        for mask, gdf in ndf.groupby('node_mask')
    ]
    cm1_map.sort(reverse=True)
    
    cluster_dump_file = f"{csv_stem}_{field}_set_scores.csv"
    with open(cluster_dump_file, "wt", encoding="utf8") as csvfd:
        wtr = csv.writer(csvfd, lineterminator="\n")
        wtr.writerow(['score', 'subset'])
        for score, subset in cm1_map:
            wtr.writerow([score, '/'.join(subset)])

    impact_map = defaultdict(float)
    for score, subset in cm1_map:
        for nt in subset:
            impact_map[nt] += (score / len(subset))
    impact_scores = [(v, k) for k, v in impact_map.items()]
    impact_scores.sort(reverse=True)
    for score, nt in impact_scores:
        print(f"{score:10.2f}: {nt}")
    
    """ for score, subset in cm1_map[:25]:
        print(f"{score:10.2f}: {'/'.join(subset)}")
    print('-'*40)
    for score, subset in cm1_map[-25:]:
        print(f"{score:10.2f}: {'/'.join(subset)}") """


if __name__ == "__main__":
    main(sys.argv)