#!/usr/bin/env python3
import sys
import os
import re
import itertools
from collections import defaultdict

import numpy as np
import pandas as pd


groups = {n: [f"{n}{i}" for i in range(1, 3)] for n in ('vanilla', 'prototype', 'splitkey', 'fullblock3p')}

def main(argv):
    try:
        csv_name = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} CSV_FILE")
    
    csv_stem, _ = os.path.splitext(csv_name)

    df = pd.read_csv(csv_name)
    df = df[(df.is_root == False) & (df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)
    df.dropna()

    overlap_series = defaultdict(list)
    column_names = None
    for site, site_values in df.groupby('site_tag'):
        profile_subset = site_values.groupby('profile_tag').sum()
        if not column_names:
            column_names = profile_subset.columns
        try:
            windows = {
                group_key: {
                    col_name: (
                        min(profile_subset.loc[g1][col_name], profile_subset.loc[g2][col_name]),
                        max(profile_subset.loc[g1][col_name], profile_subset.loc[g2][col_name]),
                    )
                    for col_name in column_names
                }
                for group_key, (g1, g2) in groups.items()
            }
        except KeyError:
            continue
        
        
        for g1, g2, in itertools.combinations(groups, 2):
            overlap_row = []
            for col_name in column_names:
                g1u, g1v = windows[g1][col_name]
                g2u, g2v = windows[g2][col_name]
                overlap_row.append((g2v >= g1u) and (g2u <= g2v))
            overlap_series[f"{g1}/{g2}"].append(sum(overlap_row))

    huffduff = pd.DataFrame(overlap_series)
    ax = huffduff.median().plot.bar()
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(pdf_name)
    print(huffduff)


        
        



if __name__ == "__main__":
    main(sys.argv)