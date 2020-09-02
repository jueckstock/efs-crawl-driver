#!/usr/bin/env python3
import sys
import os

import pandas as pd
import numpy as np
from scikit_posthocs import posthoc_miller_friedman


NO_HALFBLOCK = bool(os.environ.get("NO_HALFBLOCK", False))


def main(argv):
    try:
        csv_file = argv[1]
        col_names = argv[2:]
        col_names[0]
    except IndexError:
        print(f"usage: {argv[0]} CSV_FILE COLUMN1 [COLUMN2 [...]]")
        return
    
    df = pd.read_csv(csv_file)

    # if this is has root/is-ad metadata, filter to 3p-no-ad and drop those columns
    if 'is_root' in df.columns:
        df = df[(df.is_root == False) & (df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)
    
    # filter out graphs with no nodes (broken crawls)
    df = df[df.total_nodes > 0].dropna()

    # sum all stats within a site-visit (per profile)
    #df = df.groupby(['site_tag', 'profile_tag']).sum().reset_index()

    # perform distribution-difference test/post-hoc analysis across profiles
    alpha = 0.05
    df3pg = df.groupby(['site_tag', 'profile_tag'])
    for col_name in col_names:
        matrix = df3pg[col_name].sum().unstack(fill_value=0)
        if NO_HALFBLOCK:
            matrix = matrix.drop(['halfblock3p1', 'halfblock3p2'], axis=1) # broken data from collection5
        print(col_name)
        print('-'*40)
        pvalues = posthoc_miller_friedman(matrix)
        """ diffset = set()
        for p1, series in pvalues.iterrows():
            for p2, cell in series.iteritems():
                if cell <= alpha:
                    diffset.add(tuple(sorted((p1[:-1], p2[:-1]))))
        print(diffset) """
        print(pvalues[pvalues <= alpha])
        print('-'*40)



if __name__ == "__main__":
    main(sys.argv)