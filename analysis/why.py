#!/usr/bin/env python3
import os
import sys

import pandas as pd


def main(argv):
    try:
        filename = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} WALKER_GRAPH_CSV")
        return
    
    csv_stem = os.path.splitext(filename)[0]
    counts_csv = csv_stem + "_counts.csv"
    ranges_csv = csv_stem + "_ranges.csv"
    
    df = pd.read_csv(filename)

    cdf = df.groupby(['site_tag', 'profile_tag']).is_root.count().unstack(fill_value=0)
    cdf.to_csv(counts_csv)

    rdf = cdf.transpose().apply(lambda x: x.max() - x.min()).transpose().sort_values(ascending=False)
    rdf.to_csv(ranges_csv, header=False)


if __name__ == "__main__":
    main(sys.argv)
