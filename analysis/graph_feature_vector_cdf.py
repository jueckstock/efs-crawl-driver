#!/usr/bin/env python3
import sys
import os

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def main(argv):
    try:
        csv_name = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} CSV_FILE")
    
    csv_stem, _ = os.path.splitext(csv_name)
    pdf_name = csv_stem + "_cumsum.pdf"

    df = pd.read_csv(csv_name)

    if 'is_root' in df.columns:
        df = df[(df.is_root == False) & (df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)

    df = df[df.total_nodes > 0].dropna()
    df = df.groupby(['site_tag', 'profile_tag']).sum()
    fields = df.columns
    #df = df.reset_index()

    for field in fields:
        cdf = df[field].unstack(fill_value=0).cumsum()
        ax = cdf.plot(title=f"Cumulative '{field}' Across All Crawled URLs")
        ax.set_xticklabels([])
        fig = ax.get_figure()
        fig.autofmt_xdate(rotation=45)
        fig.tight_layout()
        fig.savefig(f"{csv_stem}_cumulative_{field}.pdf")
        plt.close(fig)


if __name__ == "__main__":
    main(sys.argv)