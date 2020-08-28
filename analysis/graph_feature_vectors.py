#!/usr/bin/env python3
import sys
import os

import numpy as np
import pandas as pd


def main(argv):
    try:
        csv_name = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} CSV_FILE")
    
    csv_stem, _ = os.path.splitext(csv_name)
    pdf_name = csv_stem + ".pdf"

    df = pd.read_csv(csv_name)
    df = df[df.total_nodes > 0].dropna()
    meds = df.groupby('profile_tag').median()
    normed_meds = meds.apply(lambda x: x / x.max()) #meds.apply(lambda x: (x - np.mean(x)) / (np.max(x) - np.min(x)))
    ax = normed_meds.transpose().plot.bar()
    fig = ax.get_figure()
    fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(pdf_name)
    print(meds)


if __name__ == "__main__":
    main(sys.argv)