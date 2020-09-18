#!/usb/bin/env python3
import os
import sys
import re
from collections import Counter

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def save_ax_pdf(ax, filename: str, no_xticks: bool = True):
    fig = ax.get_figure()
    if no_xticks:
        ax.set_xticklabels([])
    else:
        fig.autofmt_xdate(rotation=45)
    fig.tight_layout()
    fig.savefig(filename)
    plt.close(fig)


def main(argv):
    try:
        csvfile = argv[1]
    except IndexError:
        print(f"usage: {argv[0]} CSV_FILE")
        return
    
    csv_stem = os.path.splitext(csvfile)[0]
    df = pd.read_csv(csvfile)

    YRANGE = (-0.1, 1.1)

    by_node = df.groupby(['site_tag', 'frame_url', 'p2', 'p1']).node_jaccard.sum().unstack().dropna()
    by_edge = df.groupby(['site_tag', 'frame_url', 'p2', 'p1']).edge_jaccard.sum().unstack().dropna()

    T1 = 0.95
    T2 = 0.10
    bn_good = by_node[by_node.vanilla1 >= T1]
    bn_base = bn_good[['fullblock3p1', 'fullblock3p2']].transpose().mean().sort_values()
    bn_meat = bn_good.loc[bn_base.iloc[:int(len(bn_good) * T2)].index]
    ax = (bn_meat.cumsum() / len(bn_meat)).plot(title=f"Cumulative Normalized Node-Bag Similarity", ylim=YRANGE)
    save_ax_pdf(ax, f"{csv_stem}_nodes_norm_curves.pdf")
    ax = bn_meat.plot.box(title=f"Cumulative Normalized Node-Bag Similarity", ylim=YRANGE)
    save_ax_pdf(ax, f"{csv_stem}_nodes_norm_boxes.pdf")


if __name__ == "__main__":
    main(sys.argv)