#!/usb/bin/env python3
import os
import sys
import re
from collections import Counter

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def stable0(df: pd.DataFrame) -> pd.DataFrame:
    return df


def stable1(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.vanilla1 == 1.0]


def stable2(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[(df.vanilla1 == 1.0) & (df.fullblock3p1 < 1.0) & (df.fullblock3p2)]


STABLES = {
    'raw': stable0,
    'vanilla-self': stable1,
    'vanilla-fullblock': stable2,
}


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

    for stability_algo, stability_func in STABLES.items():
        by_node = stability_func(df.groupby(['site_tag', 'frame_url', 'p2', 'p1']).node_jaccard.sum().unstack().dropna())
        by_edge = stability_func(df.groupby(['site_tag', 'frame_url', 'p2', 'p1']).edge_jaccard.sum().unstack().dropna())
        
        ax = (by_node.cumsum() / len(by_node)).plot(title=f"Cumulative Node-Bag Similarity [0.0-1.0] Scores ({stability_algo})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_nodes_sum_{stability_algo}.pdf")
        ax = by_node.plot.box(title=f"Node-Bag Similarity [0.0-1.0] Score Distributions ({stability_algo})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_nodes_box_{stability_algo}.pdf", no_xticks=False)
        
        ax = (by_edge.cumsum() / len(by_edge)).plot(title=f"Cumulative Edge-Bag Similarity [0.0-1.0] Scores ({stability_algo})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_edges_sum_{stability_algo}.pdf")
        ax = by_edge.plot.box(title=f"Edge-Bag Similarity [0.0-1.0] Score Distributions ({stability_algo})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_edges_box_{stability_algo}.pdf", no_xticks=False)


if __name__ == "__main__":
    main(sys.argv)