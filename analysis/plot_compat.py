#!/usb/bin/env python3
import os
import sys
import re
from collections import Counter

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


T1 = float(os.environ.get("T1", 1.0))
ROOT = bool(os.environ.get("ROOT", False))


def stable0(df: pd.DataFrame) -> pd.DataFrame:
    return df


def stable1(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.vanilla1 >= T1]


def stable2(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[(df.vanilla1 >= T1) & (df.fullblock3p1 < T1) & (df.fullblock3p2 < T1)]


STABLES = {
    'raw': stable0,
    #'vanilla-self': stable1,
    'high-confidence': stable2,
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

    if 'is_root' in df.columns:
        if ROOT:
            df = df[df.is_root == True].drop(['is_root', 'is_ad'], axis=1)
            csv_stem += "_root"
            subset = "Root/1p Frames"
        else:
            df = df[(df.is_root == False) & (df.is_ad == False)].drop(['is_root', 'is_ad'], axis=1)
            csv_stem += "_3pnoad"
            subset = "Non-Ad 3p Frames"
    else:
        subset = "All Frames"
    
    if 'session' in df.columns:
        groupers = ['session', 'site_tag', 'frame_url', 'p2', 'p1']
    else:
        groupers = ['site_tag', 'frame_url', 'p2', 'p1']

    YRANGE = (-0.1, 1.1)

    for stability_algo, stability_func in STABLES.items():
        raw_by_node = df.groupby(groupers).node_jaccard.sum().unstack().dropna()
        by_node = stability_func(raw_by_node)
        node_ratio = len(by_node) / len(raw_by_node)
        raw_by_edge = df.groupby(groupers).edge_jaccard.sum().unstack().dropna()
        by_edge = stability_func(raw_by_edge)
        edge_ratio = len(by_edge) / len(raw_by_edge)
        
        ax = (by_node.cumsum() / len(by_node)).plot(title=f"Cumulative Node-Bag Similarity [0.0-1.0] Scores\n({subset}; {stability_algo}[{T1:.2f}]: {node_ratio:.2%})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_nodes_sum_{stability_algo}.pdf")
        ax = by_node.plot.box(title=f"Node-Bag Similarity [0.0-1.0] Score Distributions\n({subset}; {stability_algo}[{T1:.2f}]: {node_ratio:.2%})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_nodes_box_{stability_algo}.pdf", no_xticks=False)
        
        ax = (by_edge.cumsum() / len(by_edge)).plot(title=f"Cumulative Edge-Bag Similarity [0.0-1.0] Scores\n({subset}; {stability_algo}[{T1:.2f}]: {edge_ratio:.2%})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_edges_sum_{stability_algo}.pdf")
        ax = by_edge.plot.box(title=f"Edge-Bag Similarity [0.0-1.0] Score Distributions\n({subset}; {stability_algo}[{T1:.2f}]: {edge_ratio:.2%})", ylim=YRANGE)
        save_ax_pdf(ax, f"{csv_stem}_edges_box_{stability_algo}.pdf", no_xticks=False)


if __name__ == "__main__":
    main(sys.argv)