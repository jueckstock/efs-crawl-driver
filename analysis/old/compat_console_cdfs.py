#!/usr/bin/env python3
import glob
import itertools
import json
import os
import sys
from collections import namedtuple, Counter
from typing import Iterable, Optional, Sequence
from urllib.parse import urlparse

import matplotlib.pyplot as plt
import multiset
import networkx as nx
import numpy as np
import pandas as pd
from loguru import logger
from publicsuffix2 import get_sld

from common import (
    get_profile_groups,
    graphs_in_dir,
    parallel_stats,
    rank_distinguished_items,
)

BASENAME = os.environ.get("BASENAME", "console_bag_base")
TOP_COUNT = int(os.environ.get("TOP_COUNT", 50))


ConsoleTuple = namedtuple("ConsoleTuple", ["kind", "level", "etld1", "path"])


def get_console_bag_for_dir(directory: Optional[str]) -> multiset.Multiset:
    bag = multiset.Multiset()
    for graph in graphs_in_dir(directory):
        try:
            node_types = nx.get_node_attributes(graph, "node type")
            console_node_candidates = [
                k
                for k, v in node_types.items()
                if v == "web API" and graph.nodes[k].get("method") == "console.log"
            ]
            if console_node_candidates:
                cln = console_node_candidates[0]
                for u, v, eid, args in graph.in_edges(cln, data="args", keys=True):
                    if args:
                        jargs = json.loads(args)
                        url = jargs.get("location", {}).get("url")
                        if url:
                            bits = urlparse(url)
                            hostname = bits.hostname
                            upath = bits.path
                        else:
                            hostname = upath = None
                        bag.add(
                            ConsoleTuple(
                                jargs.get("source"),
                                jargs.get("level"),
                                get_sld(hostname) if hostname else None,
                                upath,
                            )
                        )
        except:
            logger.exception(f"error processing graph in {directory} (skipping)")
    return bag


def main(argv):
    if len(argv) < 2:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]
    root_map = dict(zip(tags, directories))
    profile_groups = get_profile_groups(root_map)

    console_bag_csv = f"{BASENAME}.csv"
    try:
        console_bag_df = pd.read_csv(console_bag_csv)
    except FileNotFoundError:
        rows = []
        for stem, bags in parallel_stats(root_map, get_console_bag_for_dir):
            for tag, bag in zip(tags, bags):
                for key, value in Counter(iter(bag)).items():
                    rows.append((stem, *key, tag, value))
        console_bag_df = pd.DataFrame(
            rows, columns=["stem", *ConsoleTuple._fields, "profile", "value"]
        )
        console_bag_df.to_csv(console_bag_csv, index=False, header=True)
    
    # compute the TOP_COUNT-most-distinguished url-domains
    ddf = rank_distinguished_items(
        console_bag_df, ["etld1", "profile"], "value", profile_groups
    )
    print(ddf)
    top_items = list(ddf.index[:TOP_COUNT])

    for script_etld1 in top_items:
        cdf = (
            console_bag_df[console_bag_df.etld1 == script_etld1]
            .groupby(["stem", "profile"]).value.sum().unstack(fill_value=0).cumsum()
        )
        ax = cdf.plot(title=f"Per-Profile CDF of Console Messages from '{script_etld1}' script URLs")
        fig = ax.get_figure()
        fig.savefig(f"{BASENAME}_DOMAIN-{script_etld1}.pdf")
        plt.close(fig)

    # Show total-per-level CDF
    for level in console_bag_df.level.unique():
        cdf = (
            console_bag_df[console_bag_df.level == level]
            .groupby(["stem", "profile"]).value.sum().unstack(fill_value=0).cumsum()
        )
        ax = cdf.plot(title=f"Per-Profile CDF of All '{level}'-level Console Messages")
        fig = ax.get_figure()
        fig.savefig(f"{BASENAME}_LEVEL-{level}.pdf")
        plt.close(fig)

    cdf = console_bag_df.groupby(["stem", "profile"]).sum().unstack().cumsum()
    ax = cdf.plot(title=f"Per-Profile CDF of All Console Messages")
    fig = ax.get_figure()
    fig.savefig(f"{BASENAME}_GRAND_TOTAL.pdf")
    plt.close(fig)


if __name__ == "__main__":
    main(sys.argv)
