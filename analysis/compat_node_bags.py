#!/usr/bin/env python3
import glob
import itertools
import os
import sys
from collections import Counter, deque, defaultdict
from typing import Iterable, Sequence

import multiset
import networkx as nx
import numpy as np
import pandas as pd

from common import walk_experiment_trees, jaccard_index, parallel_ji_distros

NODE_BAG_BASENAME = os.environ.get('NODE_BAG_BASENAME', 'node_bag_ji_distros')


def get_node_bag_for_dir(dirname: str) -> multiset.Multiset:
    bag_map = multiset.Multiset()
    for fn in glob.glob(os.path.join(dirname, "*.graphml")):
        graph = nx.read_graphml(fn)
        node_types = nx.get_node_attributes(graph, "node type")
        html_nodes = [graph.nodes[k].get("tag name") for k, v in node_types.items() if v == "HTML element"]
        bag_map.update(Counter(html_nodes))
    return bag_map


def main(argv):
    if len(argv) < 2:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    
    node_bag_csv = f"{NODE_BAG_BASENAME}.csv"
    node_bag_pdf = f"{NODE_BAG_BASENAME}.pdf"
    
    try:
        node_bag_df = pd.read_csv(node_bag_csv)
    except FileNotFoundError:
        node_bag_df = parallel_ji_distros(argv[1:], bagger=get_node_bag_for_dir)
        node_bag_df.to_csv(node_bag_csv)

    ax = node_bag_df.plot.density(xlim=[0.0, 1.0])
    fig = ax.get_figure()
    fig.savefig(node_bag_pdf)


if __name__ == "__main__":
    main(sys.argv)
