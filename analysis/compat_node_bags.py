#!/usr/bin/env python3
import glob
import itertools
import os
import sys
from collections import Counter, deque, defaultdict
from typing import Iterable, Sequence, Optional

import multiset
import networkx as nx
import numpy as np
import pandas as pd

from common import parallel_ji_distros

BASENAME = os.environ.get('BASENAME', 'node_bag_ji_distros')


def get_node_bag_for_dir(dirname: Optional[str]) -> multiset.Multiset:
    bag_map = multiset.Multiset()
    if dirname is not None:
        for fn in glob.glob(os.path.join(dirname, "*.graphml")):
            graph = nx.read_graphml(fn)
            node_types = nx.get_node_attributes(graph, "node type")
            html_nodes = [graph.nodes[k].get("tag name") for k, v in node_types.items() if v == "HTML element"]
            bag_map.update(Counter(html_nodes))
    return bag_map


def main(argv):
    if len(argv) < 1:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]
    root_map = dict(zip(tags, directories))

    node_bag_csv = f"{BASENAME}.csv"
    node_bag_pdf = f"{BASENAME}.pdf"
    
    try:
        node_bag_df = pd.read_csv(node_bag_csv)
    except FileNotFoundError:
        node_bag_df = parallel_ji_distros(root_map, bagger=get_node_bag_for_dir)
        node_bag_df.to_csv(node_bag_csv)

    ax = node_bag_df.plot.density(xlim=[0.0, 1.0])
    fig = ax.get_figure()
    fig.savefig(node_bag_pdf)

if __name__ == "__main__":
    main(sys.argv)
