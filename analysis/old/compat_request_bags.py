#!/usr/bin/env python3
import glob
import os
import sys
from urllib.parse import urlparse
from typing import Optional

import multiset
import networkx as nx
import pandas as pd
from publicsuffix2 import get_sld

from common import parallel_ji_distros, find_3p_nonad_graphs

BASENAME = os.environ.get('BASENAME', 'request_bag_ji_distros')


def get_request_bag_for_dir(dirname: Optional[str]) -> multiset.Multiset:
    bag_map = multiset.Multiset()
    for graph in find_3p_nonad_graphs(dirname):
        resource_nodes = [k for k, v in nx.get_node_attributes(graph, "node type").items() if v == "resource"]
        for k, v in nx.get_node_attributes(graph, "node type").items():
            if v == 'resource':
                url_fields = urlparse(graph.nodes[k]["url"])
                etld1 = get_sld(url_fields.hostname)
                bag_map.update((etld1, rt) for n1, n2, eid, rt in graph.in_edges(k, data="request type", keys=True))
    return bag_map


def main(argv):
    if len(argv) < 2:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    directories = argv[1:]
    tags = [os.path.basename(d) for d in directories]
    root_map = dict(zip(tags, directories))

    request_bag_csv = f"{BASENAME}.csv"
    request_bag_pdf = f"{BASENAME}.pdf"
    
    try:
        request_bag_df = pd.read_csv(request_bag_csv)
    except FileNotFoundError:
        request_bag_df = parallel_ji_distros(root_map, bagger=get_request_bag_for_dir)
        request_bag_df.to_csv(request_bag_csv)

    ax = request_bag_df.plot.density(xlim=[0.0, 1.0])
    fig = ax.get_figure()
    fig.savefig(request_bag_pdf)


if __name__ == "__main__":
    main(sys.argv)
