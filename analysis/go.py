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


def get_forward_edges(G, node, base_id):
    return [
        (nu, nv, ek, eid)
        for nu, nv, ek, eid in G.edges(node, data="id", keys=True)
        if eid > base_id
    ]

def structure_bfs(G, n1):
    connected_nodes = set()
    q = deque([n1])
    while q:
        node = q.popleft()
        connected_nodes.add(node)
        for nu, nv, etype in G.edges(node, data="edge type"):
            if (etype == "structure") and (nv not in connected_nodes):
                q.append(nv)
    return connected_nodes

def dump_frame_bags(G) -> Iterable:
    node_types = nx.get_node_attributes(G, "node type")
    dom_roots = [k for k, v in node_types.items() if v == "DOM root"]
    for n in dom_roots:
        if G.nodes[n].get("url", "about:blank").startswith("http"):
            connected_nodes = structure_bfs(G, n)
            #for nn in connected_nodes:
            #    print(n, nn, G.nodes[nn])
            yield (G.nodes[n]["url"], multiset.Multiset(Counter([G.nodes[nn]["tag name"] for nn in connected_nodes if G.nodes[nn]["node type"] == "HTML element"])))


def walk_experiment_trees(roots: Sequence[str]) -> Iterable[Sequence[str]]:
    baseline_root = roots[0]
    baseline_dirs = []
    for node, dirs, files in os.walk(baseline_root):
        if any(f.endswith(".graphml") for f in files):
            baseline_dirs.append(node)

    for bd in baseline_dirs:
        stem = os.path.relpath(bd, baseline_root)
        yield [stem] + [(lambda d: d if os.path.isdir(d) else None)(os.path.join(r, stem)) for r in roots]


def get_node_bag_for_dir(dirname: str) -> multiset.Multiset:
    bag_map = multiset.Multiset()
    for fn in glob.glob(os.path.join(dirname, "*.graphml")):
        graph = nx.read_graphml(fn)
        node_types = nx.get_node_attributes(graph, "node type")
        html_nodes = [k for k, v in node_types.items() if v == "HTML element"]
        bag_map.update(Counter(html_nodes))
    return bag_map


def jaccard_index(a: multiset.Multiset, b: multiset.Multiset) -> float:
    num = len(a.intersection(b))
    den = len(a.union(b))
    return num / den if den else 0.0


def main(argv):
    try:
        directories = argv[1:]
        tags = [os.path.basename(d) for d in directories]
        baseline_tag = tags[0]
    except IndexError:
        print(f"usage: {argv[0]} DIR1 DIR2 [DIR3 [...]]")
        return
    
    print(f"using {baseline_tag} as baseline (tags: {tags})...")

    headers = ['site/page'] + [f"|{t}|" for t in tags] + [f"JI({t1}/{t2})" for t1, t2 in itertools.combinations(tags, 2)]
    print('\t'.join(headers))


    for stem, *dirs in walk_experiment_trees(directories):
        fields = [stem]
        bags = [get_node_bag_for_dir(d) for d in dirs]
        fields += [str(len(b)) for b in bags]

        for (t1, bag1), (t2, bag2) in itertools.combinations(zip(tags, bags), 2):
            ji = jaccard_index(bag1, bag2)
            fields.append(str(ji))
        
        print('\t'.join(fields))
        
        """ for url, bag in baseline_bag_map.items():
            for i, alt_dir in enumerate(tuples[1:]):
                if alt_dir:
                    alt_bag_map = get_node_bags_for_dir(alt_dir)
                    if url in alt_bag_map:
                        ji = jaccard_index(bag, alt_bag_map[url])
                        print(f"{baseline_tag}:{tags[i+1]}\t{url}\tmatch\t{ji}")
                    else:
                        print(f"{baseline_tag}:{tags[i+1]}\t{url}\tmissing-url\t0.0")
                else:
                    print(f"{baseline_tag}:{tags[i+1]}\t{url}\tmissing-dir\t0.0") """
        


if __name__ == "__main__":
    main(sys.argv)
