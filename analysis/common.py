"""common: utilties for extracting stats from parallel crawls
"""
import glob
import itertools
import multiprocessing
import os
from collections import defaultdict
from typing import Any, Callable, Iterable, Optional, Sequence, Tuple, Mapping
from urllib.parse import urlparse

import multiset
import networkx as nx
import numpy as np
import pandas as pd
from publicsuffix2 import get_sld


def url_etld1(url: str) -> str:
    bits = urlparse(url)
    return get_sld(bits.hostname)


def walk_experiment_trees(root_map: Mapping[str, str]) -> Iterable[Sequence[str]]:
    graphml_dirs = defaultdict(dict)
    for tag, root in root_map.items():
        for node, _, files in os.walk(root):
            if any(f.endswith(".graphml") for f in files):
                stem = os.path.relpath(node, root)
                pre_stem = node[:-len(stem)]
                graphml_dirs[stem][tag] = node
    
    tags = list(root_map.keys())
    for stem, dir_map in graphml_dirs.items():
        yield [stem] + [dir_map.get(t) for t in tags]


def graphs_in_dir(directory: Optional[str]) -> Iterable[nx.MultiDiGraph]:
    if directory is not None:
        for fn in glob.glob(os.path.join(directory, "*.graphml")):
            yield nx.read_graphml(fn)


def jaccard_index(a: multiset.Multiset, b: multiset.Multiset) -> float:
    num = len(a.intersection(b))
    den = len(a.union(b))
    return num / den if den else np.nan


def parallel_stats(root_map: Mapping[str, str], extractor: Callable[[Optional[str]], multiset.Multiset]) -> Iterable[Tuple[str, Sequence[Any]]]:
    with multiprocessing.Pool(processes=len(root_map)) as pool:
        for stem, *dirs in walk_experiment_trees(root_map):
            things = pool.map(extractor, dirs, chunksize=1)
            yield (stem, things)


def parallel_ji_distros(root_map: Mapping[str, str], bagger: Callable[[Optional[str]], multiset.Multiset]) -> pd.DataFrame:
    scores = defaultdict(list)
    tags = list(root_map)
    for _, bags in parallel_stats(root_map, bagger):
        for (t1, bag1), (t2, bag2) in itertools.combinations(zip(tags, bags), 2):
            ji = jaccard_index(bag1, bag2)
            scores[(t1, t2)].append(ji)

    return pd.DataFrame(scores)
