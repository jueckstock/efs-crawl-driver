"""common: utilties for extracting stats from parallel crawls
"""
import glob
import itertools
import multiprocessing
import os
from collections import defaultdict
from typing import Callable, Iterable, Sequence

import multiset
import networkx as nx
import numpy as np
import pandas as pd


def walk_experiment_trees(roots: Sequence[str]) -> Iterable[Sequence[str]]:
    baseline_root = roots[0]
    baseline_dirs = []
    for node, dirs, files in os.walk(baseline_root):
        if any(f.endswith(".graphml") for f in files):
            baseline_dirs.append(node)

    for bd in baseline_dirs:
        stem = os.path.relpath(bd, baseline_root)
        yield [stem] + [(lambda d: d if os.path.isdir(d) else None)(os.path.join(r, stem)) for r in roots]


def graphs_in_dir(directory: str) -> Iterable[nx.MultiDiGraph]:
    for fn in glob.glob(os.path.join(directory, "*.graphml")):
        yield nx.read_graphml(fn)


def jaccard_index(a: multiset.Multiset, b: multiset.Multiset) -> float:
    num = len(a.intersection(b))
    den = len(a.union(b))
    return num / den if den else np.nan


def parallel_ji_distros(directories: Sequence[str], bagger: Callable[[str], multiset.Multiset]) -> pd.DataFrame:
    tags = [os.path.basename(d) for d in directories]
    scores = defaultdict(list)

    with multiprocessing.Pool(processes=len(directories)) as pool:
        for stem, *dirs in walk_experiment_trees(directories):
            bags = pool.map(bagger, dirs, chunksize=1)

            for (t1, bag1), (t2, bag2) in itertools.combinations(zip(tags, bags), 2):
                ji = jaccard_index(bag1, bag2)
                scores[(t1, t2)].append(ji)

    return pd.DataFrame(scores)
