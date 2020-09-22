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
from compat_console_cdfs import get_console_bag_for_dir

BASENAME = os.environ.get('BASENAME', 'console_bag_ji_distro')


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
        node_bag_df = parallel_ji_distros(root_map, bagger=get_console_bag_for_dir)
        node_bag_df.to_csv(node_bag_csv)

    ax = node_bag_df.plot.density(xlim=[0.0, 1.0])
    fig = ax.get_figure()
    fig.savefig(node_bag_pdf)

if __name__ == "__main__":
    main(sys.argv)
