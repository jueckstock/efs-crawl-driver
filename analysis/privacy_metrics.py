#!/usr/bin/env python3
import csv
import itertools
import multiprocessing
import os
import re
import sys
from collections import namedtuple
from http import cookies
from typing import Iterable, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qsl, urlparse

import networkx as nx
import numpy as np
import pandas as pd
from loguru import logger
from publicsuffix2 import get_sld

from common import graphs_in_dir, walk_experiment_trees

SITE_THRESHOLD = int(os.environ.get("SITE_THRESHOLD", 5))


def main(argv):
    if len(argv) < 2:
        print(f"usage: {argv[0]} PRIVACY_METRICS_CSV [src1 [src2 [...]]]")
        return
    privacy_csv = argv[1]
    csv_stem = os.path.splitext(privacy_csv)[0]
    privacy_pdf = csv_stem + ".pdf"
    
    df = pd.read_csv(privacy_csv)
    sources = argv[2:]
    if sources:
        df = df[df.source.isin(sources)]
    df = df.drop('source', axis=1)

    counts = df.groupby(['http_etld1', 'key', 'value']).nunique()
    distinctive_flows = counts[(counts.profile == 1) & (counts.site_etld1 > SITE_THRESHOLD)].index
    privacy_tokens = df.set_index(['http_etld1', 'key', 'value']).loc[distinctive_flows]
    trackability = privacy_tokens.reset_index().groupby(['http_etld1', 'profile']).site_etld1.nunique().unstack(fill_value=0)

    ax = trackability.cumsum().plot()
    fig = ax.get_figure()
    fig.savefig(privacy_pdf)


if __name__ == "__main__":
    main(sys.argv)
