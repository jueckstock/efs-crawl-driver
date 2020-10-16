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
from matplotlib import pyplot as plt

SITE_THRESHOLD = int(os.environ.get("SITE_THRESHOLD", 1))
LENGTH_FLOOR = int(os.environ.get("LENGTH_FLOOR", 8))
ROOT = bool(os.environ.get("ROOT", False))

STYLE=os.environ.get("STYLE", "tableau-colorblind10")

def main(argv):
    if len(argv) < 3:
        print(f"usage: {argv[0]} PRIVACY_METRICS_CSV (lat|long) [src1 [src2 [...]]]")
        return
    
    privacy_csv = argv[1]
    csv_stem = os.path.splitext(privacy_csv)[0]

    mode = argv[2].lower()
    assert mode in ('lat', 'long'), "Invalid mode!"

    # read data and filter frames based on ROOTness
    df = pd.read_csv(privacy_csv)
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
    
    # baseline set of profiles we must include
    PROFILES = ['vanilla1', 'vanilla2', 'splitkey1', 'splitkey2', 'prototype1', 'prototype2', 'fullblock3p1', 'fullblock3p2']
    
    # compute filename encoding all relevant settings
    source_tag = "_" + '-'.join(a.lower() for a in argv[3:]) if argv[3:] else ''
    privacy_pdf = f"{csv_stem}_{mode}{source_tag}.pdf"
    
    # limit to specfied token-sources (if any)
    sources = argv[3:]
    if sources:
        df = df[df.source.isin(sources)]
    df = df.drop('source', axis=1)

    # apply token-value-length floor filter to eliminate too-small tokens
    df = df[df.value.str.len() >= LENGTH_FLOOR]

    # Identify unique flow tuples (target eTLD+1, key, value) and their lateral/longitudinal reach
    counts = df.groupby(['http_etld1', 'key', 'value']).nunique()
    if mode == 'lat':
        distinctive_flows = counts[(counts.profile == 1) & (counts.site_etld1 > SITE_THRESHOLD)]
        xfield = 'http_etld1'
        yfield = 'site_etld1'
        XLABEL = "distinct third-party sites\ncapable of cross-site cookie tracking"
        YLABEL = "cumulative counts of first-party sites\nacross which tracking is possible"
        STRIDE = 25
    else:
        distinctive_flows = counts[(counts.profile == 1) & (counts.session > 1)]
        xfield = 'site_etld1'
        yfield = 'http_etld1'
        XLABEL = "distinct first-party sites\non which cross-time tracking is possible"
        YLABEL = "cumulative counts of third-party sites\ncapable of tracking"
        #XLABEL = "distinct third-party sites\ncapable of longitudinal cookie tracking"
        #YLABEL = "cumulative counts of first-party sites\non which longitudinal tracking is possible"
        STRIDE = 50
    
    # Filter the data to just the flows of interest and group/sum to compute and plot aggregate privacy impact
    privacy_tokens = df.set_index(['http_etld1', 'key', 'value']).loc[distinctive_flows.index]
    trackability = privacy_tokens.reset_index().groupby([xfield, 'profile'])[yfield].nunique().unstack(fill_value=0)
    
    # hack to fill in missing profile-columns (that didn't have any token flows and so were left out of the final aggregate)
    for p in PROFILES:
        if p not in trackability.columns:
            trackability[p] = 0


    plt.style.use(STYLE)
    COLORS = plt.rcParams['axes.prop_cycle'].by_key()['color']

    policy_info = {
        'vanilla': ('Permissive', COLORS[0]),
        'prototype': ('Page-length', COLORS[1]),
        'splitkey': ('Site-keyed', COLORS[2]),
        'fullblock3p': ('Blocking', COLORS[3]),
    }

    variant_style = {
        '1': '.',
        '2': 'x',
    }

    tsum = trackability.cumsum()
    series_map = dict(tsum.items())

    ax = None
    for policy, (name, color) in policy_info.items():
        for N in "12":
            series = series_map[policy + N]
            ax = series.plot(ax=ax, label=f"{name}-{N}", color=color, marker=variant_style[N], markevery=STRIDE)
    
    ax.legend()

    ax.set_xticks([i for i in range(0, len(tsum) + 1, STRIDE)], minor=False)
    ax.set_xticklabels([str(i) for i in range(0, len(tsum) + 1, STRIDE)], minor=False)
    ax.set_xlabel(XLABEL)
    ax.set_ylabel(YLABEL)
    fig = ax.get_figure()
    fig.tight_layout()
    fig.savefig(privacy_pdf)


if __name__ == "__main__":
    main(sys.argv)
